import { DateTimeFormatter, ZonedDateTime } from '@js-joda/core'

import { Request, Response, Router } from 'express'
import { SKInflux } from './influx'
import { InfluxDB as InfluxV1 } from 'influx'
import { FluxResultObserver, FluxTableMetaData } from '@influxdata/influxdb-client'

const supportedFormats: readonly string[] = ['gpx', 'json']

function makeArray(d1: number, d2: number) {
  const arr = []
  for (let i = 0; i < d1; i++) {
    arr.push(new Array(d2))
  }
  return arr
}

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  influx: SKInflux,
  selfId: string,
  debug: (k: string) => void,
) {
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context, format } = getRequestParams(req as FromToContextRequest, selfId)
    getValues(influx, context, from, to, format, debug, req, res)
  })
  router.get('/signalk/v1/history/contexts', (req: Request, res: Response) => getContexts(influx, res))
  router.get('/signalk/v1/history/paths', (req: Request, res: Response) => {
    const { from, to } = getRequestParams(req as FromToContextRequest, selfId)
    getPaths(influx, from, to, res)
  })
}

async function getContexts(influx: SKInflux, res: Response) {
  influx.queryApi
    .collectRows(
      `
  import "influxdata/influxdb/v1"
  v1.tagValues(bucket: "${influx.bucket}", tag: "context")
  `,
      (row, tableMeta) => {
        return tableMeta.get(row, '_value')
      },
    )
    .then((r) => res.json(r))
}

async function getPaths(influx: SKInflux, from: ZonedDateTime, to: ZonedDateTime, res: Response) {
  const r = await influx.queryApi.collectRows(
    `
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "${influx.bucket}")`,
    (row, tableMeta) => {
      return tableMeta.get(row, '_value')
    },
  )
  res.json(r)
}

interface ValuesResult {
  context: string
  range: {
    from: string
    to: string
  }
  values: {
    path: string
    method: string
    source?: string
  }[]
  data: ValuesResultRow[]
}

interface SimpleResponse {
  status: (s: number) => void
  /* eslint-disable-next-line  @typescript-eslint/no-explicit-any */
  json: (j: any) => void
  header: (n: string, v: string) => void
  send: (c: string) => void
}

interface SimpleRequest {
  query: {
    resolution?: string
    paths?: string
    format?: string
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ValuesResultRow = any[]

function getPositions(
  v1Client: InfluxV1,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  format: string,
  timeResolutionMillis: number,
  debug: (s: string) => void,
  res: Response,
) {
  const query = `
  select
    first(lat) as lat, first(lon) as lon
  from
    "navigation.position"
  where
    "context" = '${context}'
    and
    time >= '${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
    and
   time <= '${to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
  group by time(${timeResolutionMillis}ms)`

  debug(query)

  if (format) format = format.toLowerCase()

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  v1Client.query(query).then((rows: any[]) => {
    if (format === 'gpx') {
      outputPositionsGpx(rows, context, res)
    } else if (format === 'json' || format === undefined) {
      outputPositionsJson(rows, context, from, to, res)
    } else {
      // requested format not supported, return error with list of supported formats
      outputError(
        res,
        400,
        "Format '" + format + "' is not supported. Supported formats are: " + supportedFormats + '.',
      )
    }
  })
}

export function getValues(
  influx: SKInflux,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  format: string,
  debug: (s: string) => void,
  req: SimpleRequest,
  res: Response,
) {
  const start = Date.now()
  const timeResolutionMillis =
    (req.query.resolution
      ? Number.parseFloat(req.query.resolution as string)
      : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000
  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z.,:]/gi, '').split(',')
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression)

  if (pathSpecs[0].path === 'navigation.position') {
    getPositions(influx.v1Client, context, from, to, format, timeResolutionMillis, debug, res)
    return
  }

  const uniquePaths = pathSpecs.reduce<string[]>((acc, ps) => {
    if (acc.indexOf(ps.path) === -1) {
      acc.push(ps.path)
    }
    return acc
  }, [])
  const uniqueAggregates = pathSpecs.reduce<string[]>((acc, ps) => {
    if (acc.indexOf(ps.aggregateFunction) === -1) {
      acc.push(ps.aggregateFunction)
    }
    return acc
  }, [])

  const query = `
  select
    ${uniqueAggregates.map((aggregateFunction) => `${aggregateFunction}(value)`).join(',')}
  from
    ${uniquePaths.map((s) => `"${s}"`).join(',')}
  where
    "context" = '${context}'
    and
    time >= '${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
    and
   time <= '${to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
  group by time(${timeResolutionMillis}ms)`
  debug(query)

  influx.v1Client
    .query(query)
    .then((result) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rows = result as any[]
      debug(`got rows ${Date.now() - start}`)
      const resultLength = rows.length / uniquePaths.length
      const resultData = makeArray(resultLength, pathSpecs.length + 1)

      for (let j = 0; j < resultLength; j++) {
        resultData[j][0] = rows[j].time.toISOString()
      }

      result.groups().forEach((group) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const groupPathSpecs = pathSpecs.reduce<any[]>((acc, ps, i) => {
          if (ps.path === group.name) {
            acc.push([i + 1, ps.aggregateFunction])
          }
          return acc
        }, [])
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        group.rows.forEach((row: any, i) => {
          groupPathSpecs.forEach(([fieldIndex, fieldName]) => {
            resultData[i][fieldIndex] = row[fieldName]
          })
        })
      })
      debug(`rows done ${Date.now() - start}`)
      res.json({
        context,
        range: {
          from: from.toString(),
          to: to.toString(),
        },
        values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({ path, method: aggregateMethod })),
        data: resultData,
      })
    })
    .catch((e) => console.error(e))
}

export async function getValuesFlux(
  influx: SKInflux,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void,
  req: SimpleRequest,
  res: SimpleResponse,
): Promise<ValuesResult | void> {
  const timeResolutionMillis =
    (req.query.resolution
      ? Number.parseFloat(req.query.resolution as string)
      : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000

  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z.,:]/gi, '').split(',')
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const resultData: any[] = []

  const measurements = pathSpecs
    .map(
      ({ path, aggregateFunction, queryResultName }, i) => `
  dataForContext
  |> filter(fn: (r) => r._measurement == "${path}")
  |> aggregateWindow(every: ${timeResolutionMillis.toFixed(0)}ms, fn: ${aggregateFunction})
  |> yield(name: "${queryResultName + i}")
  `,
    )
    .join('\n')
  let query = `
    dataForContext = from(bucket: "${influx.bucket}")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
  )}Z)
    |> filter(fn: (r) => r.context == "${context}")

    ${measurements}
    `

  if (pathSpecs[0].path === 'navigation.position') {
    query = `
    from(bucket: "${influx.bucket}")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    )}Z)
    |> filter(fn: (r) =>
      r.context == "${context}" and
      r._measurement == "navigation.position" and (r._field == "lat" or r._field == "lon") )
    |> aggregateWindow(every: ${timeResolutionMillis.toFixed(0)}ms, fn: first)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["_time", "lat", "lon"])
    |> sort(columns:["_time"])
    `
  }
  debug(query)

  const queryResultNames = pathSpecs.map(({ queryResultName }, i) => `${queryResultName + i}`)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const resultTimes: Record<any, number> = {}
  let i = 0
  let j = 0

  const start = Date.now()
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o: FluxResultObserver<any> = {
    next: (row: string[], tableMeta: FluxTableMetaData) => {
      if (j++ === 0) {
        debug(`start  ${Date.now() - start}`)
      }
      const time = tableMeta.get(row, '_time')
      if (resultTimes[time] === undefined) {
        resultTimes[time] = i++
        resultData.push([time])
      }
      const result = tableMeta.get(row, 'result')
      const value = tableMeta.get(row, '_value')
      const fieldIndex = queryResultNames.indexOf(result)
      resultData[resultTimes[time]][fieldIndex + 1] = value
      return true
    },
    error: (s: Error) => {
      console.error(s.message)
      console.error(query)
      res.status(500)
      res.json(s)
    },
    complete: () => {
      debug(`complete ${Date.now() - start}`)
      res.json({
        context,
        range: {
          from: from.toString(),
          to: to.toString(),
        },
        values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({ path, method: aggregateMethod })),
        data: resultData,
      })
    },
  }
  influx.queryApi.queryRows(query, o)
}

function getContext(contextFromQuery: string, selfId: string) {
  if (!contextFromQuery || contextFromQuery === 'vessels.self' || contextFromQuery === 'self') {
    return `vessels.${selfId}`
  }
  return contextFromQuery.replace(/ /gi, '')
}

interface PathSpec {
  path: string
  queryResultName: string
  aggregateMethod: string
  aggregateFunction: string
}

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':')
  let aggregateMethod = parts[1] || 'average'
  if (parts[0] === 'navigation.position') {
    aggregateMethod = 'first'
  }
  return {
    path: parts[0],
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction: (functionForAggregate[aggregateMethod] as string) || 'mean()',
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function outputPositionsGpx(rows: any[], context: string, res: SimpleResponse) {
  let responseBody = `<?xml version="1.0" encoding="UTF-8" ?>
  <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="signalk-to-influxdb2">
  <metadata><author>${context}</author></metadata>
  <trk>`
  let inSegment = false
  rows.forEach((p) => {
    if (p.lat != null && p.lon != null) {
      if (!inSegment) {
        responseBody += '\n<trseg>'
        inSegment = true
      }
      responseBody += `<trkpt lat="${p.lat}" lon="${p.lon}"><time>${p.time.toISOString()}</time></trkpt>`
    } else {
      if (inSegment) {
        responseBody += '</trseg>'
        inSegment = false
      }
    }
  })
  if (inSegment) responseBody += '</trseg>'
  responseBody += `
  </trk>
  </gpx>`
  res.header('Content-Type', 'application/xml')
  res.status(200)
  res.send(responseBody)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function outputPositionsJson(
  rows: any[],
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  res: SimpleResponse,
) {
  const resultData = rows.map((row) => {
    return [row.time.toISOString(), [row.lon, row.lat]]
  })
  res.json({
    context,
    range: {
      from: from.toString(),
      to: to.toString(),
    },
    values: [{ path: 'navigation.position', method: 'first' }],
    data: resultData,
  })
}

function outputError(res: SimpleResponse, status: number, detail: string) {
  res.status(status)
  res.json({
    status: status.toString(),
    detail: detail,
  })
}

const functionForAggregate: { [key: string]: string } = {
  average: 'mean',
  min: 'min',
  max: 'max',
  first: 'first',
}

type FromToContextRequest = Request<
  unknown,
  unknown,
  unknown,
  {
    from: string
    to: string
    context: string
    format: string
  }
>

const getRequestParams = ({ query }: FromToContextRequest, selfId: string) => {
  try {
    const from = ZonedDateTime.parse(query['from'])
    const to = ZonedDateTime.parse(query['to'])
    const format = query['format']
    return { from, to, format, context: getContext(query.context, selfId) }
  } catch (e: unknown) {
    throw new Error(`Error extracting from/to query parameters from ${JSON.stringify(query)}`)
  }
}
