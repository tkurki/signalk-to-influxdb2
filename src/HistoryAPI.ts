import { DateTimeFormatter, LocalDate, ZoneId, ZonedDateTime } from '@js-joda/core'

import { Request, Response, Router } from 'express'
import { SKInflux } from './influx'
import { InfluxDB as InfluxV1 } from 'influx'
import { FluxResultObserver, FluxTableMetaData } from '@influxdata/influxdb-client'
import { Brand, Context, Path, Timestamp } from '@signalk/server-api'

type AggregateMethod = Brand<string, 'aggregatemethod'>

type ValueList = {
  path: Path
  method: AggregateMethod
}[]

interface DataResult {
  values: ValueList
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
}

export interface ValuesResponse extends DataResult {
  context: Context
  range: {
    from: Timestamp
    to: Timestamp
  }
}

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

export function getPositions(
  v1Client: InfluxV1,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  debug: (s: string) => void,
): Promise<DataResult> {
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

  return v1Client.query(query).then(toDataResult)
}

export function getValues(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  format: string,
  debug: (s: string) => void,
  req: SimpleRequest,
  res: SimpleResponse,
) {
  const timeResolutionMillis =
    (req.query.resolution
      ? Number.parseFloat(req.query.resolution as string)
      : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000
  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z.,:]/gi, '').split(',')
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression)

  const positionPathSpecs = pathSpecs.filter(({ path }) => path === 'navigation.position').slice(0, 1)
  const positionResult = positionPathSpecs.length
    ? getPositions(influx.v1Client, context, from, to, timeResolutionMillis, debug)
    : Promise.resolve({
        values: [],
        data: [],
      })

  const nonPositionPathSpecs = pathSpecs.filter(({ path }) => path !== 'navigation.position')
  const nonPositionResult: Promise<DataResult> = nonPositionPathSpecs.length
    ? getNumericValues(influx, context, from, to, timeResolutionMillis, nonPositionPathSpecs, format, debug)
    : Promise.resolve({
        values: [],
        data: [],
      })

  return Promise.all([positionResult, nonPositionResult])
    .then(([positionResult, nonPositionResult]) => {
      if (format === 'gpx' && pathSpecs[0]?.path === 'navigation.position') {
        outputPositionsGpx(positionResult, context, res)
      } else {
        if (
          positionResult.data.length > 0 &&
          nonPositionResult.data.length > 0 &&
          positionResult.data.length !== nonPositionResult.data.length
        ) {
          throw new Error('Query result lengths do not match')
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let data: any[] = []
        let values: ValueList = []
        if (positionResult.data.length > 0) {
          data = positionResult.data
          values = positionResult.values
          if (nonPositionResult.data.length) {
            values = values.concat(nonPositionResult.values)
            // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any
            nonPositionResult.data.forEach(([ts, ...numericValues]: any[], i: number) => data[i].push(...numericValues))
          }
        } else {
          data = nonPositionResult.data
          values = nonPositionResult.values
        }
        const result: ValuesResponse = {
          context,
          range: {
            from: from.toString() as Timestamp,
            to: to.toString() as Timestamp,
          },
          values,
          data,
        }
        res.json(result)
      }
    })
    .catch((reason) => {
      res.status(500)
      res.json({ error: reason.toString() })
    })
}

function getNumericValues(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  format: string,
  debug: (s: string) => void,
): Promise<DataResult> {
  const start = Date.now()

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

  return influx.v1Client.query(query).then((result) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rows = result as any[]
    debug(`got ${rows.length} rows in ${Date.now() - start}ms`)
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
    debug(`rows done ${Date.now() - start}ms`)
    return {
      values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({ path, method: aggregateMethod })),
      data: resultData,
    }
  })
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
          from: from.toString() as Timestamp,
          to: to.toString() as Timestamp,
        },
        values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({ path, method: aggregateMethod })),
        data: resultData,
      })
    },
  }
  influx.queryApi.queryRows(query, o)
}

function getContext(contextFromQuery: string, selfId: string): Context {
  if (!contextFromQuery || contextFromQuery === 'vessels.self' || contextFromQuery === 'self') {
    return `vessels.${selfId}` as Context
  }
  return contextFromQuery.replace(/ /gi, '') as Context
}

interface PathSpec {
  path: Path
  queryResultName: string
  aggregateMethod: AggregateMethod
  aggregateFunction: string
}

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':')
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod
  if (parts[0] === 'navigation.position') {
    aggregateMethod = 'first' as AggregateMethod
  }
  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction: (functionForAggregate[aggregateMethod] as string) || 'mean()',
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function outputPositionsGpx(data: DataResult, context: string, res: SimpleResponse) {
  let responseBody = `<?xml version="1.0" encoding="UTF-8" ?>
  <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="signalk-to-influxdb2">
  <metadata><author>${context}</author></metadata>
  <trk>`
  let inSegment = false
  data.data.forEach((p: [Timestamp, [number, number]]) => {
    const [time, position] = p
    const [lon, lat] = position
    if (lat !== null && lon !== null) {
      if (!inSegment) {
        responseBody += '\n<trseg>'
        inSegment = true
      }
      responseBody += `<trkpt lat="${lat}" lon="${lon}"><time>${time}</time></trkpt>`
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
function toDataResult(rows: any[]): DataResult {
  const resultData = rows.map((row) => {
    return [row.time.toISOString(), [row.lon, row.lat]]
  })
  return {
    values: [{ path: 'navigation.position' as Path, method: 'first' as AggregateMethod }],
    data: resultData,
  }
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
    const context: Context = getContext(query.context, selfId)
    return { from, to, format, context }
  } catch (e: unknown) {
    throw new Error(`Error extracting from/to query parameters from ${JSON.stringify(query)}`)
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getDailyLogData(influx: SKInflux, selfId: string, debug: (...args: any) => void) {
  return new Promise<{ length: number }>((resolve) => {
    const startOfToday = LocalDate.now().atStartOfDay().atZone(ZoneId.of('UTC'))
    getValues(
      influx,
      `vessels.${selfId}` as Context,
      startOfToday,
      ZonedDateTime.now(),
      '',
      debug,
      {
        query: { paths: 'navigation.position', resolution: `${60}` },
      },
      {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-empty-function
        status: function (s: number): void {},
        // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any
        json: (result: any) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const validData = result.data.filter((d: any) => Array.isArray(d) && d[1][0] !== null && d[1][1] !== null)
          resolve(trackStats(validData))
        },
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        header: function (n: string, v: string): void {
          throw new Error('Function not implemented.')
        },
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        send: function (c: string): void {
          throw new Error('Function not implemented.')
        },
      },
    )
  })
}

const R = 6371 * 1000 // Earth's radius in meters

function haversineDistance([lon1, lat1]: number[], [lon2, lat2]: number[]) {
  const dLat = lat2 - lat1
  const dLon = lon2 - lon1
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2)

  return 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function trackStats(track: any[][]) {
  if (track.length === 0) {
    return {
      length: 0,
    }
  }
  let previousPoint = [toRadians(track[0][1][0]), toRadians(track[0][1][1])]
  return {
    length:
      track.slice(1).reduce((acc, trackPoint) => {
        const thisPoint = [toRadians(trackPoint[1][0]), toRadians(trackPoint[1][1])]
        acc += haversineDistance(previousPoint, thisPoint)
        previousPoint = thisPoint
        return acc
      }, 0) * R,
  }
}

function toRadians(degrees: number) {
  return (degrees * Math.PI) / 180
}
