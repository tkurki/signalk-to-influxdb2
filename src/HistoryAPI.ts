import { DateTimeFormatter, ZonedDateTime } from '@js-joda/core'

import { Request, Response, Router } from 'express'
import { SKInflux } from './influx'
import { FluxResultObserver, FluxTableMetaData } from '@influxdata/influxdb-client'

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  influx: SKInflux,
  selfId: string,
  debug: (k: string) => void,
) {
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context } = getFromToContext(req as FromToContextRequest, selfId)
    getValues(influx, context, from, to, debug, req, res)
  })
  router.get('/signalk/v1/history/contexts', (req: Request, res: Response) => getContexts(influx, res))
  router.get('/signalk/v1/history/paths', (req: Request, res: Response) => {
    const { from, to } = getFromToContext(req as FromToContextRequest, selfId)
    getPaths(influx, from, to, res)
  })
}

async function getContexts(influx: SKInflux, res: Response) {
  influx.queryApi
    .collectRows(
      `
  import "influxdata/influxdb/v1"
  v1.tagValues(bucket: "signalk_bucket", tag: "context")
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ValuesResultRow = any[]

async function getValues(
  influx: SKInflux,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void,
  req: Request,
  res: Response,
): Promise<ValuesResult | void> {
  const timeResolutionMillis =
    (req.query.resolution
      ? Number.parseFloat(req.query.resolution as string)
      : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000

  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z.,:]/gi, '').split(',')
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression)
  const valuesResult: ValuesResult = {
    context,
    range: {
      from: from.toString(),
      to: to.toString(),
    },
    values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({ path, method: aggregateMethod })),
    data: [],
  }

  const measurementsOrClause = pathSpecs.map(({ path }) => `r._measurement == "${path}"`).join(' or ')
  let query = `
    from(bucket: "${influx.bucket}")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
  )}Z)
    |> filter(fn: (r) =>
      r.context == "${context}" and
      ${measurementsOrClause} and
      r._field == "value"
    )
    |> aggregateWindow(every: ${timeResolutionMillis.toFixed(0)}ms, fn: ${pathSpecs[0].aggregateFunction})
    |> pivot(rowKey: ["_time"], columnKey: ["_measurement"], valueColumn: "_value")
    `

  if (pathSpecs[0].path === 'navigation.position') {
    query = `
    lat =
    from(bucket: "signalk_bucket")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    )}Z)
    |> filter(fn: (r) =>
      r.context == "${context}" and
      r._measurement == "navigation.position" and r._field == "lat")
    |> drop(columns: ["s2_cell_id"])
    |> aggregateWindow(every: ${timeResolutionMillis.toFixed(0)}ms, fn: first)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

    lon =
    from(bucket: "signalk_bucket")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    )}Z)
    |> filter(fn: (r) =>
      r.context == "${context}" and
      r._measurement == "navigation.position" and r._field == "lon")
    |> drop(columns: ["s2_cell_id"])
    |> aggregateWindow(every: ${timeResolutionMillis.toFixed(0)}ms, fn: first)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

    join(tables: {lat, lon}, on: ["_time"])
    |> keep(columns: ["_time", "lat", "lon"])
    `
  }
  debug(query)

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o: FluxResultObserver<any> = {
    next: (row: string[], tableMeta: FluxTableMetaData) => {
      const time = tableMeta.get(row, '_time')
      const dataRow = [time, ...pathSpecs.map((pathSpec) => pathSpec.extractValue(pathSpec.path, row, tableMeta))]
      valuesResult.data.push(dataRow)
      return true
    },
    error: (s: Error) => {
      console.error(s.message)
      console.error(query)
      res.status(500)
      res.json(s)
    },
    complete: () => res.json(valuesResult),
  }
  influx.queryApi.queryRows(query, o)
}

function getContext(contextFromQuery: string, selfId: string) {
  if (!contextFromQuery || contextFromQuery === 'vessels.self' || contextFromQuery === 'self') {
    return `vessels.${selfId}`
  }
  return contextFromQuery.replace(/ /gi, '')
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ExtractValue = (path: string, row: string[], tableMeta: FluxTableMetaData) => any

interface PathSpec {
  path: string
  aggregateMethod: string
  aggregateFunction: string
  extractValue: ExtractValue
}
const EXTRACT_POSITION = (path: string, row: string[], tableMeta: FluxTableMetaData) => [
  tableMeta.get(row, 'lon'),
  tableMeta.get(row, 'lat'),
]
const EXTRACT_NUMBER = (path: string, row: string[], tableMeta: FluxTableMetaData) => tableMeta.get(row, path)

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':')
  let aggregateMethod = parts[1] || 'average'
  let extractValue: ExtractValue = EXTRACT_NUMBER
  if (parts[0] === 'navigation.position') {
    aggregateMethod = 'first'
    extractValue = EXTRACT_POSITION
  }
  return {
    path: parts[0],
    aggregateMethod,
    extractValue,
    aggregateFunction: (functionForAggregate[aggregateMethod] as string) || 'mean()',
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
  }
>

const getFromToContext = ({ query }: FromToContextRequest, selfId: string) => {
  try {
    const from = ZonedDateTime.parse(query['from'])
    const to = ZonedDateTime.parse(query['to'])
    return { from, to, context: getContext(query.context, selfId) }
  } catch (e: unknown) {
    throw new Error(`Error extracting from/to query parameters from ${JSON.stringify(query)}`)
  }
}
