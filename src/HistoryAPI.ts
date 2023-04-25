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
  router.get(
    "/signalk/v1/history/contexts", (req: Request, res: Response) => getContexts(influx, res))
  // router.get(
  //   "/signalk/v1/history/paths",
}

async function getContexts(
  influx: SKInflux,
  res: Response
) {
  influx.queryApi.collectRows(`
  import "influxdata/influxdb/v1"
  v1.tagValues(bucket: "signalk_bucket", tag: "context")
  `, (row, tableMeta) => {
    return tableMeta.get(row, '_value')
  }).then(r => res.json(r))
}

async function getPaths(
  influx: SKInflux,
  from: ZonedDateTime,
  to: ZonedDateTime,
  debug: (s: string) => void,
  req: Request,
): Promise<string[]> {
  // const query = `SHOW MEASUREMENTS`;
  // console.log(query);
  // return influx.then((i) => i.query(query)).then((d) => d.map((r:any) => r.name));
  return Promise.resolve(['navigation.speedOverGround'])
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
  const timeResolutionSeconds = req.query.resolution
    ? Number.parseFloat(req.query.resolution as string)
    : (to.toEpochSecond() - from.toEpochSecond()) / 500

  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z\.,\:]/gi, '').split(',')
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
  const o: FluxResultObserver<any> = {
    next: (row: string[], tableMeta: FluxTableMetaData) => {
      const time = tableMeta.get(row, '_time')
      const dataRow = pathSpecs.map(({ path }) => ({
        time,
        value: tableMeta.get(row, path),
      }))
      valuesResult.data.push(dataRow)
      return true
    },
    error: (s: Error) => {
      res.status(500)
      res.json(s)
    },
    complete: () => res.json(valuesResult),
  }

  const measurementsOrClause = pathSpecs.map(({ path }) => `r._measurement == "${path}"`).join(' or ')
  const query = `
    from(bucket: "${influx.bucket}")
    |> range(start: ${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z, stop: ${to.format(
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
  )}Z)
    |> filter(fn: (r) =>
      r.context == "${context}" and
      ${measurementsOrClause} and
      r._field == "value"
    )
    |> aggregateWindow(every: ${timeResolutionSeconds.toFixed(0)}s, fn: ${pathSpecs[0].aggregateFunction})
    |> pivot(rowKey: ["_time"], columnKey: ["_measurement"], valueColumn: "_value")
    `
  debug(query)
  console.log(query)
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
  aggregateMethod: string
  aggregateFunction: string
  extractValue: (x: any) => any
}

interface WithValue {
  value?: any
}
type ExtractValue = (r: WithValue) => any
const EXTRACT_POSITION = (r: WithValue) => {
  if (r.value) {
    const position = JSON.parse(r.value)
    return [position.longitude, position.latitude]
  }
  return null
}
const EXTRACT_NUMBER = (r: WithValue) => Number(r.value)

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
  const from = ZonedDateTime.parse(query['from'])
  const to = ZonedDateTime.parse(query['to'])
  return { from, to, context: getContext(query.context, selfId) }
}
