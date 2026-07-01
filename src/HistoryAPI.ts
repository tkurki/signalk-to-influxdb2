import { DateTimeFormatter, LocalDate, ZoneId, ZonedDateTime } from '@js-joda/core'

import { SKInflux } from './influx'
import { InfluxDB as InfluxV1 } from 'influx'
import { Context, Path, SourceRef, Timestamp } from '@signalk/server-api'
import {
  AggregateMethod,
  DataRow,
  ValueList,
  ValuesResponse,
  HistoryApi,
  ValuesRequest,
  PathsRequest,
  ContextsRequest,
  PathsResponse,
  ContextsResponse,
} from '@signalk/server-api/history'

export type DataResult = Omit<ValuesResponse, 'context' | 'range'>
type SourcePolicy = 'preferred' | 'all'

const DEFAULT_EMA_PERIOD = 5

function resolveEmaParams(spec: PathSpec): { period: number; alpha: number } {
  const rawParam = spec.parameters.length > 0 ? Number.parseFloat(spec.parameters[0]) : Number.NaN

  if (Number.isFinite(rawParam) && rawParam > 0 && rawParam < 1) {
    const alpha = rawParam
    const period = 2 / alpha - 1
    return { period, alpha }
  }

  const period = Number.isFinite(rawParam) && rawParam > 0 ? rawParam : DEFAULT_EMA_PERIOD
  const alpha = 2 / (period + 1)
  return { period, alpha }
}

function makeArray(d1: number, d2: number) {
  const arr = []
  for (let i = 0; i < d1; i++) {
    arr.push(new Array(d2))
  }
  return arr
}

export class InfluxHistoryProvider implements HistoryApi {
  constructor(private influx: SKInflux, private selfId: string, private debug: (k: string) => void) {}

  async getValues(query: ValuesRequest): Promise<ValuesResponse> {
    const { from, to } = getTimeRange(query)
    const context = ((query.context === 'vessels.self' ? `vessels.${this.selfId}` : query.context) ||
      `vessels.${this.selfId}`) as Context
    const resolution = query.resolution || (to.toEpochSecond() - from.toEpochSecond()) / 1000

    // Convert pathSpecs to the format expected by internal functions
    let pathSpecs: PathSpec[] = query.pathSpecs.map((spec) => {
      // sourceRef is part of the History API spec but may be absent from the
      // installed @signalk/server-api typings, hence the cast.
      const sourceRef = (spec as { sourceRef?: SourceRef }).sourceRef
      return {
        path: spec.path,
        queryResultName: spec.path.replace(/\./g, '_'),
        aggregateMethod: spec.aggregate,
        aggregateFunction: (functionForAggregate[spec.aggregate] as string) || 'mean',
        parameters: spec.parameter || [],
        ...(sourceRef ? { sourceRef } : {}),
      }
    })

    const sourcePolicy = (query as ValuesRequest & { sourcePolicy?: SourcePolicy }).sourcePolicy
    if (sourcePolicy === 'preferred') {
      throw new Error(
        "sourcePolicy='preferred' is not implemented by signalk-to-influxdb2; omit sourcePolicy for provider default behavior or use sourcePolicy='all'",
      )
    }

    const sourcePolicyAll = sourcePolicy === 'all'
    if (sourcePolicyAll) {
      pathSpecs = await expandPathSpecsBySource(this.influx, context, from, to, pathSpecs, this.debug)
    }

    const positionPathSpecs = pathSpecs.filter(({ path }) => path === 'navigation.position')
    const requestedPositionPathSpecs = sourcePolicyAll ? positionPathSpecs : positionPathSpecs.slice(0, 1)
    const nonPositionPathSpecs = pathSpecs.filter(({ path }) => path !== 'navigation.position')
    const needsCollation = nonPositionPathSpecs.length > 0 && requestedPositionPathSpecs.length > 0

    // Calculate extended query window for SMA and EMA
    const maxSmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === 'sma') {
        const windowSize = spec.parameters.length > 0 ? parseInt(spec.parameters[0], 10) : 5
        return Math.max(max, windowSize)
      }
      return max
    }, 0)

    // EMA needs more history: initial SMA period (3x EMA period) + EMA period
    const maxEmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === 'ema') {
        const { period } = resolveEmaParams(spec)
        // Need 3x period for initial SMA + 1x period for EMA itself
        return Math.max(max, Math.ceil(period * 4))
      }
      return max
    }, 0)

    const maxWindow = Math.max(maxSmaWindow, maxEmaWindow)

    // Extend the start time backwards to get enough data for SMA/EMA calculation
    const extendedFrom = maxWindow > 0 ? from.minusNanos(maxWindow * resolution * 1000 * 1_000_000) : from

    const positionResult = requestedPositionPathSpecs.length
      ? sourcePolicyAll
        ? getPositionValues(
            this.influx.v1Client,
            context,
            from,
            to,
            resolution * 1000,
            requestedPositionPathSpecs,
            needsCollation,
            this.debug,
          )
        : getPositions(
            this.influx.v1Client,
            context,
            from,
            to,
            resolution * 1000,
            needsCollation,
            this.debug,
            requestedPositionPathSpecs[0].sourceRef,
          )
      : Promise.resolve({
          values: [],
          data: [],
        })

    const nonPositionResult: Promise<DataResult> = nonPositionPathSpecs.length
      ? getNumericValues(
          this.influx,
          context,
          extendedFrom,
          to,
          resolution * 1000,
          nonPositionPathSpecs,
          needsCollation,
          '',
          this.debug,
        )
      : Promise.resolve({
          values: [],
          data: [],
        })

    const [posResult, nonPosResult] = await Promise.all([positionResult, nonPositionResult])

    // Apply SMA and EMA post-processing if needed
    let processedNonPosResult = nonPosResult
    if ((maxSmaWindow > 0 || maxEmaWindow > 0) && nonPosResult.data.length > 0) {
      processedNonPosResult = applyMovingAveragePostProcessing(
        nonPosResult,
        nonPositionPathSpecs,
        from.toString() as Timestamp,
      )
    }

    if (sourcePolicyAll) {
      const { values, data } = mergeResultsByTimestamp(posResult, processedNonPosResult)

      return {
        context,
        range: {
          from: from.toString() as Timestamp,
          to: to.toString() as Timestamp,
        },
        values,
        data,
      }
    }

    if (
      posResult.data.length > 0 &&
      processedNonPosResult.data.length > 0 &&
      posResult.data.length !== processedNonPosResult.data.length
    ) {
      throw new Error('Query result lengths do not match')
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let data: any[] = []
    let values: ValueList = []

    if (posResult.data.length > 0) {
      data = posResult.data
      values = posResult.values
      if (processedNonPosResult.data.length) {
        values = values.concat(processedNonPosResult.values)
        // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any
        processedNonPosResult.data.forEach(([ts, ...numericValues]: any[], i: number) => {
          let hasNonNulls = data[i][1][0] !== null //first coordinate of position is not null
          numericValues.forEach((value) => (hasNonNulls = hasNonNulls || value !== null))
          if (hasNonNulls) {
            data[i].push(...numericValues)
          }
        })
        //filter out rows with all null values
        data = data.filter((row) => row.length !== pathSpecs.length)
      } else {
        //only positions, check that first coordinate is not null
        data = data.filter((row) => row[1][0] !== null)
      }
    } else {
      //filter out rows with all null values
      data = processedNonPosResult.data.filter((row) => row.slice(1).some((value) => value !== null))
      values = processedNonPosResult.values
    }

    return {
      context,
      range: {
        from: from.toString() as Timestamp,
        to: to.toString() as Timestamp,
      },
      values,
      data,
    }
  }

  async getContexts(): Promise<ContextsResponse> {
    const contexts = await this.influx.queryApi.collectRows(
      `
  import "influxdata/influxdb/v1"
  v1.tagValues(bucket: "${this.influx.bucket}", tag: "context")
  `,
      (row, tableMeta) => {
        return tableMeta.get(row, '_value')
      },
    )
    return contexts as Context[]
  }

  async getPaths(): Promise<PathsResponse> {
    const paths = await this.influx.queryApi.collectRows(
      `
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "${this.influx.bucket}")`,
      (row, tableMeta) => {
        return tableMeta.get(row, '_value')
      },
    )
    return paths as Path[]
  }
}

function getTimeRange(query: ValuesRequest | PathsRequest | ContextsRequest): {
  from: ZonedDateTime
  to: ZonedDateTime
} {
  if ('duration' in query && query.duration !== undefined) {
    const durationMs = typeof query.duration === 'number' ? query.duration : query.duration.total('milliseconds')

    if ('from' in query && query.from !== undefined) {
      const from = ZonedDateTime.parse(query.from.toString())
      const to = from.plusNanos(durationMs * 1_000_000)
      return { from, to }
    } else if ('to' in query && query.to !== undefined) {
      const to = ZonedDateTime.parse(query.to.toString())
      const from = to.minusNanos(durationMs * 1_000_000)
      return { from, to }
    } else {
      const to = ZonedDateTime.now(ZoneId.UTC)
      const from = to.minusNanos(durationMs * 1_000_000)
      return { from, to }
    }
  } else if ('from' in query && query.from !== undefined) {
    const from = ZonedDateTime.parse(query.from.toString())
    if ('to' in query && query.to !== undefined) {
      const to = ZonedDateTime.parse(query.to.toString())
      return { from, to }
    } else {
      const to = ZonedDateTime.now(ZoneId.UTC)
      return { from, to }
    }
  }

  throw new Error('Invalid time range parameters')
}

function applyMovingAveragePostProcessing(
  result: DataResult,
  pathSpecs: PathSpec[],
  requestedFromTimestamp: Timestamp,
): DataResult {
  const data = result.data

  // Find paths that require SMA or EMA processing
  const smaIndices = pathSpecs.map((spec, idx) => ({ spec, idx })).filter(({ spec }) => spec.aggregateMethod === 'sma')

  const emaIndices = pathSpecs.map((spec, idx) => ({ spec, idx })).filter(({ spec }) => spec.aggregateMethod === 'ema')

  if (smaIndices.length === 0 && emaIndices.length === 0) {
    // No processing needed, just trim to requested time range
    const requestedFromMs = new Date(requestedFromTimestamp).toISOString()
    const trimmedData = data.filter((row) => (row[0] as string) >= requestedFromMs)
    return {
      values: result.values,
      data: trimmedData as DataRow[],
    }
  }

  // Process each column
  const processedData = data.map((row) => [...row])

  // Calculate SMA for each SMA column
  smaIndices.forEach(({ spec, idx }) => {
    const windowSize = spec.parameters.length > 0 ? parseInt(spec.parameters[0], 10) : 5
    const columnIndex = idx + 1 // +1 because first column is timestamp

    for (let i = 0; i < processedData.length; i++) {
      const startIdx = Math.max(0, i - windowSize + 1)
      const values: number[] = []

      // Collect non-null values in the window
      for (let j = startIdx; j <= i; j++) {
        const value = data[j][columnIndex]
        if (value !== null && value !== undefined && typeof value === 'number') {
          values.push(value)
        }
      }

      // Calculate average if we have values
      if (values.length > 0) {
        const sum = values.reduce((acc, val) => acc + val, 0)
        processedData[i][columnIndex] = sum / values.length
      } else {
        processedData[i][columnIndex] = null
      }
    }
  })

  // Calculate EMA for each EMA column
  emaIndices.forEach(({ spec, idx }) => {
    const { period, alpha } = resolveEmaParams(spec)
    const columnIndex = idx + 1 // +1 because first column is timestamp
    const smoothingFactor = alpha

    // Use 3x period for initial SMA to seed the EMA
    const initialSmaWindow = Math.max(1, Math.round(period * 3))
    let ema: number | null = null

    for (let i = 0; i < processedData.length; i++) {
      const currentValue = data[i][columnIndex]

      if (currentValue === null || currentValue === undefined || typeof currentValue !== 'number') {
        processedData[i][columnIndex] = ema // Carry forward last EMA when current value is null
        continue
      }

      if (ema === null) {
        // Initialize EMA with SMA of first N values
        if (i >= initialSmaWindow - 1) {
          const startIdx = Math.max(0, i - initialSmaWindow + 1)
          const values: number[] = []

          for (let j = startIdx; j <= i; j++) {
            const value = data[j][columnIndex]
            if (value !== null && value !== undefined && typeof value === 'number') {
              values.push(value)
            }
          }

          if (values.length > 0) {
            const sum = values.reduce((acc, val) => acc + val, 0)
            ema = sum / values.length
            processedData[i][columnIndex] = ema
          } else {
            processedData[i][columnIndex] = null
          }
        } else {
          // Not enough data yet for initial SMA
          processedData[i][columnIndex] = null
        }
      } else {
        // Calculate EMA: EMA_t = α * Value_t + (1 - α) * EMA_{t-1}
        ema = smoothingFactor * currentValue + (1 - smoothingFactor) * ema
        processedData[i][columnIndex] = ema
      }
    }
  })

  // Trim to requested time range
  // Convert requested timestamp to millisecond precision for comparison
  const requestedFromMs = new Date(requestedFromTimestamp).toISOString()
  const trimmedData = processedData.filter((row) => (row[0] as string) >= requestedFromMs)

  return {
    values: result.values,
    data: trimmedData as DataRow[],
  }
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

export function getPositions(
  v1Client: InfluxV1,
  context: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  needsCollation: boolean,
  debug: (s: string) => void,
  sourceRef?: SourceRef,
): Promise<DataResult> {
  const sourceClause = sourceRef ? `\n    and\n    "source" = '${sourceRef}'` : ''

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
   time <= '${to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'${sourceClause}
  group by time(${timeResolutionMillis}ms)${!needsCollation ? ' fill(none)' : ''}`

  debug(query)

  return v1Client.query(query).then((rows) => toDataResult(rows, sourceRef))
}

async function expandPathSpecsBySource(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  pathSpecs: PathSpec[],
  debug: (s: string) => void,
): Promise<PathSpec[]> {
  const expanded: PathSpec[] = []

  for (const pathSpec of pathSpecs) {
    if (pathSpec.sourceRef) {
      expanded.push(pathSpec)
      continue
    }

    const sourceRefs = await getSourceRefsForPath(influx, context, from, to, pathSpec.path, debug)
    sourceRefs.forEach((sourceRef) => expanded.push({ ...pathSpec, sourceRef }))
  }

  return expanded
}

function getSourceRefsForPath(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  path: Path,
  debug: (s: string) => void,
): Promise<SourceRef[]> {
  const countField = path === 'navigation.position' ? 'lat' : 'value'
  const query = `
  select
    count(${countField}) as sample_count
  from
    "${path}"
  where
    "context" = '${context}'
    and
    time >= '${from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
    and
   time <= '${to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'
  group by "source"`

  debug(query)

  return influx.v1Client.query(query).then((result) => sourceRefsFromInfluxResult(result as InfluxSourceResult))
}

interface InfluxSourceRow {
  source?: unknown
}

interface InfluxSourceGroup {
  tags?: {
    source?: unknown
  }
  rows?: InfluxSourceRow[]
}

type InfluxSourceResult = InfluxSourceRow[] & {
  groups?: () => InfluxSourceGroup[]
}

function sourceRefsFromInfluxResult(result: InfluxSourceResult): SourceRef[] {
  const sourceRefs = new Set<SourceRef>()

  result.forEach((row) => {
    if (typeof row.source === 'string') {
      sourceRefs.add(row.source as SourceRef)
    }
  })

  if (typeof result.groups === 'function') {
    result.groups().forEach((group) => {
      if (typeof group.tags?.source === 'string') {
        sourceRefs.add(group.tags.source as SourceRef)
      }
      group.rows?.forEach((row) => {
        if (typeof row.source === 'string') {
          sourceRefs.add(row.source as SourceRef)
        }
      })
    })
  }

  return Array.from(sourceRefs).sort()
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
      : (to.toEpochSecond() - from.toEpochSecond()) / 1000) * 1000
  const pathExpressions = ((req.query.paths as string) || '').replace(/[^0-9a-z.,:|]/gi, '').split(',')
  const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression)

  const positionPathSpecs = pathSpecs.filter(({ path }) => path === 'navigation.position').slice(0, 1)
  const nonPositionPathSpecs = pathSpecs.filter(({ path }) => path !== 'navigation.position')
  const needsCollation = nonPositionPathSpecs.length > 0 && positionPathSpecs.length > 0

  const positionResult = positionPathSpecs.length
    ? getPositions(
        influx.v1Client,
        context,
        from,
        to,
        timeResolutionMillis,
        needsCollation,
        debug,
        positionPathSpecs[0].sourceRef,
      )
    : Promise.resolve({
        values: [],
        data: [],
      })

  const nonPositionResult: Promise<DataResult> = nonPositionPathSpecs.length
    ? getNumericValues(
        influx,
        context,
        from,
        to,
        timeResolutionMillis,
        nonPositionPathSpecs,
        needsCollation,
        format,
        debug,
      )
    : Promise.resolve({
        values: [],
        data: [],
      })

  return Promise.all([positionResult, nonPositionResult])
    .then(([positionResult, nonPositionResult]) => {
      if (format === 'gpx' && pathSpecs[0]?.path === 'navigation.position') {
        outputPositionsGpx(positionResult, context, res)
      } else {
        // Collate by timestamp (union of timestamps from both sources), not by row order.
        // This avoids mismatches when one query returns extra (all-null) rows.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const data: any[] = []
        let values: ValueList = []

        // Build timestamp -> positionValue map.
        // In InfluxQL, `group by time()` without `fill(none)` can create rows with
        // null lat/lon; treat those as missing and emit `null` in the response.
        const positionByTs = new Map<string, [number, number] | null>()
        positionResult.data.forEach((row: DataRow) => {
          const ts = row[0] as string
          const pos = row[1]
          if (!Array.isArray(pos)) {
            return
          }

          const [lon, lat] = pos
          if ((lon === null || lon === undefined) && (lat === null || lat === undefined)) {
            return
          }

          const existing = positionByTs.get(ts)
          if (!existing) {
            positionByTs.set(ts, [lon, lat])
            return
          }

          if (!Array.isArray(existing)) {
            positionByTs.set(ts, [lon, lat])
            return
          }

          const [existingLon, existingLat] = existing
          const mergedLon = (existingLon === null || existingLon === undefined) && lon != null ? lon : existingLon
          const mergedLat = (existingLat === null || existingLat === undefined) && lat != null ? lat : existingLat
          positionByTs.set(ts, [mergedLon, mergedLat])
        })

        // Build timestamp -> numericValues map, merging duplicates by filling missing values.
        const numericByTs = new Map<string, (number | null)[]>()
        nonPositionResult.data.forEach((row: DataRow) => {
          const ts = row[0] as string
          const numericValues = row.slice(1) as (number | null)[]
          const existing = numericByTs.get(ts)
          if (!existing) {
            numericByTs.set(ts, [...numericValues])
            return
          }
          for (let k = 0; k < numericValues.length; k++) {
            if (
              (existing[k] === null || existing[k] === undefined) &&
              numericValues[k] !== null &&
              numericValues[k] !== undefined
            ) {
              existing[k] = numericValues[k]
            }
          }
        })

        // Values list ordering: position (if requested) first, then numeric.
        if (positionPathSpecs.length > 0 && positionResult.values.length > 0) {
          values = values.concat(positionResult.values)
        }
        if (nonPositionPathSpecs.length > 0 && nonPositionResult.values.length > 0) {
          values = values.concat(nonPositionResult.values)
        }

        // Union timestamps from both results.
        const timestamps = Array.from(new Set<string>([...positionByTs.keys(), ...numericByTs.keys()]))
        timestamps.sort()

        const numericWidth = nonPositionPathSpecs.length
        timestamps.forEach((ts) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const row: any[] = [ts]

          if (positionPathSpecs.length > 0) {
            row.push(positionByTs.get(ts) ?? null)
          }
          if (nonPositionPathSpecs.length > 0) {
            row.push(...(numericByTs.get(ts) ?? new Array(numericWidth).fill(null)))
          }

          const hasAnyValue = row.slice(1).some((v) => {
            if (Array.isArray(v)) {
              return v.some((x) => x !== null && x !== undefined)
            }
            return v !== null && v !== undefined
          })

          if (hasAnyValue) {
            data.push(row)
          }
        })

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

// Builds the `values` descriptor list for a set of path specs, including the
// sourceRef only when one was requested for that path.
function valuesForSpecs(pathSpecs: PathSpec[]): ValueList {
  return pathSpecs.map(({ path, aggregateMethod, sourceRef }: PathSpec) => ({
    path,
    method: aggregateMethod,
    ...(sourceRef ? { sourceRef } : {}),
  }))
}

function getPositionValues(
  v1Client: InfluxV1,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  needsCollation: boolean,
  debug: (s: string) => void,
): Promise<DataResult> {
  if (pathSpecs.length <= 1) {
    return getPositions(
      v1Client,
      context,
      from,
      to,
      timeResolutionMillis,
      needsCollation,
      debug,
      pathSpecs[0]?.sourceRef,
    )
  }

  const queries = pathSpecs.map((pathSpec, index) =>
    getPositions(v1Client, context, from, to, timeResolutionMillis, needsCollation, debug, pathSpec.sourceRef).then(
      (result) => ({ result, index }),
    ),
  )

  return Promise.all(queries).then((results) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataByTs = new Map<string, any[]>()

    results.forEach(({ result, index }) => {
      result.data.forEach((row) => {
        const ts = row[0] as string
        let target = dataByTs.get(ts)
        if (!target) {
          target = new Array(pathSpecs.length + 1).fill(null)
          target[0] = ts
          dataByTs.set(ts, target)
        }
        target[index + 1] = row[1] ?? null
      })
    })

    return {
      values: valuesForSpecs(pathSpecs),
      data: Array.from(dataByTs.values()).sort((a, b) => String(a[0]).localeCompare(String(b[0]))) as DataRow[],
    }
  })
}

function mergeResultsByTimestamp(posResult: DataResult, nonPosResult: DataResult): DataResult {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const dataByTs = new Map<string, any[]>()
  const values = posResult.values.concat(nonPosResult.values)
  const valueCount = values.length

  addResultRows(dataByTs, posResult, 0, valueCount)
  addResultRows(dataByTs, nonPosResult, posResult.values.length, valueCount)

  const data = Array.from(dataByTs.values())
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .filter((row) => row.slice(1).some(hasMeaningfulValue)) as DataRow[]

  return { values, data }
}

function addResultRows(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dataByTs: Map<string, any[]>,
  result: DataResult,
  offset: number,
  valueCount: number,
) {
  result.data.forEach((sourceRow) => {
    const ts = sourceRow[0] as string
    let target = dataByTs.get(ts)

    if (!target) {
      target = new Array(valueCount + 1).fill(null)
      target[0] = ts
      dataByTs.set(ts, target)
    }

    for (let sourceIndex = 1; sourceIndex < sourceRow.length; sourceIndex++) {
      target[offset + sourceIndex] = sourceRow[sourceIndex] ?? null
    }
  })
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function hasMeaningfulValue(value: any): boolean {
  if (Array.isArray(value)) {
    return value.some(hasMeaningfulValue)
  }
  return value !== null && value !== undefined
}

function getNumericValues(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  needsCollation: boolean,
  format: string,
  debug: (s: string) => void,
): Promise<DataResult> {
  const distinctSourceRefs = new Set(pathSpecs.map((ps) => ps.sourceRef))
  const distinctPaths = new Set(pathSpecs.map((ps) => ps.path))

  // Legacy unfiltered queries keep the old single-query behaviour. A
  // source-specific query with multiple measurements is split below because
  // InfluxQL result collation across measurements is not stable enough for
  // reconstructing the requested column order.
  const sourceRef = pathSpecs[0]?.sourceRef
  if (distinctSourceRefs.size <= 1 && (sourceRef === undefined || distinctPaths.size === 1)) {
    return querySourceGroup(
      influx,
      context,
      from,
      to,
      timeResolutionMillis,
      pathSpecs,
      needsCollation,
      debug,
      sourceRef,
    )
  }

  // Mixed or source-specific multi-path queries are run per source/path group
  // and collated by timestamp back into the original column order.
  const groups = new Map<string, { specs: PathSpec[]; indices: number[] }>()
  pathSpecs.forEach((ps, i) => {
    const groupKey = `${ps.sourceRef ?? ''}\u0000${ps.path}`
    let group = groups.get(groupKey)
    if (!group) {
      group = { specs: [], indices: [] }
      groups.set(groupKey, group)
    }
    group.specs.push(ps)
    group.indices.push(i)
  })

  const groupPromises = Array.from(groups.values()).map((group) =>
    querySourceGroup(
      influx,
      context,
      from,
      to,
      timeResolutionMillis,
      group.specs,
      needsCollation,
      debug,
      group.specs[0].sourceRef,
    ).then((result) => ({ result, indices: group.indices })),
  )

  return Promise.all(groupPromises).then((groupResults) => {
    const allTimestamps = Array.from(
      new Set(groupResults.flatMap(({ result }) => result.data.map((row) => row[0] as string))),
    ).sort()
    const rowByTs = new Map<string, (number | null)[]>()
    allTimestamps.forEach((ts) => {
      const row: (number | null)[] = new Array(pathSpecs.length + 1).fill(null)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ;(row as any[])[0] = ts
      rowByTs.set(ts, row)
    })

    groupResults.forEach(({ result, indices }) => {
      result.data.forEach((groupRow) => {
        const ts = groupRow[0] as string
        const target = rowByTs.get(ts)
        if (!target) {
          return
        }
        indices.forEach((originalIndex, groupColumn) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          target[originalIndex + 1] = (groupRow as any[])[groupColumn + 1] ?? null
        })
      })
    })

    return {
      values: valuesForSpecs(pathSpecs),
      data: allTimestamps.map((ts) => rowByTs.get(ts)) as DataRow[],
    }
  })
}

// Runs a single InfluxQL query for path specs that share one source (or none),
// returning rows in the same column order as `pathSpecs`.
function querySourceGroup(
  influx: SKInflux,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  needsCollation: boolean,
  debug: (s: string) => void,
  sourceRef?: SourceRef,
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

  const sourceClause = sourceRef ? `\n    and\n    "source" = '${sourceRef}'` : ''

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
   time <= '${to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}Z'${sourceClause}
  group by time(${timeResolutionMillis}ms)${!needsCollation ? ' fill(none)' : ''}`
  debug(query)

  return influx.v1Client.query(query).then((result) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rows = result as any[]
    debug(`got ${rows.length} rows in ${Date.now() - start}ms`)
    const resultLength = rows.length
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
      values: valuesForSpecs(pathSpecs),
      data: resultData as DataRow[],
    }
  })
}

interface PathSpec {
  path: Path
  queryResultName: string
  aggregateMethod: AggregateMethod
  aggregateFunction: string
  parameters: string[]
  sourceRef?: SourceRef
}

// A `|sourceRef` suffix on a path expression filters that path to a single
// source, e.g. 'navigation.speedOverGround:max|n2k-on-ve.can0.115'.
function splitPathExpression(pathExpression: string): PathSpec {
  const pipeIdx = pathExpression.indexOf('|')
  let sourceRef: SourceRef | undefined
  let expr = pathExpression
  if (pipeIdx >= 0) {
    sourceRef = pathExpression.substring(pipeIdx + 1) as SourceRef
    expr = pathExpression.substring(0, pipeIdx)
  }

  const parts = expr.split(':')
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod
  if (parts[0] === 'navigation.position') {
    aggregateMethod = 'first' as AggregateMethod
  }
  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction: (functionForAggregate[aggregateMethod] as string) || 'mean()',
    parameters: [],
    ...(sourceRef ? { sourceRef } : {}),
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function outputPositionsGpx(data: DataResult, context: string, res: SimpleResponse) {
  let responseBody = `<?xml version="1.0" encoding="UTF-8" ?>
  <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="signalk-to-influxdb2">
  <metadata><author>${context}</author></metadata>
  <trk>`
  let inSegment = false
  data.data.forEach((dr) => {
    const p = dr as [Timestamp, [number, number]]
    const [time, position] = p
    const [lon, lat] = position
    if (lat !== null && lon !== null) {
      if (!inSegment) {
        responseBody += '\n<trkseg>'
        inSegment = true
      }
      responseBody += `<trkpt lat="${lat}" lon="${lon}"><time>${time}</time></trkpt>`
    } else {
      if (inSegment) {
        responseBody += '</trkseg>'
        inSegment = false
      }
    }
  })
  if (inSegment) responseBody += '</trkseg>'
  responseBody += `
  </trk>
  </gpx>`
  res.header('Content-Type', 'application/xml')
  res.status(200)
  res.send(responseBody)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function toDataResult(rows: any[], sourceRef?: SourceRef): DataResult {
  const resultData = rows.map<DataRow>((row) => {
    return [row.time.toISOString(), [row.lon, row.lat]]
  })
  return {
    values: [
      {
        path: 'navigation.position' as Path,
        method: 'first' as AggregateMethod,
        ...(sourceRef ? { sourceRef } : {}),
      },
    ],
    data: resultData,
  }
}

const functionForAggregate: { [key: string]: string } = {
  average: 'mean',
  min: 'min',
  max: 'max',
  first: 'first',
  sma: 'mean', // Use mean from DB, then apply SMA post-processing
  ema: 'mean', // Use mean from DB, then apply EMA post-processing
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
