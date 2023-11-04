/*
 * Copyright 2022 Teppo Kurki <teppo.kurki@iki.fi>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { SKContext } from '@chacal/signalk-ts'
import { HttpError, InfluxDB, Point, QueryApi, WriteApi, WriteOptions } from '@influxdata/influxdb-client'
import { BucketsAPI, DbrpsAPI, OrgsAPI } from '@influxdata/influxdb-client-apis'
import { InfluxDB as InfluxV1 } from 'influx'
import { getUnits } from '@signalk/signalk-schema'

import { Logging, QueryParams } from './plugin'
import { S2 } from 's2-geometry'

export const SELF_TAG_NAME = 'self'
export const SELF_TAG_VALUE = 'true'

export interface SKInfluxConfig {
  /**
   * Url of the InfluxDb 2 server
   *
   * @title Url
   */
  url: string
  /**
   * Authentication token
   *
   * @title Token
   */
  token: string
  /**
   * @title Organisation
   */
  org: string
  /**
   * @title Bucket
   */
  bucket: string

  /**
   * @title Store only self data
   * @default true
   * @description Store data only for "self" data, not for example AIS targets' data
   */
  onlySelf: boolean

  /**
   * @title Ignored paths
   * @default []
   * @description Paths that should be ignored and not written to InfluxDB (JS regular expressions work)
   */
  ignoredPaths: string[]

  /**
   * @title Ignored sources
   * @default []
   * @description Sources whose data should be ignored and not written to InfluxDB (JS regular expressions work)
   */
  ignoredSources: string[]

  writeOptions: Partial<WriteOptions>
}

type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>

interface PathValue {
  path: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any
}

export const influxPath = (path: string) => (path !== '' ? path : '<empty>')

enum JsValueType {
  number = 'number',
  string = 'string',
  boolean = 'boolean',
  object = 'object',
}

const VALUETYPECACHE: {
  [key: string]: JsValueType
} = {}

export class SKInflux {
  private influx: InfluxDB
  public org: string
  public bucket: string
  private writeApi: WriteApi
  public queryApi: QueryApi
  public writtenLinesCount = 0
  public failedLinesCount = 0
  public lastWriteCallbackSucceeded = false
  public url: string
  private onlySelf: boolean
  public v1Client: InfluxV1
  private ignoreStatusForPathSources: {
    [path: string]: boolean
  } = {}
  private ignoredPaths: string[]
  private ignoredSources: string[]

  constructor(config: SKInfluxConfig, private logging: Logging, triggerStatusUpdate: () => void) {
    const { org, bucket, url, onlySelf, ignoredPaths, ignoredSources } = config
    this.influx = new InfluxDB(config)
    this.org = org
    this.bucket = bucket
    this.url = url
    this.onlySelf = onlySelf
    this.ignoredPaths = ignoredPaths
    this.ignoredSources = ignoredSources
    this.writeApi = this.influx.getWriteApi(org, bucket, 'ms', {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      writeFailed: (_error, lines, _attempt, _expires) => {
        this.failedLinesCount += lines.length
        this.lastWriteCallbackSucceeded = false
        triggerStatusUpdate()
      },
      writeSuccess: (lines) => {
        this.writtenLinesCount += lines.length
        this.lastWriteCallbackSucceeded = true
        triggerStatusUpdate()
      },
    })
    this.queryApi = this.influx.getQueryApi(org)
    const parsedUrl = new URL(url)
    this.v1Client = new InfluxV1({
      host: parsedUrl.hostname,
      username: org,
      // leave password empty
      password: '',
      port: Number(parsedUrl.port),
      protocol: <'http' | 'https'>parsedUrl.protocol.slice(0, -1),
      database: bucket,
      options: {
        headers: {
          Authorization: `Token ${config.token}`,
        },
      },
    })
  }

  async init() {
    const bucketId = await ensureBucketExists(this.influx, this.org, this.bucket)
    try {
      await this.ensureV1MappingExists(bucketId)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (err: any) {
      this.logging.error('Could not verify or create v1 database mapping, history api will not work')
      this.logging.error(err)
    }
  }

  async ensureV1MappingExists(bucketId: string | undefined) {
    if (!bucketId) {
      throw new Error('No bucketid')
    }
    const dbrsApi = new DbrpsAPI(this.influx)
    const dbrs = await dbrsApi.getDBRPs({ org: this.org })
    if (!dbrs.content) {
      throw new Error('Error retrieving dbrs')
    }
    //is there mapping where v1 database name is the same as bucket
    if (!dbrs.content.find((dbr) => dbr.database === this.bucket)) {
      await dbrsApi.postDBRP({
        body: {
          org: this.org,
          database: this.bucket,
          bucketID: bucketId,
          retention_policy: 'default',
          default: true,
        },
      })
      this.logging.debug(`Created database retention policy`)
      console.log('Created database retention policy')
    }
  }

  handleValue(context: SKContext, isSelf: boolean, source: string, pathValue: PathValue) {
    if (this.isIgnored(pathValue.path, source)) {
      return
    }
    if (!this.onlySelf || isSelf) {
      const point = toPoint(context, isSelf, source, pathValue, this.logging.debug)
      if (point) {
        this.writeApi.writePoint(point)
      }
    }
  }

  flush() {
    return this.writeApi.flush()
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getValues(params: QueryParams): Promise<Array<any>> {
    return this.queryApi.collectRows(paramsToQuery(this.bucket, params))
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getSelfValues(params: Omit<QueryParams, 'context'>): Promise<Array<any>> {
    const query = paramsToQuery(this.bucket, params)
    // console.log(query)
    return this.queryApi.collectRows(query)
  }

  isIgnored(path: string, sourceRef: string): boolean {
    const pathSource = `${path}:${sourceRef}`
    let ignoreStatus = this.ignoreStatusForPathSources[pathSource]
    if (ignoreStatus === undefined) {
      ignoreStatus = this.ignoreStatusForPathSources[pathSource] = this.ignoreStatusByConfig(path, sourceRef)
    }
    return ignoreStatus
  }

  ignoreStatusByConfig(path: string, sourceRef: string): boolean {
    try {
      const ignoredByPath =
        this.ignoredPaths?.reduce<boolean>((acc, ignoredPathExp) => {
          const ignoredPathRegExp = new RegExp(ignoredPathExp.replace(/\./g, '.'))
          const ignored = ignoredPathRegExp.test(path)
          ignored && this.logging.debug(`${path} from ${sourceRef} will be ignored by ${ignoredPathRegExp}`)
          return acc || ignored
        }, false) || false
      const ignoredBySource =
        this.ignoredSources?.reduce<boolean>((acc, ignoredSourceExp) => {
          const ignoredSourceRegExp = new RegExp(ignoredSourceExp.replace(/\./g, '.'))
          const ignored = ignoredSourceRegExp.test(sourceRef)
          ignored && this.logging.debug(`${path} from ${sourceRef} will be ignored by ${ignoredSourceRegExp}`)
          return acc || ignored
        }, false) || false
      return ignoredByPath || ignoredBySource
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      this.logging.error(e.message)
      return false
    }
  }
}

const toPoint = (
  context: SKContext,
  isSelf: boolean,
  source: string,
  pathValue: PathValue,
  debug: (s: string) => void,
) => {
  const point = new Point(influxPath(pathValue.path)).tag('context', context).tag('source', source)
  if (isSelf) {
    point.tag(SELF_TAG_NAME, SELF_TAG_VALUE)
  }
  if (
    pathValue.path === 'navigation.position' &&
    typeof pathValue.value === 'object' &&
    pathValue.value !== null &&
    pathValue.value.latitude !== null &&
    pathValue.value.longitude !== null &&
    !isNaN(pathValue.value.latitude) &&
    !isNaN(pathValue.value.longitude)
  ) {
    point.floatField('lat', pathValue.value.latitude)
    point.floatField('lon', pathValue.value.longitude)
    point.tag('s2_cell_id', posToS2CellId(pathValue.value))
  } else {
    const valueType = typeFor(pathValue)
    if (pathValue.value === null) {
      return
    }
    try {
      switch (valueType) {
        case JsValueType.number:
          point.floatField('value', pathValue.value)
          break
        case JsValueType.string:
          point.stringField('value', pathValue.value)
          break
        case JsValueType.boolean:
          point.booleanField('value', pathValue.value)
          break
        case JsValueType.object:
          point.stringField('value', JSON.stringify(pathValue.value))
      }
    } catch (e) {
      debug(`Error creating point ${pathValue.path}:${pathValue.value} => ${valueType}`)
      return undefined
    }
  }
  return point
}

const typeFor = (pathValue: PathValue): JsValueType => {
  let r = VALUETYPECACHE[pathValue.path]
  if (!r) {
    r = VALUETYPECACHE[pathValue.path] = _typeFor(pathValue)
  }
  return r
}

const _typeFor = (pathValue: PathValue): JsValueType => {
  const unit = getUnits(`vessels.self.${pathValue.path}`)
  if (unit && unit !== 'RFC 3339 (UTC)') {
    return JsValueType.number
  }
  return typeof pathValue.value as JsValueType
}

const paramsToQuery = (bucket: string, params: PartialBy<QueryParams, 'context'>) => {
  const contextTagClause = params.context
    ? `and r.context == "${params.context}"`
    : `and r.${SELF_TAG_NAME} == "${SELF_TAG_VALUE}"`
  return `
  from(bucket: "${bucket}")
    |> range(start: -1y)
    |> filter(fn: (r) => r["_measurement"] == "${params.paths[0]}" ${contextTagClause})
  `
}

const posToS2CellId = (position: { latitude: number; longitude: number }) => {
  const cell = S2.S2Cell.FromLatLng({ lat: position.latitude, lng: position.longitude }, 10)
  return S2.keyToId(cell.toHilbertQuadkey())
}

async function ensureBucketExists(influx: InfluxDB, org: string, name: string): Promise<string | undefined> {
  const orgsAPI = new OrgsAPI(influx)
  const organizations = await orgsAPI.getOrgs({ org })
  if (!organizations || !organizations.orgs || !organizations.orgs.length) {
    throw new Error(`No organization named "${org}" found!`)
  }
  const orgID = organizations.orgs[0].id || 'no orgid'
  const bucketsAPI = new BucketsAPI(influx)
  try {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const buckets = await bucketsAPI.getBuckets({ orgID, name })
    if (!buckets.buckets) {
      throw new Error('Retrieving buckets failed')
    }
    return buckets.buckets[0].id
  } catch (e) {
    if (e instanceof HttpError && e.statusCode == 404) {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const bucket = await bucketsAPI.postBuckets({ body: { orgID, name, retentionRules: [] } })
      // eslint-disable-next-line no-console
      console.log(`Influxdb2: created bucket ${name}`)
    } else {
      throw e
    }
  }
}
