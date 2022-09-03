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
import { HttpError, InfluxDB, Point, QueryApi, WriteApi } from '@influxdata/influxdb-client'
import { BucketsAPI, OrgsAPI } from '@influxdata/influxdb-client-apis'

import { QueryParams } from './plugin'
import { S2 } from 's2-geometry'

export interface SKInfluxConfig {
  /**
   * Url of the InfluxDb 2 server
   * 
   * @title Foo
   */
  url: string
  /**
   * Token
   * 
   */
  token: string
  /**
   * Organisation
   */
  org: string
  /**
   * Bucket
   */
  bucket: string
}

interface PathValue {
  path: string
  value: any
}

export class SKInflux {
  private influx: InfluxDB
  private org: string
  private bucket: string
  private writeApi: WriteApi
  private queryApi: QueryApi
  constructor(config: SKInfluxConfig) {
    const { org, bucket } = config
    this.influx = new InfluxDB(config)
    this.org = org
    this.bucket = bucket
    this.writeApi = this.influx.getWriteApi(org, bucket, 'ms')
    this.queryApi = this.influx.getQueryApi(org)
  }

  init() {
    return ensureBucketExists(this.influx, this.org, this.bucket)
  }

  handleValue(context: SKContext, source: string, pathValue: PathValue) {
    const point = new Point(pathValue.path).tag('context', context).tag('source', source)
    if (pathValue.path === 'navigation.position') {
      point.floatField('lat', pathValue.value.latitude)
      point.floatField('lon', pathValue.value.longitude)
      point.tag('s2_cell_id', posToS2CellId(pathValue.value))
    } else {
      switch (typeof pathValue.value) {
        case 'number':
          point.floatField('value', pathValue.value)
          break
        case 'string':
          point.stringField('value', pathValue.value)
          break
        case 'boolean':
          point.booleanField('value', pathValue.value)
      }
    }
    this.writeApi.writePoint(point)
  }

  flush() {
    return this.writeApi.flush()
  }

  getValues(params: QueryParams): Promise<Array<any>> {
    return this.queryApi.collectRows(paramsToQuery(this.bucket, params))
  }
}

const paramsToQuery = (bucket: string, params: QueryParams) => {
  let query = `
  from(bucket: "${bucket}")
    |> range(start: -1y)
    |> filter(fn: (r) => r["_measurement"] == "${params.paths[0]}")
  `
  if (params.bbox && params.paths[0] === 'navigation.position') {
    query = `import "experimental/geo"
    ` + query  + `
     |> geo.filterRows(region: {minLat: ${params.bbox.sw.latitude},maxLat: ${params.bbox.ne.latitude},minLon: ${params.bbox.sw.longitude},maxLon: ${params.bbox.ne.longitude}})
    `
  }
  return query
}


const posToS2CellId = (position: { latitude: number; longitude: number }) => {
  const cell = S2.S2Cell.FromLatLng({ lat: position.latitude, lng: position.longitude }, 10)
  const rawId = S2.keyToId(cell.toHilbertQuadkey())
  return Number(rawId).toString(16).replace(/0*$/,'')
}

async function ensureBucketExists(influx: InfluxDB, org: string, name: string) {
  const orgsAPI = new OrgsAPI(influx)
  const organizations = await orgsAPI.getOrgs({ org })
  if (!organizations || !organizations.orgs || !organizations.orgs.length) {
    throw new Error(`No organization named "${org}" found!`)
  }
  const orgID = organizations.orgs[0].id || 'no orgid'
  const bucketsAPI = new BucketsAPI(influx)
  try {
    const buckets = await bucketsAPI.getBuckets({ orgID, name })
  } catch (e) {
    if (e instanceof HttpError && e.statusCode == 404) {
      const bucket = await bucketsAPI.postBuckets({ body: { orgID, name, retentionRules: [] } })
      console.log(`Influxdb2: created bucket ${name}`)
        } else {
      throw e
    }
  }
}
