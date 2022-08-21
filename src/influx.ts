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

import { SKContext, SKValue } from '@chacal/signalk-ts'
import { FluxTableMetaData, InfluxDB, Point, QueryApi, WriteApi } from '@influxdata/influxdb-client'
import { QueryParams } from '.'

export interface SKInfluxConfig {
  url: string
  token: string
  org: string
  bucket: string
}

export class SKInflux {
  private writeApi: WriteApi
  private queryApi: QueryApi
  constructor(config: SKInfluxConfig) {
    const { org, bucket } = config
    const influx = new InfluxDB(config)
    this.writeApi = influx.getWriteApi(org, bucket)
    this.queryApi = influx.getQueryApi(org)
  }

  handleValue(context: SKContext, source: string, pathValue: SKValue) {
    const point = new Point(pathValue.path).tag('context', context).tag('source', source)
    switch (typeof pathValue.value) {
      case 'number':
        point.floatField('value', pathValue.value)
        break
      case 'string':
        point.stringField('value', pathValue.value)
        break
    }
    this.writeApi.writePoint(point)
  }

  flush() {
    return this.writeApi.flush()
  }

  getValues(bucket: string, params: QueryParams): Promise<Array<any>> {
    return this.queryApi.collectRows(paramsToQuery(bucket, params))
  }
}

const paramsToQuery = (bucket: string, params: QueryParams) => `
from(bucket: "${bucket}")
  |> range(start: -1d)
  |> mean()
  |> yield(name: "_results")
`
