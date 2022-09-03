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

import { SKInflux, SKInfluxConfig } from './influx'
import { SKDelta, SKPosition } from '@chacal/signalk-ts'
import { EventEmitter } from 'stream'
import { ZonedDateTime } from '@js-joda/core'

const packageInfo = require('../package.json')

export interface App {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug: (...args: any) => void
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error: (...args: any) => void
  signalk: EventEmitter
}

interface Plugin {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  start: (c: PluginConfig) => Promise<unknown>
  stop: () => void
  // signalKApiRoutes: (r: Router) => Router
  id: string
  name: string
  description: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  schema: any
}


export class BBox {
  readonly ne: SKPosition
  readonly sw: SKPosition

  constructor(corners: { ne: SKPosition; sw: SKPosition }) {
    this.ne = corners.ne
    this.sw = corners.sw
  }
}

export interface QueryParams {
  context: string
  from: ZonedDateTime
  to: ZonedDateTime
  paths: string[]
  resolution: number
  bbox?: BBox
}

interface InfluxPlugin {
  getValues: (params: QueryParams) => Promise<Array<any>>
  flush: () => Promise<unknown>
}

export interface PluginConfig {
  influxes: SKInfluxConfig[]
}

export default function InfluxPluginFactory(app: App): Plugin & InfluxPlugin {
  let skInfluxes: SKInflux[]
  return {
    start: function (config: PluginConfig) {
      skInfluxes = config.influxes.map((config: SKInfluxConfig) => new SKInflux(config))
      return Promise.all(skInfluxes.map((skInflux) => skInflux.init())).then(() =>
        app.signalk.on('delta', (delta: SKDelta) => {
          delta.updates.forEach((update) => {
            update.values.forEach((pathValue) => {
              skInfluxes.forEach((skInflux) => skInflux.handleValue(delta.context, update.$source, pathValue))
            })
          })
        }),
      )
    },

    stop: function () {},

    flush: () => Promise.all(skInfluxes.map((ski) => ski.flush())),

    getValues: (params: QueryParams) => skInfluxes[0].getValues(params),

    id: packageInfo.name,
    name: packageInfo.description,
    description: 'Signal K integration with InfluxDb 2',
    schema: require('../dist/PluginConfig.json')
  }
}
