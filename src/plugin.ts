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
import { SKDelta } from '@chacal/signalk-ts'
import { EventEmitter } from 'stream'
import { registerHistoryApiRoute } from './HistoryAPI'
import { IRouter } from 'express'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageInfo = require('../package.json')

export interface Logging {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug: (...args: any) => void
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error: (...args: any) => void
}
export interface App extends Logging, Pick<IRouter, 'get'> {
  signalk: EventEmitter
  selfId: string
  setPluginStatus: (msg: string) => void
}

export interface Plugin {
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

export interface QueryParams {
  context: string
  paths: string[]
  influxIndex: number
}

export interface InfluxPlugin {
  getValues: (params: QueryParams) => Promise<Array<unknown>>
  getSelfValues: (params: Omit<QueryParams, 'context'>) => Promise<Array<unknown>>
  flush: () => Promise<unknown>
}

export interface PluginConfig {
  influxes: SKInfluxConfig[]
}

export default function InfluxPluginFactory(app: App): Plugin & InfluxPlugin {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const schema = require('../dist/PluginConfig.json')
  const writeOptionsProps = schema.properties.influxes.items.properties.writeOptions.properties
  delete writeOptionsProps.writeFailed
  delete writeOptionsProps.writeSuccess
  const selfContext = 'vessels.' + app.selfId

  let skInfluxes: SKInflux[]
  return {
    start: function (config: PluginConfig) {
      const updatePluginStatus = () => {
        app.setPluginStatus(
          skInfluxes
            .map((skInflux) => {
              const db = `${skInflux.url}:${skInflux.org}:${skInflux.bucket}`
              return skInflux.lastWriteCallbackSucceeded ? `${db}(${skInflux.writtenLinesCount})` : `${db}:Error`
            })
            .join(';'),
        )
      }
      skInfluxes = config.influxes.map((config: SKInfluxConfig) => new SKInflux(config, app, updatePluginStatus))
      registerHistoryApiRoute(app, skInfluxes[0], app.selfId, app.debug)
      return Promise.all(skInfluxes.map((skInflux) => skInflux.init())).then(() =>
        app.signalk.on('delta', (delta: SKDelta) => {
          const isSelf = delta.context === selfContext
          delta.updates.forEach((update) => {
            update.values.forEach((pathValue) => {
              skInfluxes.forEach((skInflux) => skInflux.handleValue(delta.context, isSelf, update.$source, pathValue))
            })
          })
        }),
      )
    },

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    stop: function () {},

    flush: () => Promise.all(skInfluxes.map((ski) => ski.flush())),

    getValues: (params: QueryParams) => skInfluxes[0].getValues(params),
    getSelfValues: (params: Omit<QueryParams, 'context'>) => skInfluxes[0].getSelfValues(params),

    id: packageInfo.name,
    name: packageInfo.description,
    description: 'Signal K integration with InfluxDb 2',
    schema,
  }
}
