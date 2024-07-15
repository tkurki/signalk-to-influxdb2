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
import { EventEmitter } from 'stream'
import { getDailyLogData, registerHistoryApiRoute } from './HistoryAPI'
import { IRouter } from 'express'
import { Context, Delta, MetaDelta, Path, PathValue, SourceRef, ValuesDelta } from '@signalk/server-api'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageInfo = require('../package.json')

export interface Logging {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug: (...args: any) => void
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error: (...args: any) => void
}
export interface App extends Logging, Pick<IRouter, 'get'> {
  handleMessage(id: string, delta: Delta): void
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
  skInfluxes: () => SKInflux[]
}

export interface PluginConfig {
  /**
   * @title Output Daily Distance Log values
   * @description Calculate periodically daily distance from navigation.position values since 00:00 local time and output under navigation.trip.daily
   * @default false
   */
  outputDailyLog: boolean
  influxes: SKInfluxConfig[]
}

export default function InfluxPluginFactory(app: App): Plugin & InfluxPlugin {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const schema = require('../dist/PluginConfig.json')
  const writeOptionsProps = schema.properties.influxes.items.properties.writeOptions.properties
  delete writeOptionsProps.writeFailed
  delete writeOptionsProps.writeSuccess
  const selfContext = 'vessels.' + app.selfId

  let skInfluxes: SKInflux[] = []
  let onStop: (() => void)[] = []
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

      onStop = []
      skInfluxes.forEach((skInflux) => {
        const pruner = setInterval(() => skInflux.pruneLastWrittenTimestamps(), 5 * 60 * 1000)
        onStop.push(() => clearInterval(pruner))
      })

      if (config.outputDailyLog) {
        app.handleMessage('', {
          updates: [
            {
              meta: [
                {
                  path: 'navigation.trip.daily.log',
                  value: {
                    units: 'm'
                  }
                }
              ]
            }
          ]
        } as unknown as MetaDelta)
        let previousLength = 0
        const get = () => {
          app.debug('getDailyLogData')
          getDailyLogData(skInfluxes[0], app.selfId, app.debug).then(({ length }) => {
            app.debug(length)
            if (length !== previousLength) {
              previousLength = length
              app.handleMessage('', {
                updates: [{
                  values: [{
                    path: 'navigation.trip.daily.log' as Path,
                    value: length
                  }]
                }]
              })
            }
          })
        }
        const interval = setInterval(get, 5 * 60 * 1000)
        get()
        onStop.push(() => clearInterval(interval))
      }

      return Promise.all(skInfluxes.map((skInflux) => skInflux.init())).then(() => {
        const onDelta = (_delta: Delta) => {
          const delta = _delta as ValuesDelta
          const now = Date.now()
          const isSelf = delta.context === selfContext
          delta.updates &&
            delta.updates.forEach((update) => {
              update.values &&
                update.values.forEach((pathValue) => {
                  skInfluxes.forEach((skInflux) =>
                    skInflux.handleValue(
                      delta.context as Context,
                      isSelf,
                      update.$source as SourceRef,
                      update.timestamp,
                      pathValue as PathValue,
                      now,
                    ),
                  )
                })
            })
        }
        app.signalk.on('delta', onDelta)
        onStop.push(() => app.signalk.removeListener('delta', onDelta))
      })
    },

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    stop: () => {
      onStop.forEach((f) => f())
      onStop = []
    },

    flush: () => Promise.all(skInfluxes.map((ski) => ski.flush())),

    getValues: (params: QueryParams) => skInfluxes[0].getValues(params),
    getSelfValues: (params: Omit<QueryParams, 'context'>) => skInfluxes[0].getSelfValues(params),

    id: packageInfo.name,
    name: packageInfo.description,
    description: 'Signal K integration with InfluxDb 2',
    schema,
    skInfluxes: () => skInfluxes,
  }
}
