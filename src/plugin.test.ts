import { expect } from 'chai'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App, InfluxPlugin, Plugin } from './plugin'
import waitOn from 'wait-on'
import retry from 'async-await-retry'
import { influxPath } from './influx'
import { Context, Path, PathValue } from '@signalk/server-api'
import { getValues } from './HistoryAPI'
import { ZoneId, ZonedDateTime } from '@js-joda/core'
import { ValuesResponse } from '@signalk/server-api/history'

const INFLUX_HOST = process.env['INFLUX_HOST'] || '127.0.0.1'

const TESTSOURCE = 'test$source'
const TESTPATHNUMERIC = 'test.path.numeric'
const TESTNUMERICVALUE = 3.14
const TESTPATHBOOLEAN = 'test.path.boolean'
const selfId = 'urn:mrn:signalk:uuid:c0d79334-4e25-4245-8892-54e8ccc85elf'
const TESTCONTEXT = `vessels.${selfId}`
const MMSICONTEXT = 'vessels.urn:mrn:imo:mmsi:200000000'

describe('Plugin', () => {
  const app: App = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
    debug: function (...args: any): void {
      // eslint-disable-next-line no-console
      console.log(`debug:${args}`)
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
    error: function (...args: any): void {
      // eslint-disable-next-line no-console
      console.error(`debug:${args}`)
    },
    signalk: new EventEmitter(),
    selfId,
    setPluginStatus: (s: string) => console.log(s),
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    get: () => undefined,
  }

  before(async () => {
    await waitOn({
      resources: [`http://${INFLUX_HOST}:8086`],
    })
  })

  let bucket: string
  let plugin: Plugin & InfluxPlugin

  describe('Using current timestamp when writing', () => {
    beforeEach(async () => {
      bucket = `test_bucket_${Date.now()}`
      plugin = InfluxPluginFactory(app)
      await plugin.start({
        influxes: [
          {
            url: `http://${INFLUX_HOST}:8086`,
            token: 'signalk_token',
            org: 'signalk_org',
            bucket,
            onlySelf: false,
            writeOptions: {
              batchSize: 1,
              flushInterval: 10,
              maxRetries: 1,
            },
            filteringRules: [],
            ignoredPaths: [],
            ignoredSources: [],
            useSKTimestamp: false,
            resolution: 0,
          },
        ],
        outputDailyLog: false,
      })
    })

    afterEach(() => {
      plugin.stop()
    })

    it('writes something to InfluxDb', async () => {
      const TESTVALUES = [
        [
          {
            path: TESTPATHNUMERIC,
            value: TESTNUMERICVALUE,
            rowCount: 1,
          },
          {
            path: TESTPATHBOOLEAN,
            value: false,
            rowCount: 1,
          },
        ],
        [
          {
            path: 'navigation.position',
            value: {
              latitude: 60.1513403,
              longitude: 24.8916156,
            },
            rowCount: 2,
          },
        ],
        [
          {
            path: '',
            value: {
              mmsi: '20123456',
            },
            rowCount: 1,
          },
        ],
      ]
      TESTVALUES.forEach((values) =>
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: new Date('2022-08-17T17:01:00Z'),
              values,
            },
          ],
        }),
      )
      return (
        plugin
          .flush()
          .then(async () => {
            const testAllValuesFoundInDb = async () =>
              Promise.all(
                TESTVALUES.reduce((acc, values) => {
                  acc = acc.concat(
                    values.map((pathValue) =>
                      plugin
                        .getValues({
                          context: TESTCONTEXT,
                          paths: [influxPath(pathValue.path)],
                          influxIndex: 0,
                        })
                        .then((rows) => {
                          expect(rows.length).to.equal(pathValue.rowCount, `${JSON.stringify(pathValue)}`)
                        }),
                    ),
                  )
                  acc.push([
                    plugin
                      .getSelfValues({
                        paths: [influxPath(values[0].path)],
                        influxIndex: 0,
                      })
                      .then((rows) => {
                        expect(rows.length).to.equal(values[0].rowCount, `${JSON.stringify(values[0])}`)
                      }),
                  ])
                  return acc
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                }, new Array<any[]>()),
              )
            return await retry(testAllValuesFoundInDb, [null], {
              retriesMax: 5,
              interval: 50,
            })
          })
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          .then((results: any[][]) => {
            expect(results.length).be.greaterThan(0)
            return true
          })
      )
    })

    it('Tags self data so getSelfData works', async () => {
      app.signalk.emit('delta', {
        context: TESTCONTEXT,
        updates: [
          {
            $source: TESTSOURCE,
            timestamp: new Date('2022-08-17T17:01:00Z'),
            values: [
              {
                path: TESTPATHNUMERIC,
                value: 100,
              },
            ],
          },
        ],
      })
      app.signalk.emit('delta', {
        context: MMSICONTEXT,
        updates: [
          {
            $source: TESTSOURCE,
            timestamp: new Date('2022-08-17T17:01:00Z'),
            values: [
              {
                path: TESTPATHNUMERIC,
                value: 200,
              },
            ],
          },
        ],
      })

      const assertData = () =>
        plugin.flush().then(() =>
          plugin
            .getSelfValues({
              paths: [TESTPATHNUMERIC],
              influxIndex: 0,
            })
            .then((rows) => expect(rows.length).to.equal(1)),
        )
      return retry(assertData, [null], {
        retriesMax: 5,
        interval: 50,
      })
    })

    it('Uses data types from schema, initial null value', async () => assertNumericAfterFirstOtherValue(null))
    it('Uses data types from schema, , initial string value', async () =>
      assertNumericAfterFirstOtherValue('first string value'))

    it('Forces object as value type for notifications', async () => {
      const notificationPaths = [
        'notifications.environment.inside.humidity' as Path,
        'notifications.environment.inside.temperature' as Path,
      ]
      notificationPaths.map(toNotificationDelta).forEach((d) => app.signalk.emit('delta', d))
      return plugin.flush().then(() =>
        retry(
          () =>
            Promise.all(
              notificationPaths.map((path) =>
                plugin
                  .getSelfValues({
                    paths: [path],
                    influxIndex: 0,
                  })
                  .then((rows) => expect(rows.length).to.equal(1)),
              ),
            ),
          [null],
          { retriesMax: 5, interval: 50 },
        ),
      )
    })

    it('Can retrieve positions with other data', async () => {
      const from = ZonedDateTime.now(ZoneId.UTC)
      const TESTPOSITION = { latitude: 60.1703524, longitude: 24.9589753 }
      ;[
        {
          path: 'navigation.speedThroughWater' as Path,
          value: 1001,
        },
        {
          path: 'navigation.position' as Path,
          value: TESTPOSITION,
        },
        {
          path: 'navigation.speedOverGround' as Path,
          value: 3.14,
        },
      ]
        .map(toDelta)
        .forEach((d) => app.signalk.emit('delta', d))

      return plugin.flush().then(() =>
        retry(
          () =>
            new Promise<ValuesResponse>((resolve) => {
              const to = ZonedDateTime.now(ZoneId.UTC)
              getValues(
                plugin.skInfluxes()[0],
                TESTCONTEXT as Context,
                from,
                to,
                '',
                (s) => console.log(s),
                {
                  query: {
                    paths: 'navigation.speedThroughWater,navigation.position,navigation.speedOverGround',
                    resolution: '10',
                  },
                },
                {
                  send: () => {
                    throw new Error('send called')
                  },
                  header: () => {
                    throw new Error('header called')
                  },
                  status: (s) => console.log(s),
                  json: (r) => resolve(r),
                },
              )
            }).then((result) => {
              expect(result.values.length).to.equal(3)
              expect(result.data.length).to.equal(1)
              expect(result.data[0][1]).to.deep.equal([TESTPOSITION.longitude, TESTPOSITION.latitude])
              expect(result.data[0][2]).to.deep.equal(1001)
              expect(result.data[0][3]).to.deep.equal(3.14)
            }),
          [null],
          {
            retriesMax: 5,
            interval: 50,
          },
        ),
      )
    })
  })

  describe('Using delta timestamp when writing', () => {
    beforeEach(async () => {
      bucket = `test_bucket_${Date.now()}`
      plugin = InfluxPluginFactory(app)
      await plugin.start({
        influxes: [
          {
            url: `http://${INFLUX_HOST}:8086`,
            token: 'signalk_token',
            org: 'signalk_org',
            bucket,
            onlySelf: false,
            writeOptions: {
              batchSize: 1,
              flushInterval: 10,
              maxRetries: 1,
            },
            filteringRules: [],
            ignoredPaths: [],
            ignoredSources: [],
            useSKTimestamp: true,
            resolution: 0,
          },
        ],
        outputDailyLog: false,
      })
    })

    afterEach(() => {
      plugin.stop()
    })

    it('Uses delta timestamp by configuration', async () => {
      const FIXED_TIMESTAMP = '2023-08-17T17:01:00Z'
      app.signalk.emit('delta', {
        context: TESTCONTEXT,
        updates: [
          {
            $source: TESTSOURCE,
            timestamp: new Date(FIXED_TIMESTAMP),
            values: [
              {
                path: TESTPATHNUMERIC,
                value: 222,
              },
            ],
          },
        ],
      })

      return retry(
        () =>
          plugin.flush().then(() =>
            plugin
              .getSelfValues({
                paths: [TESTPATHNUMERIC],
                influxIndex: 0,
              })
              .then((rows) => {
                expect(rows.length).to.equal(1)
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                expect((rows as any)[0]._time).to.equal(FIXED_TIMESTAMP)
              }),
          ),
        [null],
        {
          retriesMax: 5,
          interval: 50,
        },
      )
    })
  })

  describe('useSKTimestamp enabled (no write throttling)', () => {
    beforeEach(async () => {
      bucket = `test_bucket_${Date.now()}`
      plugin = InfluxPluginFactory(app)
      await plugin.start({
        influxes: [
          {
            url: `http://${INFLUX_HOST}:8086`,
            token: 'signalk_token',
            org: 'signalk_org',
            bucket,
            onlySelf: false,
            writeOptions: {
              batchSize: 1,
              flushInterval: 10,
              maxRetries: 1,
            },
            filteringRules: [],
            ignoredPaths: [],
            ignoredSources: [],
            useSKTimestamp: true,
            // Disable per-path write throttling in this test so rapid emissions
            // don't collapse into a handful of points based on wall-clock time.
            resolution: -1,
          },
        ],
        outputDailyLog: false,
      })
    })

    afterEach(() => {
      plugin.stop()
    })

    it('Collates positions and numeric values over time', async () => {
      // Use SK timestamps so we can control the time buckets precisely.

      const base = new Date('2023-01-01T00:00:00.000Z')
      const stepMs = 10_000
      const count = 30

      for (let i = 0; i < count; i++) {
        const ts = new Date(base.getTime() + i * stepMs)
        const position = { latitude: 60 + i * 0.001, longitude: 24 + i * 0.001 }
        const stw = 1000 + i
        const sog = 3.0 + i * 0.1

        // Create some buckets with only numeric or only position to exercise timestamp union collation.
        const positionOnly = i % 7 === 0 && i % 5 !== 0
        const numericOnly = i % 5 === 0 && i % 7 !== 0

        const values: PathValue[] = []
        if (!numericOnly) {
          values.push({ path: 'navigation.position' as Path, value: position })
        }
        if (!positionOnly) {
          values.push({ path: 'navigation.speedThroughWater' as Path, value: stw })
          values.push({ path: 'navigation.speedOverGround' as Path, value: sog })
        }

        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: ts,
              values,
            },
          ],
        })
      }

      await plugin.flush()

      const from = ZonedDateTime.parse('2023-01-01T00:00:00Z')
      const to = ZonedDateTime.parse('2023-01-01T00:05:00Z')

      return retry(
        () =>
          new Promise<ValuesResponse>((resolve) => {
            getValues(
              plugin.skInfluxes()[0],
              TESTCONTEXT as Context,
              from,
              to,
              '',
              () => undefined,
              {
                query: {
                  paths: 'navigation.speedThroughWater,navigation.position,navigation.speedOverGround',
                  resolution: '10',
                },
              },
              {
                send: () => {
                  throw new Error('send called')
                },
                header: () => {
                  throw new Error('header called')
                },
                status: () => undefined,
                json: (r) => resolve(r),
              },
            )
          }).then((result) => {
            expect(result.values.map((v) => v.path)).to.deep.equal([
              'navigation.position',
              'navigation.speedThroughWater',
              'navigation.speedOverGround',
            ])
            expect(result.data.length).to.equal(count)

            const assertRow = (i: number) => {
              const expectedTs = new Date(base.getTime() + i * stepMs).toISOString()
              const positionOnly = i % 7 === 0 && i % 5 !== 0
              const numericOnly = i % 5 === 0 && i % 7 !== 0
              const position = { latitude: 60 + i * 0.001, longitude: 24 + i * 0.001 }
              const stw = 1000 + i
              const sog = 3.0 + i * 0.1

              expect(result.data[i][0]).to.equal(expectedTs)
              expect(result.data[i][1]).to.deep.equal(numericOnly ? null : [position.longitude, position.latitude])
              expect(result.data[i][2]).to.deep.equal(positionOnly ? null : stw)
              expect(result.data[i][3]).to.deep.equal(positionOnly ? null : sog)
            }

            // Spot-check a few buckets.
            assertRow(0) // both
            assertRow(5) // numericOnly
            assertRow(7) // positionOnly
            assertRow(count - 1)
          }),
        [null],
        {
          retriesMax: 5,
          interval: 50,
        },
      )
    })
  })

  const assertNumericAfterFirstOtherValue = (firstValue: string | null) => {
    const NUMERICSCHEMAPATH = 'navigation.headingTrue'
    ;[firstValue, 3.14, null, 'last string value'].forEach((value) =>
      app.signalk.emit('delta', {
        context: TESTCONTEXT,
        updates: [
          {
            $source: TESTSOURCE,
            timestamp: new Date('2022-08-17T17:01:00Z'),
            values: [
              {
                path: NUMERICSCHEMAPATH,
                value,
              },
            ],
          },
        ],
      }),
    )

    const assertData = () =>
      plugin.flush().then(() =>
        plugin
          .getSelfValues({
            paths: [NUMERICSCHEMAPATH],
            influxIndex: 0,
          })
          .then((rows) => expect(rows.length).to.equal(1)),
      )
    return retry(assertData, [null], {
      retriesMax: 5,
      interval: 50,
    })
  }
})

const toNotificationDelta = (path: Path) => ({
  context: TESTCONTEXT,
  updates: [
    {
      $source: TESTSOURCE,
      values: [
        {
          path,
          value: {
            state: 'normal',
            message: 'vlag',
            method: ['visual', 'sound'],
            timestamp: '2023-12-28T10:36:21.348Z',
          },
        },
      ],
    },
  ],
})

const toDelta = (pathValue: PathValue) => ({
  context: TESTCONTEXT,
  updates: [
    {
      $source: TESTSOURCE,
      values: [pathValue],
    },
  ],
})
