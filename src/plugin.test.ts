import { expect } from 'chai'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App, InfluxPlugin, Plugin } from './plugin'
import waitOn from 'wait-on'
import retry from 'async-await-retry'
import { influxPath } from './influx'
import { Context, Path, PathValue } from '@signalk/server-api'
import { getValues, InfluxHistoryProvider } from './HistoryAPI'
import { ZoneId, ZonedDateTime } from '@js-joda/core'
import { ValuesResponse } from '@signalk/server-api/history'
import { Temporal } from '@js-temporal/polyfill'

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
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    registerHistoryApiProvider: (provider) => {
      // Mock implementation for tests
    },
    unregisterHistoryApiProvider: () => {
      // Mock implementation for tests
    },
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

  describe('SMA (Simple Moving Average)', () => {
    let bucket: string
    let plugin: Plugin & InfluxPlugin

    beforeEach(async () => {
      bucket = `test_bucket_sma_${Date.now()}`
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

    it('calculates SMA with extended query window', async () => {
      // Generate test data: values 1-10 at 1-second intervals
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'environment.water.temperature' as Path

      // Write 10 data points
      for (let i = 1; i <= 10; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i,
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()

      // Wait for data to be written
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      // Request data from point 6 onwards (so we have 5 previous points for SMA-5)
      const from = Temporal.Instant.from('2024-01-01T12:00:06Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:10Z')

      // Test SMA with window size 3
      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'sma',
            parameter: ['3'],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // SMA(3) at position 6 should be average of [4, 5, 6] = 5
      // SMA(3) at position 7 should be average of [5, 6, 7] = 6
      // SMA(3) at position 8 should be average of [6, 7, 8] = 7
      // SMA(3) at position 9 should be average of [7, 8, 9] = 8
      // SMA(3) at position 10 should be average of [8, 9, 10] = 9

      const firstValue = result.data[0][1] as number
      const lastValue = result.data[result.data.length - 1][1] as number

      // First value should be around 5 (average of 4, 5, 6)
      expect(firstValue).to.be.closeTo(5, 0.5)

      // Last value should be around 9 (average of 8, 9, 10)
      expect(lastValue).to.be.closeTo(9, 0.5)
    })

    it('handles SMA with different window sizes', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'navigation.speedOverGround' as Path

      // Write 20 data points with values 1-20
      for (let i = 1; i <= 20; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i,
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      // Request data from point 16 onwards (enough history for SMA-10)
      const from = Temporal.Instant.from('2024-01-01T12:00:16Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:20Z')

      // Test SMA with window size 5
      const result5 = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'sma',
            parameter: ['5'],
          },
        ],
      })

      // SMA(5) at position 16 should be average of [12,13,14,15,16] = 14
      const firstValue5 = result5.data[0][1] as number
      expect(firstValue5).to.be.closeTo(14, 0.5)

      // Test SMA with window size 10
      const result10 = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'sma',
            parameter: ['10'],
          },
        ],
      })

      // SMA(10) at position 16 should be average of [7,8,9,10,11,12,13,14,15,16] = 11.5
      const firstValue10 = result10.data[0][1] as number
      expect(firstValue10).to.be.closeTo(11.5, 0.5)
    })

    it('handles SMA with null values in the data', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'electrical.batteries.house.voltage' as Path

      // Write data with some gaps (null values)
      const values = [1, 2, null, 4, 5, null, 7, 8, 9, 10]

      for (let i = 0; i < values.length; i++) {
        const timestamp = new Date(baseTime.getTime() + (i + 1) * 1000)
        if (values[i] !== null) {
          app.signalk.emit('delta', {
            context: TESTCONTEXT,
            updates: [
              {
                $source: TESTSOURCE,
                timestamp: timestamp.toISOString(),
                values: [
                  {
                    path: testPath,
                    value: values[i],
                  },
                ],
              },
            ],
          })
        }
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      const from = Temporal.Instant.from('2024-01-01T12:00:07Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:10Z')

      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'sma',
            parameter: ['3'],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // SMA should skip null values and calculate from available data
      result.data.forEach((row) => {
        const value = row[1]
        if (value !== null) {
          expect(value).to.be.a('number')
          expect(value).to.be.greaterThan(0)
        }
      })
    })

    it('uses default window size of 5 when parameter not specified', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'navigation.courseOverGroundTrue' as Path

      // Write 15 data points
      for (let i = 1; i <= 15; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i * 10, // values 10, 20, 30, ...
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      const from = Temporal.Instant.from('2024-01-01T12:00:10Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:15Z')

      // Test SMA without specifying window size (should default to 5)
      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'sma',
            parameter: [],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // At position 10, with default SMA(5): avg([6,7,8,9,10]*10) = avg([60,70,80,90,100]) = 80
      const firstValue = result.data[0][1] as number
      expect(firstValue).to.be.closeTo(80, 5)
    })
  })

  describe('EMA (Exponential Moving Average)', () => {
    let bucket: string
    let plugin: Plugin & InfluxPlugin

    beforeEach(async () => {
      bucket = `test_bucket_ema_${Date.now()}`
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

    it('calculates EMA with initial SMA seed', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'environment.wind.speedApparent' as Path

      // Write 30 data points with values 1-30
      for (let i = 1; i <= 30; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i,
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      // Request data from point 20 onwards with EMA(5)
      const from = Temporal.Instant.from('2024-01-01T12:00:20Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:30Z')

      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'ema',
            parameter: ['5'],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // EMA should be calculated after initial SMA period (15 points for 3x5)
      // Values should be smoothly increasing
      const firstValue = result.data[0][1] as number
      const lastValue = result.data[result.data.length - 1][1] as number

      expect(firstValue).to.be.greaterThan(15) // Should be more than halfway through initial values
      expect(lastValue).to.be.greaterThan(firstValue) // Should be increasing
      expect(lastValue).to.be.lessThan(30) // EMA lags behind actual values
    })

    it('handles different EMA periods', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'environment.wind.angleApparent' as Path

      // Write 40 data points
      for (let i = 1; i <= 40; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i * 2, // values 2, 4, 6, ...
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      const from = Temporal.Instant.from('2024-01-01T12:00:31Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:40Z')

      // Test EMA with period 3
      const result3 = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'ema',
            parameter: ['3'],
          },
        ],
      })

      // Test EMA with period 10
      const result10 = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'ema',
            parameter: ['10'],
          },
        ],
      })

      expect(result3.data.length).to.be.greaterThan(0)
      expect(result10.data.length).to.be.greaterThan(0)

      // Shorter period EMA should respond faster (be closer to current values)
      const last3 = result3.data[result3.data.length - 1][1] as number
      const last10 = result10.data[result10.data.length - 1][1] as number

      // EMA(3) should be closer to 80 (value at t=40) than EMA(10)
      expect(Math.abs(last3 - 80)).to.be.lessThan(Math.abs(last10 - 80))
    })

    it('handles null values in EMA calculation', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'navigation.speedThroughWater' as Path

      // Write data with gaps
      const values = Array.from({ length: 25 }, (_, i) => (i % 4 === 2 ? null : i + 1))

      for (let i = 0; i < values.length; i++) {
        const timestamp = new Date(baseTime.getTime() + (i + 1) * 1000)
        if (values[i] !== null) {
          app.signalk.emit('delta', {
            context: TESTCONTEXT,
            updates: [
              {
                $source: TESTSOURCE,
                timestamp: timestamp.toISOString(),
                values: [
                  {
                    path: testPath,
                    value: values[i],
                  },
                ],
              },
            ],
          })
        }
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      const from = Temporal.Instant.from('2024-01-01T12:00:16Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:25Z')

      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'ema',
            parameter: ['5'],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // EMA should carry forward when encountering nulls
      result.data.forEach((row) => {
        const value = row[1]
        if (value !== null) {
          expect(value).to.be.a('number')
          expect(value).to.be.greaterThan(0)
        }
      })
    })

    it('uses default period of 5 for EMA when not specified', async () => {
      const baseTime = new Date('2024-01-01T12:00:00Z')
      const testPath = 'environment.water.temperature' as Path

      // Write 25 data points
      for (let i = 1; i <= 25; i++) {
        const timestamp = new Date(baseTime.getTime() + i * 1000)
        app.signalk.emit('delta', {
          context: TESTCONTEXT,
          updates: [
            {
              $source: TESTSOURCE,
              timestamp: timestamp.toISOString(),
              values: [
                {
                  path: testPath,
                  value: i * 5, // values 5, 10, 15, ...
                },
              ],
            },
          ],
        })
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      await plugin.flush()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      const influx = plugin.skInfluxes()[0]
      const historyProvider = new InfluxHistoryProvider(influx, selfId, app.debug)

      const from = Temporal.Instant.from('2024-01-01T12:00:20Z')
      const to = Temporal.Instant.from('2024-01-01T12:00:25Z')

      // Test EMA without specifying period (should default to 5)
      const result = await historyProvider.getValues({
        from,
        to,
        context: TESTCONTEXT as Context,
        resolution: 1,
        pathSpecs: [
          {
            path: testPath,
            aggregate: 'ema',
            parameter: [],
          },
        ],
      })

      expect(result.data.length).to.be.greaterThan(0)

      // Should have valid EMA values
      const firstValue = result.data[0][1] as number
      expect(firstValue).to.be.greaterThan(0)
      expect(firstValue).to.be.lessThan(125) // Should be less than max value
    })
  })
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
