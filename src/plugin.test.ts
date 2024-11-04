import { expect } from 'chai'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App, InfluxPlugin, Plugin } from './plugin'
import waitOn from 'wait-on'
import retry from 'async-await-retry'
import { influxPath } from './influx'
import { Context, Path, PathValue } from '@signalk/server-api'
import { ValuesResponse, getValues } from './HistoryAPI'
import { ZoneId, ZonedDateTime } from '@js-joda/core'

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
      retry(() =>
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
      ),
    )
  })

  it('Uses delta timestamp by configuration', async () => {
    plugin.stop()
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
          useSKTimestamp: true, // <===============
          resolution: 0,
        },
      ],
      outputDailyLog: false,
    })
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
