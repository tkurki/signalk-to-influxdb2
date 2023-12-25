import { expect } from 'chai'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App, InfluxPlugin, Plugin } from './plugin'
import waitOn from 'wait-on'
import retry from 'async-await-retry'
import { influxPath } from './influx'

const INFLUX_HOST = process.env['INFLUX_HOST'] || '127.0.0.1'

const TESTSOURCE = 'test$source'
const TESTPATHNUMERIC = 'test.path.numeric'
const TESTNUMERICVALUE = 3.14
const TESTPATHBOOLEAN = 'test.path.boolean'

describe('Plugin', () => {
  const selfId = 'urn:mrn:signalk:uuid:c0d79334-4e25-4245-8892-54e8ccc85elf'
  const TESTCONTEXT = `vessels.${selfId}`
  const MMSICONTEXT = 'vessels.urn:mrn:imo:mmsi:200000000'
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
          ignoredPaths: [],
          ignoredSources: [],
          useSKTimestamp: false,
          resolution: 0,
        },
      ],
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
          ignoredPaths: [],
          ignoredSources: [],
          useSKTimestamp: true, // <===============
          resolution: 0,
        },
      ],
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
