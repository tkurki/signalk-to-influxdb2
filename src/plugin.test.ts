import { expect } from 'chai'
import { ZonedDateTime } from '@js-joda/core'
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
  const selfId = 'testContext'
  const TESTCONTEXT = `vessels.${selfId}`
  const app: App = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
    debug: function (...args: any): void {
      // eslint-disable-next-line no-console
      console.log(args)
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
    error: function (...args: any): void {
      // eslint-disable-next-line no-console
      console.log(args)
    },
    signalk: new EventEmitter(),
    selfId,
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
          writeOptions: {
            batchSize: 1,
            flushInterval: 10,
            maxRetries: 1,
          },
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
})
