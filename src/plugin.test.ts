import { expect } from 'chai'
import { ZonedDateTime } from '@js-joda/core'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App } from './plugin'
import waitOn from 'wait-on'

const INFLUX_HOST = process.env['INFLUX_HOST'] || '127.0.0.1'

describe('Plugin', () => {
  it('writes something to InfluxDb', async () => {
    const bucket = `test_bucket_${Date.now()}`
    await waitOn({
      resources: [`http://${INFLUX_HOST}:8086`],
    })

    const app: App = {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
      debug: function (...args: any): void {
        throw new Error('Function not implemented.')
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
      error: function (...args: any): void {
        throw new Error('Function not implemented.')
      },
      signalk: new EventEmitter(),
    }
    const plugin = InfluxPluginFactory(app)
    await plugin.start({
      influxes: [
        {
          url: `http://${INFLUX_HOST}:8086`,
          token: 'signalk_token',
          org: 'signalk_org',
          bucket,
        },
      ],
    })
    const TESTCONTEXT = 'testContext'
    const TESTSOURCE = 'test$source'
    const TESTPATHNUMERIC = 'test.path.numeric'
    const TESTNUMERICVALUE = 3.14
    const TESTPATHBOOLEAN = 'test.path.boolean'
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
        .then(() => {
          return Promise.all(
            TESTVALUES.reduce((acc, values) => {
              acc = acc.concat(
                values.map((pathValue) =>
                  plugin
                    .getValues({
                      context: TESTCONTEXT,
                      from: ZonedDateTime.parse('2022-08-17T17:00:00Z'),
                      to: ZonedDateTime.parse('2022-08-17T17:00:00Z'),
                      paths: [pathValue.path],
                      resolution: 60,
                    })
                    .then((rows) => {
                      expect(rows.length).to.equal(pathValue.rowCount)
                    }),
                ),
              )
              return acc
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
            }, new Array<any[]>()),
          )
        })
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .then((results: any[][]) => {
          expect(results.length).be.greaterThan(0)
          return true
        })
    )
  })
})
