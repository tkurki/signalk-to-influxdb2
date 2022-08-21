import { expect } from 'chai'
import { ZonedDateTime } from '@js-joda/core'
import { EventEmitter } from 'stream'
import InfluxPluginFactory, { App } from './'
import waitOn from 'wait-on'

const INFLUX_HOST = process.env['INFLUX_HOST'] || '127.0.0.1'

describe('Plugin', () => {
  it('writes something to InfluxDb', async () => {
    await waitOn({
      resources: [`http://${INFLUX_HOST}:8086`],
    })

    const app: App = {
      debug: function (...args: any): void {
        throw new Error('Function not implemented.')
      },
      error: function (...args: any): void {
        throw new Error('Function not implemented.')
      },
      signalk: new EventEmitter(),
    }
    const plugin = InfluxPluginFactory(app)
    plugin.start({
      influxes: [
        {
          url: `http://${INFLUX_HOST}:8086`,
          token: 'signalk_token',
          org: 'signalk_org',
          bucket: 'signalk_bucket',
        },
      ],
    })
    const TESTCONTEXT = 'testContext'
    const TESTSOURCE = 'test$source'
    const TESTPATH = 'test.path'
    const TESTNUMERICVALUE = 3.14
    app.signalk.emit('delta', {
      context: TESTCONTEXT,
      updates: [
        {
          $source: TESTSOURCE,
          timestamp: new Date('2022-08-17T17:01:00Z'),
          values: [
            {
              path: TESTPATH,
              value: TESTNUMERICVALUE,
            },
          ],
        },
      ],
    })
    let x: ZonedDateTime
    return plugin
      .flush()
      .then(() => {
        return plugin.getValues('signalk_bucket', {
          context: TESTCONTEXT,
          from: ZonedDateTime.parse('2022-08-17T17:00:00Z'),
          to: ZonedDateTime.parse('2022-08-17T17:00:00Z'),
          paths: [TESTPATH],
          resolution: 60,
        })
      })
      .then((result) => {
        expect(result.length).to.equal(1)
        return true
      })
  })
})
