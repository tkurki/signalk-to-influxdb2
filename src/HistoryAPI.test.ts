import { expect } from 'chai'
import { Temporal } from '@js-temporal/polyfill'
import { Context, Path } from '@signalk/server-api'
import { InfluxHistoryProvider } from './HistoryAPI'
import { SKInflux } from './influx'

describe('InfluxDB History API', () => {
  it('queries the last string value in each bucket without averaging it', async () => {
    let query = ''
    const time = new Date('2026-09-05T15:44:00Z')
    const rows = Object.assign([{ time }], {
      groups: () => [{ name: 'navigation.state', rows: [{ last: 'motoring' }] }],
    })
    const influx = {
      v1Client: {
        query: async (sql: string) => {
          query = sql
          return rows
        },
      },
    } as unknown as SKInflux
    const provider = new InfluxHistoryProvider(influx, 'test-vessel', () => undefined)

    const result = await provider.getValues({
      context: 'vessels.test-vessel' as Context,
      from: Temporal.Instant.from('2026-09-05T15:44:00Z'),
      to: Temporal.Instant.from('2026-09-05T15:45:00Z'),
      resolution: 60,
      pathSpecs: [{ path: 'navigation.state' as Path, aggregate: 'last', parameter: [] }],
    })

    expect(query).to.include('last(value)')
    expect(query).not.to.include('mean(value)')
    expect(result.data).to.deep.equal([[time.toISOString(), 'motoring']])
  })
})
