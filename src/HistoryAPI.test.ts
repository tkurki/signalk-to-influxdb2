import { expect } from 'chai'
import { Temporal } from '@js-temporal/polyfill'
import { Context, Path } from '@signalk/server-api'
import { InfluxHistoryProvider } from './HistoryAPI'
import { SKInflux } from './influx'

describe('InfluxDB History API', () => {
  it('aligns sparse measurements by timestamp, not by row index', async () => {
    const times = [0, 1, 2].map((minute) => new Date(Date.UTC(2026, 8, 5, 15, 44 + minute)))
    const influx = {
      v1Client: {
        query: async () =>
          Object.assign([{ time: times[0] }, { time: times[2] }, { time: times[1] }], {
            groups: () => [
              {
                name: 'navigation.speedOverGround',
                rows: [
                  { time: times[0], max: 3 },
                  { time: times[2], max: 5 },
                ],
              },
              { name: 'propulsion.port.revolutions', rows: [{ time: times[1], max: 1000 }] },
            ],
          }),
      },
    } as unknown as SKInflux
    const provider = new InfluxHistoryProvider(influx, 'test-vessel', () => undefined)

    const result = await provider.getValues({
      context: 'vessels.test-vessel' as Context,
      from: Temporal.Instant.from('2026-09-05T15:44:00Z'),
      to: Temporal.Instant.from('2026-09-05T15:47:00Z'),
      resolution: 60,
      pathSpecs: [
        { path: 'navigation.speedOverGround' as Path, aggregate: 'max', parameter: [] },
        { path: 'propulsion.port.revolutions' as Path, aggregate: 'max', parameter: [] },
      ],
    })

    expect(result.data).to.deep.equal([
      [times[0].toISOString(), 3, null],
      [times[1].toISOString(), null, 1000],
      [times[2].toISOString(), 5, null],
    ])
  })

  it('queries mixed aggregates separately and joins them with positions by timestamp', async () => {
    const times = [0, 1, 2].map((minute) => new Date(Date.UTC(2026, 8, 5, 15, 44 + minute)))
    const queries: string[] = []
    const influx = {
      v1Client: {
        query: async (sql: string) => {
          queries.push(sql)
          if (sql.includes('"navigation.position"')) {
            return [
              { time: times[0], lat: 60, lon: 24 },
              { time: times[1], lat: 61, lon: 25 },
              { time: times[2], lat: null, lon: null },
            ]
          }
          if (sql.includes('"navigation.speedOverGround"')) {
            return Object.assign([{ time: times[0] }, { time: times[2] }], {
              groups: () => [
                {
                  name: 'navigation.speedOverGround',
                  rows: [
                    { time: times[0], max: 3 },
                    { time: times[2], max: 5 },
                  ],
                },
              ],
            })
          }
          return Object.assign([{ time: times[1] }, { time: times[2] }], {
            groups: () => [
              {
                name: 'navigation.speedThroughWater',
                rows: [
                  { time: times[1], min: 2 },
                  { time: times[2], min: 4 },
                ],
              },
            ],
          })
        },
      },
    } as unknown as SKInflux
    const provider = new InfluxHistoryProvider(influx, 'test-vessel', () => undefined)

    const result = await provider.getValues({
      context: 'vessels.test-vessel' as Context,
      from: Temporal.Instant.from('2026-09-05T15:44:00Z'),
      to: Temporal.Instant.from('2026-09-05T15:47:00Z'),
      resolution: 60,
      pathSpecs: [
        { path: 'navigation.position' as Path, aggregate: 'first', parameter: [] },
        { path: 'navigation.speedOverGround' as Path, aggregate: 'max', parameter: [] },
        { path: 'navigation.speedThroughWater' as Path, aggregate: 'min', parameter: [] },
      ],
    })

    expect(queries).to.have.length(3)
    expect(queries.find((sql) => sql.includes('"navigation.speedThroughWater"'))).to.include('min(value)')
    expect(queries.find((sql) => sql.includes('"navigation.speedThroughWater"'))).not.to.include('max(value)')
    expect(result.data).to.deep.equal([
      [times[0].toISOString(), [24, 60], 3, null],
      [times[1].toISOString(), [25, 61], null, 2],
      [times[2].toISOString(), null, 5, 4],
    ])
  })

  it('keeps separate aggregates of the same path in the requested columns', async () => {
    const time = new Date('2026-09-05T15:44:00Z')
    const queries: string[] = []
    const influx = {
      v1Client: {
        query: async (sql: string) => {
          queries.push(sql)
          const aggregate = sql.includes('min(value)') ? 'min' : 'max'
          return Object.assign([{ time }], {
            groups: () => [
              { name: 'navigation.speedOverGround', rows: [{ time, [aggregate]: aggregate === 'min' ? 2 : 5 }] },
            ],
          })
        },
      },
    } as unknown as SKInflux
    const provider = new InfluxHistoryProvider(influx, 'test-vessel', () => undefined)

    const result = await provider.getValues({
      context: 'vessels.test-vessel' as Context,
      from: Temporal.Instant.from('2026-09-05T15:44:00Z'),
      to: Temporal.Instant.from('2026-09-05T15:45:00Z'),
      resolution: 60,
      pathSpecs: [
        { path: 'navigation.speedOverGround' as Path, aggregate: 'min', parameter: [] },
        { path: 'navigation.speedOverGround' as Path, aggregate: 'max', parameter: [] },
      ],
    })

    expect(queries).to.have.length(2)
    expect(result.data).to.deep.equal([[time.toISOString(), 2, 5]])
  })
})
