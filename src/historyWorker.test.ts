import { expect } from 'chai'
import type { ValuesRequest } from '@signalk/server-api/history'
import type { Path } from '@signalk/server-api'
import { WorkerHistoryProvider, serializeValuesQuery } from './historyWorkerClient'

const config = {
  url: 'http://127.0.0.1:8086',
  token: 'token',
  org: 'org',
  bucket: 'bucket',
}

describe('history worker', () => {
  it('starts and stops without touching Influx', async () => {
    const provider = new WorkerHistoryProvider(config, 'self-id', () => undefined)
    expect(await provider.start()).to.equal(true)
    await provider.close()
  })

  it('can be disabled with SIGNALK_INFLUX_DISABLE_HISTORY_WORKER', async () => {
    const previous = process.env.SIGNALK_INFLUX_DISABLE_HISTORY_WORKER
    process.env.SIGNALK_INFLUX_DISABLE_HISTORY_WORKER = '1'
    try {
      const provider = new WorkerHistoryProvider(config, 'self-id', () => undefined)
      expect(await provider.start()).to.equal(false)
      await provider.close()
    } finally {
      if (previous === undefined) {
        delete process.env.SIGNALK_INFLUX_DISABLE_HISTORY_WORKER
      } else {
        process.env.SIGNALK_INFLUX_DISABLE_HISTORY_WORKER = previous
      }
    }
  })

  it('serializes the query into plain data for the worker', () => {
    const query = {
      context: 'vessels.self',
      from: { toString: () => '2026-01-01T00:00:00Z' },
      to: { toString: () => '2026-01-01T01:00:00Z' },
      resolution: 1000,
      sourcePolicy: 'all',
      pathSpecs: [
        {
          path: 'navigation.speedOverGround' as Path,
          aggregate: 'average',
          parameter: ['5'],
          sourceRef: 'test.source',
        },
      ],
    } as unknown as ValuesRequest

    expect(serializeValuesQuery(query)).to.deep.equal({
      from: '2026-01-01T00:00:00Z',
      to: '2026-01-01T01:00:00Z',
      context: 'vessels.self',
      resolution: 1000,
      sourcePolicy: 'all',
      pathSpecs: [
        {
          path: 'navigation.speedOverGround',
          aggregate: 'average',
          parameter: ['5'],
          sourceRef: 'test.source',
        },
      ],
    })
  })

  it('serializes a duration object to milliseconds', () => {
    const query = {
      duration: { total: () => 1500 },
      pathSpecs: [],
    } as unknown as ValuesRequest
    expect(serializeValuesQuery(query).duration).to.equal(1500)
  })
})
