import { expect } from 'chai'
import { SKInflux, SKInfluxConfig } from './influx'

describe('ignoreStatusByRule', () => {
  const noopLogging = {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any
    debug: (...args: any) => {
      return
    },
    // eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any
    error: (...args: any) => {
      return
    },
  }

  const makeConfig = (filteringRules: SKInfluxConfig['filteringRules']): SKInfluxConfig => ({
    url: 'http://127.0.0.1:8086',
    token: 'test_token',
    org: 'test_org',
    bucket: 'test_bucket',
    onlySelf: true,
    filteringRules,
    ignoredPaths: [],
    ignoredSources: [],
    useSKTimestamp: false,
    resolution: 0,
    writeOptions: {},
  })

  it('should ignore navigation.speedOverGround and allow other paths', () => {
    const config = makeConfig([{ allow: false, path: 'navigation.speedOverGround', source: '' }])
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const influx = new SKInflux(config, noopLogging, () => {})

    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'some.source')).to.equal(true)
    expect(influx.ignoreStatusByRule('navigation.courseOverGroundTrue', 'some.source')).to.equal(false)
    expect(influx.ignoreStatusByRule('environment.wind.speedApparent', 'some.source')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.position', 'some.source')).to.equal(false)
  })

  it('should allow navigation.speedOverGround and all environment.wind paths and ignore all other paths with wildcard deny rule', () => {
    const config = makeConfig([
      { allow: true, path: 'navigation.speedOverGround', source: '' },
      { allow: true, path: 'environment.wind.*', source: '' },
      { allow: false, path: '.*', source: '' },
    ])
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const influx = new SKInflux(config, noopLogging, () => {})

    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'some.source')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.courseOverGroundTrue', 'some.source')).to.equal(true)
    expect(influx.ignoreStatusByRule('environment.wind.speedApparent', 'some.source')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.position', 'some.source')).to.equal(true)
  })

  it('should allow data only from sources A and B and ignore all other sources', () => {
    const config = makeConfig([
      { allow: true, path: '', source: 'A' },
      { allow: true, path: '', source: 'B' },
      { allow: false, path: '', source: '.*' },
    ])
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const influx = new SKInflux(config, noopLogging, () => {})

    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'A')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'B')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'C')).to.equal(true)
    expect(influx.ignoreStatusByRule('navigation.position', 'A')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.position', 'B')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.position', 'some.other.source')).to.equal(true)
  })

  it('should allow navigation paths only from source A and environment paths only from source B, ignoring everything else', () => {
    const config = makeConfig([
      { allow: true, path: 'navigation.*', source: 'A' },
      { allow: true, path: 'environment.*', source: 'B' },
      { allow: false, path: '.*', source: '.*' },
    ])
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const influx = new SKInflux(config, noopLogging, () => {})

    // navigation paths from source A: allowed
    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'A')).to.equal(false)
    expect(influx.ignoreStatusByRule('navigation.position', 'A')).to.equal(false)
    // navigation paths from source B: ignored
    expect(influx.ignoreStatusByRule('navigation.speedOverGround', 'B')).to.equal(true)
    expect(influx.ignoreStatusByRule('navigation.position', 'B')).to.equal(true)
    // environment paths from source B: allowed
    expect(influx.ignoreStatusByRule('environment.wind.speedApparent', 'B')).to.equal(false)
    expect(influx.ignoreStatusByRule('environment.depth.belowTransducer', 'B')).to.equal(false)
    // environment paths from source A: ignored
    expect(influx.ignoreStatusByRule('environment.wind.speedApparent', 'A')).to.equal(true)
    expect(influx.ignoreStatusByRule('environment.depth.belowTransducer', 'A')).to.equal(true)
    // unrelated path/source combinations: ignored
    expect(influx.ignoreStatusByRule('steering.rudderAngle', 'A')).to.equal(true)
    expect(influx.ignoreStatusByRule('steering.rudderAngle', 'C')).to.equal(true)
  })
})
