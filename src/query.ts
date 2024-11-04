import { ZonedDateTime } from '@js-joda/core'
import { getValues } from './HistoryAPI'
import { SKInflux } from './influx'
import { Context } from '@signalk/server-api'

/* eslint-disable  @typescript-eslint/no-explicit-any */
/* eslint-disable  @typescript-eslint/no-unused-vars */

const toConsole = (s: any) => console.log(s)
const logging = {
  debug: toConsole,
  error: toConsole,
}

const skinflux = new SKInflux(
  {
    onlySelf: true,
    url: process.env.URL || '!!!',
    token: process.env.TOKEN || '!!!',
    bucket: process.env.BUCKET || '!!!',
    org: process.env.ORG || '!!!',
    writeOptions: {},
    filteringRules: [],
    ignoredPaths: [],
    ignoredSources: [],
    useSKTimestamp: false,
    resolution: 0,
  },
  logging,
  () => undefined,
)
const context = process.env.CONTEXT || 'no-context'
const start = ZonedDateTime.parse('2023-07-24T13:03:29.048Z')
const end = ZonedDateTime.parse('2023-07-24T13:04:29.048Z')
const format = ''
const req = {
  query: {
    paths: process.env.PATHS,
    resolution: '60s',
  },
}
const resp = {
  status: (n: number) => undefined,
  json: (s: any) => console.log(JSON.stringify(s, null, 2)),
  header: (n: string, v: string) => console.log(`${n}:${v}`),
  send: (c: string) => console.log(c),
}
getValues(skinflux, context as Context, start, end, format, toConsole, req, resp)
