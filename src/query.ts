import { ZonedDateTime } from '@js-joda/core'
import { getValues } from './HistoryAPI'
import { SKInflux } from './influx'

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
    ignoredPaths: [],
    ignoredSources: [],
  },
  logging,
  () => undefined,
)
const context = process.env.CONTEXT || 'no-context'
const start = ZonedDateTime.parse('2023-07-24T13:03:29.048Z')
const end = ZonedDateTime.parse('2023-07-24T13:04:29.048Z')
const req = {
  query: {
    paths: process.env.PATHS,
    resolution: '60s',
  },
}
const resp = {
  status: (n: number) => undefined,
  json: (s: any) => console.log(JSON.stringify(s, null, 2)),
}
getValues(skinflux, context, start, end, toConsole, req, resp)
