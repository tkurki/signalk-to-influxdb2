import type { Path, SourceRef } from '@signalk/server-api'

export interface SerializedPathSpec {
  path: Path
  aggregate: string
  parameter?: string[]
  sourceRef?: SourceRef
}

export interface SerializedValuesQuery {
  from?: string
  to?: string
  duration?: number | string
  context?: string
  resolution?: number
  sourcePolicy?: 'preferred' | 'all'
  pathSpecs: SerializedPathSpec[]
}

export interface HistoryWorkerConfig {
  influx: {
    url: string
    token: string
    org: string
    bucket: string
  }
  selfId: string
  debugEnabled: boolean
}

export type HistoryWorkerRequest =
  | { id: number; method: 'getValues'; query: SerializedValuesQuery }
  | { id: number; method: 'getContexts' }
  | { id: number; method: 'getPaths' }
  | { id: number; method: 'getDailyLog' }
  | { id: number; method: 'close' }

export interface SerializedError {
  message: string
  name?: string
  code?: string
}

export type HistoryWorkerResponse =
  | { kind: 'result'; id: number; value?: unknown }
  | { kind: 'error'; id: number; error: SerializedError }
  | { kind: 'debug'; text: string }
