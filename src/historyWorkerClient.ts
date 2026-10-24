import fs from 'node:fs'
import path from 'node:path'
import { Worker } from 'node:worker_threads'
import type { SourceRef } from '@signalk/server-api'
import type {
  ContextsResponse,
  HistoryApi,
  PathsResponse,
  ValuesRequest,
  ValuesResponse,
} from '@signalk/server-api/history'
import type {
  HistoryWorkerConfig,
  HistoryWorkerRequest,
  HistoryWorkerResponse,
  SerializedError,
  SerializedValuesQuery,
} from './historyWorkerProtocol'

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never
type RequestInput = DistributiveOmit<HistoryWorkerRequest, 'id'>

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
}

const toError = (error: SerializedError): Error => {
  const result = new Error(error.message)
  result.name = error.name ?? 'Error'
  if (error.code) {
    ;(result as NodeJS.ErrnoException).code = error.code
  }
  return result
}

export const serializeValuesQuery = (query: ValuesRequest): SerializedValuesQuery => {
  const value = query as unknown as {
    from?: unknown
    to?: unknown
    duration?: unknown
    context?: string
    resolution?: number
    sourcePolicy?: 'preferred' | 'all'
  }
  const duration = value.duration
  const serializedDuration =
    typeof duration === 'number' || typeof duration === 'string'
      ? duration
      : duration && typeof (duration as { total?: unknown }).total === 'function'
      ? (duration as { total: (unit: string) => number }).total('milliseconds')
      : undefined

  return {
    ...(value.from !== undefined ? { from: String(value.from) } : {}),
    ...(value.to !== undefined ? { to: String(value.to) } : {}),
    ...(serializedDuration !== undefined ? { duration: serializedDuration } : {}),
    ...(value.context !== undefined ? { context: value.context } : {}),
    ...(value.resolution !== undefined ? { resolution: value.resolution } : {}),
    ...(value.sourcePolicy !== undefined ? { sourcePolicy: value.sourcePolicy } : {}),
    pathSpecs: query.pathSpecs.map((spec) => {
      const sourceRef = (spec as { sourceRef?: SourceRef }).sourceRef
      return {
        path: spec.path,
        aggregate: spec.aggregate,
        ...(spec.parameter?.length ? { parameter: spec.parameter } : {}),
        ...(sourceRef ? { sourceRef } : {}),
      }
    }),
  }
}

const resolveWorker = (): { workerPath: string; execArgv: string[] } => {
  const compiled = [path.join(__dirname, 'historyWorker.js'), path.join(__dirname, '..', 'dist', 'historyWorker.js')]
  for (const candidate of compiled) {
    if (fs.existsSync(candidate)) {
      return { workerPath: candidate, execArgv: [] }
    }
  }
  const source = path.join(__dirname, 'historyWorker.ts')
  if (fs.existsSync(source)) {
    return {
      workerPath: source,
      execArgv: ['--require', require.resolve('ts-node/register')],
    }
  }
  throw new Error('Unable to locate the history worker entry point')
}

export class WorkerHistoryProvider implements HistoryApi {
  private worker: Worker | undefined
  private readonly pending = new Map<number, PendingRequest>()
  private nextId = 1
  private failed: Error | undefined
  private closing = false

  constructor(
    private readonly config: HistoryWorkerConfig['influx'],
    private readonly selfId: string,
    private readonly debug: (...args: unknown[]) => void,
  ) {}

  async start(): Promise<boolean> {
    if (process.env.SIGNALK_INFLUX_DISABLE_HISTORY_WORKER === '1') {
      return false
    }
    try {
      const { workerPath, execArgv } = resolveWorker()
      const config: HistoryWorkerConfig = {
        influx: this.config,
        selfId: this.selfId,
        debugEnabled: Boolean((this.debug as { enabled?: boolean }).enabled),
      }
      const worker = new Worker(workerPath, { workerData: config, execArgv })
      this.worker = worker
      worker.on('message', (message: HistoryWorkerResponse) => {
        this.receive(message)
      })
      worker.on('error', (error: Error) => {
        this.fail(error)
      })
      worker.on('exit', (code) => {
        if (!this.closing) {
          this.fail(new Error(`History worker exited with code ${code}`))
        }
      })
      await new Promise<void>((resolve, reject) => {
        worker.once('online', resolve)
        worker.once('error', reject)
      })
      return true
    } catch (error) {
      this.debug(`History worker unavailable, using the main thread: ${(error as Error).message}`)
      this.worker = undefined
      return false
    }
  }

  getValues(query: ValuesRequest): Promise<ValuesResponse> {
    return this.call<ValuesResponse>({
      method: 'getValues',
      query: serializeValuesQuery(query),
    })
  }

  getContexts(): Promise<ContextsResponse> {
    return this.call<ContextsResponse>({ method: 'getContexts' })
  }

  getPaths(): Promise<PathsResponse> {
    return this.call<PathsResponse>({ method: 'getPaths' })
  }

  getDailyLog(): Promise<{ length: number }> {
    return this.call<{ length: number }>({ method: 'getDailyLog' })
  }

  async close(): Promise<void> {
    if (this.closing) return
    this.closing = true
    const worker = this.worker
    if (!worker) return
    try {
      await this.call({ method: 'close' })
    } catch {
      // The worker may already have failed; terminating below is enough.
    }
    this.worker = undefined
    for (const pending of this.pending.values()) {
      pending.reject(new Error('History worker closed'))
    }
    this.pending.clear()
    await worker.terminate()
  }

  private call<T>(request: RequestInput): Promise<T> {
    if (this.failed) {
      return Promise.reject(this.failed)
    }
    const worker = this.worker
    if (!worker) {
      return Promise.reject(new Error('History worker is not running'))
    }
    const id = this.nextId++
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
      })
      worker.postMessage({ ...request, id })
    })
  }

  private receive(message: HistoryWorkerResponse): void {
    if (message.kind === 'debug') {
      this.debug(message.text)
      return
    }
    const pending = this.pending.get(message.id)
    if (!pending) return
    this.pending.delete(message.id)
    if (message.kind === 'result') {
      pending.resolve(message.value)
    } else {
      pending.reject(toError(message.error))
    }
  }

  private fail(error: Error): void {
    if (this.failed) return
    this.failed = error
    for (const pending of this.pending.values()) {
      pending.reject(error)
    }
    this.pending.clear()
  }
}
