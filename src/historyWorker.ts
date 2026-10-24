import { parentPort, workerData } from 'node:worker_threads'
import type { ValuesRequest } from '@signalk/server-api/history'
import { InfluxHistoryProvider, getDailyLogData } from './HistoryAPI'
import { createInfluxQuerySource } from './influx'
import type {
  HistoryWorkerConfig,
  HistoryWorkerRequest,
  HistoryWorkerResponse,
  SerializedError,
} from './historyWorkerProtocol'

if (!parentPort) {
  throw new Error('History worker must run in a worker thread')
}

const port = parentPort
const config = workerData as HistoryWorkerConfig
const source = createInfluxQuerySource(config.influx)

const debug = (...args: unknown[]): void => {
  if (config.debugEnabled) {
    port.postMessage({
      kind: 'debug',
      text: args.map(String).join(' '),
    } satisfies HistoryWorkerResponse)
  }
}

const provider = new InfluxHistoryProvider(source, config.selfId, debug)

const serializeError = (error: unknown): SerializedError => {
  if (error instanceof Error) {
    const code = (error as NodeJS.ErrnoException).code
    return {
      message: error.message,
      name: error.name,
      ...(code ? { code } : {}),
    }
  }
  return { message: String(error) }
}

const processRequest = async (request: HistoryWorkerRequest): Promise<void> => {
  try {
    switch (request.method) {
      case 'getValues': {
        const result = await provider.getValues(request.query as unknown as ValuesRequest)
        port.postMessage({ kind: 'result', id: request.id, value: result })
        return
      }
      case 'getContexts': {
        const result = await provider.getContexts()
        port.postMessage({ kind: 'result', id: request.id, value: result })
        return
      }
      case 'getPaths': {
        const result = await provider.getPaths()
        port.postMessage({ kind: 'result', id: request.id, value: result })
        return
      }
      case 'getDailyLog': {
        const result = await getDailyLogData(source, config.selfId, debug)
        port.postMessage({ kind: 'result', id: request.id, value: result })
        return
      }
      case 'close': {
        port.postMessage({ kind: 'result', id: request.id })
        return
      }
    }
  } catch (error) {
    port.postMessage({
      kind: 'error',
      id: request.id,
      error: serializeError(error),
    })
  }
}

port.on('message', (request: HistoryWorkerRequest) => {
  void processRequest(request)
})
