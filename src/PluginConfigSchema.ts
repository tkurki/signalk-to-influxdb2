/**
 * PluginConfig schema for UI and validation
 */
export const PluginConfigSchema = {
  $schema: 'http://json-schema.org/draft-07/schema#',
  type: 'object',
  properties: {
    outputDailyLog: {
      title: 'Output Daily Distance Log values',
      description:
        'Calculate periodically daily distance from navigation.position values since 00:00 local time and output under navigation.trip.daily',
      default: false,
      type: 'boolean',
    },
    influxes: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          url: {
            description: 'InfluxDB server address in http://hostname:port format',
            default: 'http://127.0.0.1:8086',
            title: 'Url',
            type: 'string',
          },
          token: {
            description: 'Authentication token',
            title: 'Token',
            type: 'string',
          },
          org: {
            title: 'Organisation',
            type: 'string',
          },
          bucket: {
            title: 'Bucket',
            type: 'string',
          },
          onlySelf: {
            title: 'Store only self data',
            default: true,
            description: 'Store data only for "self" vessel, not for example AIS targets\' data',
            type: 'boolean',
          },
          filteringRules: {
            title: 'Filtering Rules',
            default: [],
            description:
              'Filtering rules for allowing and/or ignoring data for writing. First matching rule decides whether to allow writing or ignore and not write the value. If there are rules but none of them match the value will be written. Adding rules disables ignoredPaths and ignoredValues. To pick some values add allow rules for them and an "ignore all" rule at the end with .* in either path or source field. Activate Debug logging in plugin configuration to get logging on server startup about ignored and allowed data.',
            type: 'array',
            items: {
              type: 'object',
              properties: {
                allow: {
                  title: 'Allow rule',
                  description:
                    'Check to use this rule for picking data for writing, otherwise this rule will cause data to be ignored.',
                  type: 'boolean',
                },
                path: {
                  title: 'Path',
                  description: 'Literal value or JS regular expression.',
                  type: 'string',
                },
                source: {
                  title: 'Source',
                  description:
                    "Literal value or JS regular expression. You can copypaste values from server's Data Browser",
                  type: 'string',
                },
              },
            },
          },
          ignoredPaths: {
            title: 'Ignored paths',
            default: [],
            description: 'Paths that should be ignored and not written to InfluxDB (JS regular expressions work)',
            type: 'array',
            items: {
              type: 'string',
            },
          },
          ignoredSources: {
            title: 'Ignored sources',
            default: [],
            description:
              'Sources whose data should be ignored and not written to InfluxDB (JS regular expressions work)',
            type: 'array',
            items: {
              type: 'string',
            },
          },
          useSKTimestamp: {
            title: 'Use timestamps from SK data',
            default: false,
            description: 'Whether the timestamps in SK data should be used instead of time of insertion to InfluxDB',
            type: 'boolean',
          },
          resolution: {
            title: 'Resolution (milliseconds)',
            default: ': 0',
            description:
              'Time resolution of data written to the database. Zero means write all data, 1000 means write each context-path-source combination once per second. Updates arriving more quicker will not be written.',
            type: 'number',
          },
          writeOptions: {
            type: 'object',
            properties: {
              batchSize: {
                description: 'max number of records/lines to send in a batch',
                type: 'number',
              },
              flushInterval: {
                description:
                  'delay between data flushes in milliseconds, at most `batch size` records are sent during flush',
                type: 'number',
              },
              defaultTags: {
                description: 'default tags, unescaped',
                type: 'object',
              },
              headers: {
                description: 'HTTP headers that will be sent with every write request',
                type: 'object',
                additionalProperties: {
                  type: 'string',
                },
              },
              gzipThreshold: {
                description: 'When specified, write bodies larger than the threshold are gzipped',
                type: 'number',
              },
              maxBatchBytes: {
                description: 'max size of a batch in bytes',
                type: 'number',
              },
              consistency: {
                description:
                  'InfluxDB Enterprise write consistency as explained in https://docs.influxdata.com/enterprise_influxdb/v1.9/concepts/clustering/#write-consistency',
                enum: ['all', 'any', 'one', 'quorum'],
                type: 'string',
              },
              writeFailed: {
                description: 'WriteFailed is called to inform about write errors.',
                type: 'object',
              },
              writeSuccess: {
                description: 'WriteSuccess is informed about successfully written lines.',
                type: 'object',
              },
              writeRetrySkipped: {
                description:
                  'WriteRetrySkipped is informed about lines that were removed from the retry buffer\nto keep the size of the retry buffer under the configured limit (maxBufferLines).',
                type: 'object',
              },
              maxRetries: {
                description: 'max count of retries after the first write fails',
                type: 'number',
              },
              maxRetryTime: {
                description: 'max time (millis) that can be spent with retries',
                type: 'number',
              },
              maxBufferLines: {
                description: 'the maximum size of retry-buffer (in lines)',
                type: 'number',
              },
              retryJitter: {
                description: 'add `random(retryJitter)` milliseconds delay when retrying HTTP calls',
                type: 'number',
              },
              minRetryDelay: {
                description: 'minimum delay when retrying write (milliseconds)',
                type: 'number',
              },
              maxRetryDelay: {
                description: 'maximum delay when retrying write (milliseconds)',
                type: 'number',
              },
              exponentialBase: {
                description: 'base for the exponential retry delay',
                type: 'number',
              },
              randomRetry: {
                description:
                  'randomRetry indicates whether the next retry delay is deterministic (false) or random (true).\nThe deterministic delay starts with `minRetryDelay * exponentialBase` and it is multiplied\nby `exponentialBase` until it exceeds `maxRetryDelay`.\nWhen random is `true`, the next delay is computed as a random number between next retry attempt (upper)\nand the lower number in the deterministic sequence. `random(retryJitter)` is added to every returned value.',
                type: 'boolean',
              },
            },
          },
        },
      },
    },
  },
}
