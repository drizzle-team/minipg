// make, layer and layerConfig for one runtime. Each runtime's client module passes the createPool of
// its own entry point, so the pool dials with that runtime's transport.
import * as Config from 'effect/Config'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import type { Pool } from '../pool.ts'
import type { PoolConfig } from '../types.ts'
import { layerFrom, makeFromPool, type MinipgClientConfig } from './MinipgClient.ts'

const toPoolConfig = (config: MinipgClientConfig) => {
  const toMillis = (duration: Duration.Input | undefined) => (duration === undefined ? undefined : Duration.toMillis(duration))
  return {
    url: config.url && Redacted.value(config.url),
    host: config.host,
    port: config.port,
    path: config.path,
    ssl: config.ssl,
    database: config.database,
    user: config.username,
    password: config.password && Redacted.value(config.password),
    connectTimeout: toMillis(config.connectTimeout),
    idleTimeoutMillis: toMillis(config.idleTimeout),
    max: config.maxConnections,
    applicationName: config.applicationName,
    socket: config.socket,
    decode: config.decode,
    temporal: config.temporal,
    prepare: config.prepare,
    pipeline: config.pipeline,
    types: config.types,
    // Effect runs transaction statements on the transaction's connection itself.
    txGuard: false,
  } satisfies PoolConfig
}

export const makeRuntime = (createPool: (config: PoolConfig) => Pool) => {
  const make = (config: MinipgClientConfig) =>
    Effect.flatMap(
      Effect.acquireRelease(
        Effect.sync(() => createPool(toPoolConfig(config))),
        (pool) => Effect.promise(() => pool.end()),
      ),
      (pool) => makeFromPool(pool, config),
    )
  const layer = (config: MinipgClientConfig) =>
    layerFrom(make(config))
  const layerConfig = (config: Config.Wrap<MinipgClientConfig>) =>
    layerFrom(Effect.flatMap(Config.unwrap(config), make))
  return { make, layer, layerConfig }
}
