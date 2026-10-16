// make, layer and layerConfig for one runtime. Each runtime's client module passes the createPool of
// its own entry point, so the pool dials with that runtime's transport.
import * as Config from 'effect/Config'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import type * as Reactivity from 'effect/reactivity/Reactivity'
import type * as Scope from 'effect/Scope'
import type * as Client from 'effect/sql/SqlClient'
import type { SqlError } from 'effect/sql/SqlError'
import type { Pool } from '../pool.ts'
import type { PoolConfig } from '../types.ts'
import { layerFrom, makeFromPool, type MinipgClient, type MinipgClientConfig } from './MinipgClient.ts'

const toPoolConfig = (config: MinipgClientConfig): PoolConfig => {
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
  }
}

const stripUndefined = <T extends object>(object: T): T =>
  Object.fromEntries(Object.entries(object).filter(([, value]) => value !== undefined)) as T

export const makeRuntime = (createPool: (config: PoolConfig) => Pool) => {
  const make = (config: MinipgClientConfig): Effect.Effect<MinipgClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
    Effect.flatMap(
      Effect.acquireRelease(
        Effect.sync(() => createPool(stripUndefined(toPoolConfig(config)))),
        (pool) => Effect.promise(() => pool.end()),
      ),
      (pool) => makeFromPool(pool, config),
    )
  const layer = (config: MinipgClientConfig): Layer.Layer<MinipgClient | Client.SqlClient, SqlError> =>
    layerFrom(make(config))
  const layerConfig = (config: Config.Wrap<MinipgClientConfig>): Layer.Layer<MinipgClient | Client.SqlClient, Config.ConfigError | SqlError> =>
    layerFrom(Effect.flatMap(Config.unwrap(config), make))
  return { make, layer, layerConfig }
}
