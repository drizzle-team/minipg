// Runtime-agnostic core entry — connect/createPool + the public API, with NO node:net/tls transport
// baked in (so it loads on workerd/Deno/edge). The node transport is added by ./index.ts (minipg/node);
// other runtimes import THIS module and supply config.socket. connect() without a transport throws.
import { Connection } from './connection.ts'
import { Pool } from './pool.ts'
import type { ConnectConfig, PoolConfig } from './types.ts'

/** Open and authenticate a single connection. Accepts a config or a `postgres://…` connection string. */
export async function connect(config: string | ConnectConfig = {}): Promise<Connection> {
  const conn = new Connection(typeof config === 'string' ? { url: config } : config)
  await conn.connect()
  return conn
}

/** Create a lazy connection pool. Accepts a config or a `postgres://…` connection string. */
export function createPool(config: string | PoolConfig = {}): Pool {
  return new Pool(config)
}

export { Connection, Pool }
export { replication, ReplicationConnection, ReplicationStreamEnded, lsnToString, lsnFromString } from './replication.ts'
export { Cursor } from './cursor.ts'
export type { CursorOptions } from './cursor.ts'
export type { ReplicationConfig, ReplicationEvent, ReplicationRelation, StartOptions, TableShape, Row } from './replication.ts'
export { PoolQuery } from './pool.ts'
export { parseConnectionString } from './url.ts'
export { PgError } from './errors.ts'
export { defaultDecoders } from './decode.ts'
export { defineType, isCustomMarker, type CustomMarker, type CustomType, type CustomTypeDef, type RawCell } from './registry.ts'
export { Shape } from './shape.ts'
export type { ShapeMapper } from './shape.ts'
export type { ShapeSpec, ShapeEntries, ShapeValue, TypeSpec, PgType } from './spec.ts'
export { Json, JsonArray, Jsonb, JsonbArray, Collect, CollectNullable, Transform, Nullable } from './json.ts'
export type { JsonSpec, SpecEntries, JsonMarker, JsTarget, CollectMarker, TransformMarker, NullableMarker } from './json.ts'
export type {
  ConnectConfig, PoolConfig, QueryOptions, StreamOptions, TxOptions,
  QueryResult, ResultMode, Decoder, Field,
} from './types.ts'
export type { TxFn } from './connection.ts'
