// minipg — a small, pure-TypeScript PostgreSQL driver: plain SQL strings + params,
// named prepared statements, pluggable result modes, single connection or pool.
// No template tags, no LISTEN/NOTIFY, no COPY.
import { Connection } from './connection.ts'
import { Pool } from './pool.ts'
import type { ConnectConfig, PoolConfig } from './types.ts'

/** Open and authenticate a single connection. */
export async function connect(config: ConnectConfig = {}): Promise<Connection> {
  const conn = new Connection(config)
  await conn.connect()
  return conn
}

/** Create a lazy connection pool. */
export function createPool(config: PoolConfig = {}): Pool {
  return new Pool(config)
}

export { Connection, Pool }
export { PgError } from './errors.ts'
export { defaultDecoders } from './codec.ts'
export { Shape } from './shape.ts'
export type { ShapeSpec, ShapeMapper } from './shape.ts'
export { Json, JsonArray, Jsonb, JsonbArray } from './json.ts'
export type { JsonSpec, JsonMarker, JsTarget } from './json.ts'
export type {
  ConnectConfig, PoolConfig, QueryOptions, StreamOptions,
  QueryResult, ResultMode, Decoder, Field,
} from './types.ts'
