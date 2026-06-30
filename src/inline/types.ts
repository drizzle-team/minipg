import type * as tls from 'node:tls'
import type { Duplex } from 'node:stream'

/** Row-shape of a query result. */
export type ResultMode = 'array' | 'object' | 'buffer' | 'raw'

/** Decodes the raw text-format bytes of a single field into a JS value. */
export type Decoder = (buf: Buffer) => unknown

export interface Field {
  name: string
  tableOid: number
  columnId: number
  dataTypeOid: number
  dataTypeSize: number
  typeModifier: number
  format: number
}

export interface QueryResult<Row = unknown[]> {
  rows: Row[]
  columns: string[]
  rowCount: number | null
  command: string | null
}

export interface ConnectConfig {
  host?: string
  port?: number
  user?: string
  password?: string
  database?: string
  /** `true`/`'require'` = encrypt (no CA verification); object = passed to tls.connect. */
  ssl?: boolean | 'require' | tls.ConnectionOptions
  applicationName?: string
  connectTimeout?: number
  /** Per-OID decoder overrides for the parsed ('array'/'object') modes. */
  types?: Record<number, Decoder>
  /** How to decode integers inside ANY json/jsonb value that exceed 2^53 (which plain
   *  JSON.parse silently truncates). 'number' (default) = plain JSON.parse; 'string' /
   *  'bigint' = auto-preserve oversized integers as their exact string / BigInt, via the
   *  native JSON source-text reviver. Order-independent (works for jsonb). For a known
   *  shape, Json()/JsonArray() is faster — this is the zero-declaration safety net. */
  jsonBigints?: 'number' | 'string' | 'bigint'
  /** Auto-reconnect this connection after an unexpected drop (default off). Object
   *  tunes backoff (baseMs/maxMs) and maxRetries (null/omitted = retry forever). */
  reconnect?: boolean | { baseMs?: number; maxMs?: number; maxRetries?: number }
  /** Custom transport: return an already-connected duplex stream (bypasses net.connect
   *  and SSL). Enables unix sockets, alternative runtimes, and in-process testing. */
  socket?: () => Duplex
}

export interface QueryOptions {
  /** Reuse a server-side prepared statement under this name (parse once, bind many). */
  name?: string
  mode?: ResultMode
  /** Cancel the query (out-of-band CancelRequest) after this many milliseconds. */
  timeout?: number
  /** Cancel the query when this AbortSignal fires. */
  signal?: AbortSignal
}

export interface StreamOptions extends QueryOptions {
  /** Rows buffered before the socket is paused for backpressure (default 200). */
  highWaterMark?: number
}

export interface PoolConfig extends ConnectConfig {
  max?: number
  /** Single-flight reconnect on connection failure. `false` disables it (fail fast).
   *  Object tunes backoff (baseMs/maxMs) and how long an acquire waits for recovery. */
  reconnect?: boolean | { baseMs?: number; maxMs?: number; acquireTimeoutMs?: number }
}
