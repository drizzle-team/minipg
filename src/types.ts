import type * as tls from 'node:tls'
import type { Duplex } from 'node:stream'
import type { Plugin, QueryMetrics } from './plugin.ts'

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
  /** Per-query timings + sizes, present only when the query opted in with `{ metrics: true }`. */
  metrics?: QueryMetrics
}

export interface ConnectConfig {
  host?: string
  port?: number
  user?: string
  password?: string
  database?: string
  /** `true`/`'require'` = encrypt (no CA verification); object = passed to tls.connect. */
  ssl?: boolean | 'require' | 'verify-ca' | 'verify-full' | 'disable' | tls.ConnectionOptions
  applicationName?: string
  connectTimeout?: number
  /** Connect via a unix-domain socket at this path (e.g. /tmp/.s.PGSQL.5432) instead of
   *  host/port TCP — lower latency / higher throughput on the same machine; SSL is skipped. */
  path?: string
  /** Row-decode strategy for 'array'/'object' modes: 'jit' compiles a per-shape mapper with new Function
   *  (fastest, needs eval — Node/Bun/Deno); 'interpreted' resolves per-column decoders and loops (no eval —
   *  CSP / Cloudflare Workers); 'auto' (default) uses jit where eval is available, else interpreted. */
  decode?: 'auto' | 'jit' | 'interpreted'
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
  socket?: () => Duplex | Promise<Duplex>
  /** Telemetry/observability plugins (e.g. otel()/sentry() from 'minipg/telemetry'). They subscribe
   *  to query + connection lifecycle hooks; enabling any plugin turns on per-query timing capture. */
  plugins?: Plugin[]
}

export interface QueryOptions {
  /** Reuse a server-side prepared statement under this name (parse once, bind many). */
  name?: string
  mode?: ResultMode
  /** Attach per-query timings/sizes to the result as `.metrics` (works with or without plugins). */
  metrics?: boolean
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
  /** Close a connection that has sat idle in the pool for this many ms (0/omitted = never evict).
   *  Also surfaced as `pool.options.idleTimeoutMillis` + a `'release'` event so Vercel's
   *  attachDatabasePool() can keep the function instance alive long enough to drain idle connections. */
  idleTimeoutMillis?: number
  /** Single-flight reconnect on connection failure. `false` disables it (fail fast).
   *  Object tunes backoff (baseMs/maxMs) and how long an acquire waits for recovery. */
  reconnect?: boolean | { baseMs?: number; maxMs?: number; acquireTimeoutMs?: number }
}
