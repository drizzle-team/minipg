import type * as tls from 'node:tls'
import type { Duplex } from 'node:stream'
import type { Plugin, QueryMetrics } from './plugin.ts'
import type { ShapeSpec } from './spec.ts'
import type { ShapeMapper } from './shape.ts'

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
  /** The resolved decode plan, present only when the query opted in with `{ debug: true }`. */
  debug?: QueryDebug
}

/** How a query's result was decoded — surfaced by `{ debug: true }` for inspection/tuning. */
export interface QueryDebug {
  sql: string
  mode: ResultMode
  /** Server-side prepared-statement name (undefined = unnamed / simple execution). */
  statementName?: string
  /** True if this reused a cached prepared statement (Parse skipped — 2nd+ execution). */
  reusedPreparedStatement: boolean
  /** True if this statement name was cached bound to DIFFERENT SQL, so the old prepared statement was
   *  deallocated and re-Parsed. Reusing one name for alternating SQL re-Parses on every switch (a footgun —
   *  prefer one name per distinct SQL). */
  repreparedSqlChanged?: boolean
  /** Which row mapper ran: 'jit' (compiled per-shape function), 'interpreted' (per-column loop, no eval),
   *  or 'none' (buffer/raw modes return undecoded cells). */
  decode: 'jit' | 'interpreted' | 'none'
  /** The generated JIT mapper source (only when `decode === 'jit'`). */
  mapperSource?: string
  /** The resolved column plan: wire OID + the format we requested + any JS-target/JSON override. */
  columns: Array<{ name: string; oid: number; format: 'text' | 'binary'; js?: string; json?: boolean }>
  /** Transparent auto-retries performed on this query (absent if the first attempt succeeded). A prepared
   *  statement invalidated by DDL (SQLSTATE 0A000 "cached plan must not change result type" / 26000
   *  "prepared statement does not exist") is Close+re-Parsed and re-run once — safe because both are raised
   *  before execution, so no rows were affected. */
  retries?: number
  /** SQLSTATE codes of the swallowed attempts that triggered a retry, e.g. `['0A000']`. */
  retriedErrors?: string[]
}

export interface ConnectConfig {
  /** A `postgres://user:pass@host:port/db?sslmode=…` connection string. Its values are used as defaults;
   *  any explicit field below overrides the matching part of the URL. Also accepted as `connect(url)` /
   *  `createPool(url)`. Supports the sslmode/application_name/connect_timeout params + unix `?host=/path`. */
  url?: string
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
  /** How date/timestamp/timestamptz columns decode by default: 'date' (default) = a JS Date (like
   *  pg/postgres.js); 'string' = the exact PG text, lossless (keeps µs, BC eras, 5-digit years, and
   *  infinity, none of which a JS Date can hold). Per-column `:string`/`:date`/`:ms` targets in a
   *  shape/queryTyped always override this. Note: Date is millisecond-precision. */
  temporal?: 'date' | 'string'
  /** Per-OID decoder overrides for the parsed ('array'/'object') modes. */
  types?: Record<number, Decoder>
  /** How to decode integers inside ANY json/jsonb value that exceed 2^53 (which plain
   *  JSON.parse silently truncates). 'number' (default) = plain JSON.parse; 'string' /
   *  'bigint' = auto-preserve oversized integers as their exact string / BigInt, via the
   *  native JSON source-text reviver. Order-independent (works for jsonb). For a known
   *  shape, Json()/JsonArray() is faster — this is the zero-declaration safety net. */
  jsonBigints?: 'number' | 'string' | 'bigint'
  /** Use server-side NAMED prepared statements for `{ name }` queries and the reused-chunks builder.
   *  Set `false` behind a transaction-mode pooler (PgBouncer/Supavisor) where a named statement prepared
   *  on one backend may not exist on the next — every query then goes unnamed (Parse+Bind+Execute).
   *  Default: auto — off when a pooler is detected (Neon `-pooler` host, Supabase `pooler.supabase.com`
   *  / port 6543, or the Vercel serverless runtime), on otherwise. Explicit value always wins. */
  prepare?: boolean
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
  /** Decode this query's result with a declared column shape (a `ShapeSpec` object, or a `Shape()` mapper —
   *  its `$cols` are reused). Enables typed/binary decode via the same cached jit/interpreted mapper the
   *  driver uses for every query; columns marked `format:'binary'` request the binary wire format. */
  shape?: ShapeSpec | ShapeMapper
  /** Force the BINARY result wire format for EVERY column (manual/testing lever — the shape path picks
   *  binary per-column automatically). Decodes via each column's binary decoder from the RowDescription
   *  OIDs. Columns whose type has no binary decoder (numeric, money, json, jsonb, …) will error — select
   *  only binary-supported types. Ignored when `shape` is given (the shape controls per-column formats). */
  binary?: boolean
  /** Attach per-query timings/sizes to the result as `.metrics` (works with or without plugins). `true`
   *  reports durations as sub-ms floating-point milliseconds; `'ms'` rounds to whole-integer milliseconds;
   *  `'us'` reports integer microseconds (best for sub-ms queries). The unit is echoed in `metrics.unit`. */
  metrics?: boolean | 'ms' | 'us'
  /** Attach the resolved decode plan to the result as `.debug` — statement name, jit/interpreted, the
   *  generated JIT mapper source, and the per-column text/binary format we picked. For inspection/tuning. */
  debug?: boolean
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
