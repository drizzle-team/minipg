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
  /** TLS sslmode string (PostgreSQL-style), or a tls.ConnectionOptions object for full control:
   *  - false / 'disable'  — no TLS
   *  - true / 'require'    — encrypt, but DO NOT verify the server cert (MITM-exposed)
   *  - 'verify-ca'         — verify the cert chain (system CAs unless you pass a custom one)
   *  - 'verify-full'       — verify chain AND that the cert matches the host (recommended)
   *  - object              — passed to tls.connect; verifies by default; supply { ca, cert, key,
   *                          rejectUnauthorized, servername } for custom CA / mTLS / overrides
   *  If the server declines TLS while any TLS mode is set, the connection fails (no silent downgrade). */
  ssl?: boolean | 'disable' | 'require' | 'verify-ca' | 'verify-full' | tls.ConnectionOptions
  applicationName?: string
  connectTimeout?: number
  /** Connect via a unix-domain socket at this path (e.g. /tmp/.s.PGSQL.5432) instead of
   *  host/port TCP — lower latency / higher throughput on the same machine; SSL is skipped. */
  path?: string
  /** Per-OID decoder overrides for the parsed ('array'/'object') modes. */
  types?: Record<number, Decoder>
  /** Auto-reconnect this connection after an unexpected drop (default off). Object
   *  tunes backoff (baseMs/maxMs) and maxRetries (null/omitted = retry forever). */
  reconnect?: boolean | { baseMs?: number; maxMs?: number; maxRetries?: number }
  /** Custom transport: return a connected duplex stream (bypasses net.connect and SSL),
   *  synchronously or as a Promise. Called on every (re)connect. Enables SSH tunnels
   *  (ssh2 forwardOut), unix sockets, alternative runtimes, and in-process testing. */
  socket?: () => Duplex | Promise<Duplex>
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
