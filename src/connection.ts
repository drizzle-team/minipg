// A single PostgreSQL connection: transport (net/tls), auth, and the extended
// query protocol. One query in flight at a time (queries queue). No LISTEN/NOTIFY.
import type { Duplex } from 'node:stream'
import { W, Writer, writeParse, writeDescribe, writeBind, writeExecute, writeClose, writeSync, Parser, parseRowDescription, parseDataRow } from './protocol.ts'
import { md5Password, scram, type Scram } from './auth.ts'
import { buildDecoders, decoderFor, encodeParam } from './codec.ts'
import { PgError, parseErrorFields } from './errors.ts'
import type { ConnectConfig, Decoder, Field, QueryDebug, QueryOptions, QueryResult, ResultMode, StreamOptions } from './types.ts'
import { INSTANT_OIDS, BINARY_FAST, type CodegenCol } from './decode2.ts'
import { buildMapperFactory, type RowMapper, type RowMapperFactory } from './mapper.ts'
import { resolveUrl } from './url.ts'
import { shapeCols, type ShapeSpec, type ShapeOf } from './spec.ts'
import type { ShapeMapper } from './shape.ts'
import type { Plugin, QueryInfo, QueryMetrics } from './plugin.ts'

type ConnState = 'idle' | 'connecting' | 'ready' | 'reconnecting' | 'closed'

// The node:net/tls transport is registered here by the Node entry (minipg/node) so the core never
// statically imports node:net/tls (which don't exist on workerd). Edge/Deno entries pass config.socket.
type TransportFactory = (cfg: NormalizedConfig, signal: AbortSignal) => Promise<Duplex>
type CancelFn = (cfg: NormalizedConfig, key: { pid: number; secret: number }) => void
let defaultTransport: TransportFactory | null = null
let defaultCancel: CancelFn | null = null
export function registerDefaultTransport(transport: TransportFactory, cancel: CancelFn): void {
  defaultTransport = transport; defaultCancel = cancel
}

export interface NormalizedConfig {
  host: string; port: number; user: string; password: string; database: string
  ssl: Exclude<NonNullable<ConnectConfig['ssl']>, 'disable'> // 'disable' normalized to false
  applicationName: string; connectTimeout: number; decoders: Map<number, Decoder>
  prepare: boolean // false -> never use server-side named prepared statements (transaction-pooler safe)
  pipelineDepth: number // max queries in flight on one connection at once (1 = gated / no pipelining)
  pipelineFlush: 'sync' | 'microtask' // batch same-tick writes into one socket write ('microtask') or write per dispatch ('sync')
  temporal: 'date' | 'string' // default decode for date/timestamp(tz) columns without an explicit target
  reuseBinaryOids: Set<number> // BINARY_FAST minus config.types overrides: cols to upgrade to binary on prepared-statement reuse
  reconnect: { enabled: boolean; base: number; max: number; maxRetries: number | null }
  socket?: () => Duplex | Promise<Duplex>
  path?: string
  plugins: Plugin[]
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))
const RTT_WINDOW = 5 // keep this many most-recent round-trip samples (ms) for connection.rtt
const DEFAULT_PIPELINE_DEPTH = 100 // max in-flight queries on one connection when pipelining is on (matches postgres.js)
const FLUSH_THRESHOLD = 64 * 1024  // flush the batch immediately once it reaches this many bytes, even in microtask mode (bounds memory, starts the transfer)

// Detect a transaction-mode pooler (where server-side NAMED prepared statements are unsafe — a Parse on
// one backend may not exist on the next Bind). Used only to DEFAULT `prepare` off; explicit config wins.
function transactionPoolerDetected(host: string, port: number): boolean {
  if (host.includes('-pooler.')) return true                              // Neon PgBouncer pooled endpoint
  if (host.includes('pooler.supabase.com') || port === 6543) return true  // Supabase Supavisor transaction pooler
  if (process.env.VERCEL || process.env.VERCEL_ENV) return true           // Vercel serverless: connections are ~always pooled
  return false
}
// Errors that won't fix themselves on retry (bad auth/config) — stop reconnecting.
function isFatalAuth(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code
  if (code === '28P01' || code === '28000' || code === '3D000') return true
  return !!(err as { fatal?: boolean } | null)?.fatal
}

// per-query timing accumulator, attached only when instrumented (plugins present) or metrics requested
interface TaskPerf { t0: number; tStart?: number; tWrite?: number; tFirst?: number; decodeMs: number; sent: number; recv: number; states?: unknown[]; metrics?: QueryMetrics }

interface Task {
  sql: string
  params: unknown[]
  name?: string | undefined
  mode: ResultMode
  rows: unknown[]
  perf?: TaskPerf
  info?: QueryInfo
  wantMetrics?: boolean
  metricsUnit?: 'ms' | 'us'
  metricsRound?: boolean
  debug?: boolean // { debug: true }: attach the resolved decode plan to the result
  _debugCols?: CodegenCol[] // captured column plan (name/oid/format/js) for debug
  _reused?: boolean // this execution reused a cached prepared statement (Parse skipped)
  _retries?: number // transparent auto-retries performed (DDL-invalidated prepared statement)
  _retryErrors?: string[] // SQLSTATE codes of the swallowed attempts
  _repreparedSqlChanged?: boolean // the name was cached with DIFFERENT SQL -> old statement deallocated + re-Parsed
  _instrDone?: boolean // guard so onQueryEnd/Error fires exactly once across the settle paths
  _wAt?: number // write timestamp (performance.now) — for the always-on live round-trip sample
  _rttDone?: boolean // guard so a query contributes at most one RTT sample (its first response byte)
  fields?: Field[]
  mapper?: RowMapper
  command?: string | null
  rowCount?: number | null
  error?: Error
  cancelled?: boolean
  stream?: boolean
  _cacheName?: string
  resultFormat?: number | number[] // result-format code(s) for Bind (ORM binary flow)
  _typed?: boolean // builder came from a caller-supplied column plan — don't override from RowDescription
  binary?: boolean // { binary: true }: force all columns binary; build the mapper binary from RowDescription OIDs
  timeout?: number
  signal?: AbortSignal
  timer?: ReturnType<typeof setTimeout>
  graceTimer?: ReturnType<typeof setTimeout>
  settled?: boolean // caller already resolved/rejected (by timeout/abort) — don't settle again
  signalCleanup?: () => void
  onRow?: (row: unknown) => void
  streamEnd?: (summary: { command?: string | null; rowCount?: number | null }) => void
  streamError?: (e: Error) => void
  resolve?: (r: QueryResult<never>) => void
  reject?: (e: Error) => void
}

function defaultUser(): string {
  // env-derived so the core needs no node:os (absent on workerd); Node/Bun always set one of these.
  return process.env.USER || process.env.USERNAME || process.env.LOGNAME || 'postgres'
}
// Join builder chunks into a $1/$2/… parameterized SQL string (V8 cons-strings make concat cheap).
function buildChunkSql(chunks: readonly string[]): string {
  let s = chunks[0] ?? ''
  for (let i = 1; i < chunks.length; i++) s += '$' + i + chunks[i]
  return s
}
function readCstrings(buf: Buffer): string[] {
  const out: string[] = []; let i = 0
  while (i < buf.length && buf[i] !== 0) { let e = i; while (e < buf.length && buf[e] !== 0) e++; out.push(buf.toString('utf8', i, e)); i = e + 1 }
  return out
}
const firstCstr = (buf: Buffer) => { const z = buf.indexOf(0); return buf.toString('utf8', 0, z === -1 ? buf.length : z) }

function makeRow(cells: (Buffer | null)[], body: Buffer, mode: ResultMode, fields: Field[], decoders: Map<number, Decoder>): unknown {
  switch (mode) {
    case 'raw': return Buffer.from(body)
    case 'buffer': return cells.map((c) => (c == null ? null : Buffer.from(c)))
    case 'object': {
      const o: Record<string, unknown> = Object.create(null) // null-proto: a column named __proto__ can't pollute
      for (let i = 0; i < fields.length; i++) { const f = fields[i]!; const c = cells[i]; o[f.name] = c == null ? null : decoderFor(f.dataTypeOid, decoders)(c) }
      return o
    }
    default: { // 'array'
      const r = new Array(fields.length)
      for (let i = 0; i < fields.length; i++) { const f = fields[i]!; const c = cells[i]; r[i] = c == null ? null : decoderFor(f.dataTypeOid, decoders)(c) }
      return r
    }
  }
}

export class Connection {
  readonly cfg: NormalizedConfig
  state: ConnState = 'idle'
  serverParams: Record<string, string> = {}
  backendKey: { pid: number; secret: number } | null = null
  txStatus = 'I' // last ReadyForQuery transaction status: I idle | T in-tx | E failed-tx

  /** True if the connection is inside (or in a failed) transaction. */
  get inTransaction(): boolean { return this.txStatus === 'T' || this.txStatus === 'E' }

  // The task currently receiving results — the head of the in-flight FIFO. Every backend message
  // (RowDescription/DataRow/CommandComplete/Error) belongs to it until its ReadyForQuery arrives.
  private get current(): Task | null { return this.inflight[0] ?? null }

  /** Live scheduler snapshot: how many queries are queued, in flight now, and the max ever in flight
   *  at once (a nonzero-above-1 max confirms pipelining actually engaged). */
  get stats(): { queued: number; inflight: number; maxInflight: number; pipelineDepth: number; writes: number } {
    return { queued: this.queue.length, inflight: this.inflight.length, maxInflight: this._maxInflight, pipelineDepth: this.cfg.pipelineDepth, writes: this._flushes }
  }

  /** Recent network round-trip samples in ms (oldest→newest, up to RTT_WINDOW). Seeded at connect
   *  from the auth-handshake legs (the polluted startup→first-response leg is skipped), then kept
   *  fresh by each query's time-to-first-byte. `min` ≈ the network floor; `avg` smooths jitter.
   *  Handy for tuning things like pipelining. All fields are null/empty until the first sample. */
  get rtt(): { avg: number | null; min: number | null; last: number | null; count: number; samples: number[] } {
    const s = this.rttSamples
    if (s.length === 0) return { avg: null, min: null, last: null, count: 0, samples: [] }
    let sum = 0, min = Infinity
    for (const x of s) { sum += x; if (x < min) min = x }
    return { avg: sum / s.length, min, last: s[s.length - 1]!, count: s.length, samples: s.slice() }
  }
  private recordRtt(ms: number): void {
    if (!Number.isFinite(ms) || ms < 0) return
    this.rttSamples.push(ms)
    if (this.rttSamples.length > RTT_WINDOW) this.rttSamples.shift()
  }
  private markPing(): void { this.lastPingAt = performance.now() }                                                    // sent a client msg that expects a reply
  private pong(): void { if (this.lastPingAt > 0) { this.recordRtt(performance.now() - this.lastPingAt); this.lastPingAt = 0 } } // …its reply arrived: record + disarm

  private socket: Duplex | null = null
  private parser = new Parser()
  private queue: Task[] = []
  private inflight: Task[] = [] // in-flight FIFO (oldest→newest); the backend replies in this order, so the
                               // head (inflight[0]) is the task currently receiving results. length ≤ pipelineDepth
  private outbuf = new Writer() // outbound batch: pipelined tasks serialize here, flushed once per dispatch pass
  private flushScheduled = false // a microtask flush is pending (guards against scheduling more than one)
  private _maxInflight = 0      // high-water mark of concurrent in-flight queries (diagnostics / tests)
  private _flushes = 0          // count of outbound socket writes for query batches (coalescing diagnostic)
  private rttSamples: number[] = [] // network round-trip samples (ms), oldest→newest, capped at RTT_WINDOW
  private lastPingAt = 0            // send time of the last handshake msg awaiting a reply (0 = none pending)
  private prepared = new Map<string, { sql: string; fields: Field[] }>()
  private staleStatements = new Set<string>() // names invalidated by DDL (0A000/26000) — Close before re-Parse
  private mapperCache = new Map<string, RowMapper>() // per-shape row mappers (standard + typed queries)
  private mapperFactory: RowMapperFactory            // interpreted or jit, chosen once from config.decode
  // query-builder fast path: a chunks array (tagged template / builder) has stable identity, so cache
  // its joined SQL + an auto-assigned prepared-statement name keyed by the array. WeakMap => auto-GC.
  private chunkCache = new WeakMap<readonly string[], { name: string; sql: string }>()
  private chunkSeq = 0
  private scramState?: Scram

  private connecting = false
  private connectAbort?: AbortController // aborted on failAttempt so a pending transport tears down its socket
  private connTimer?: ReturnType<typeof setTimeout>
  private connectPromise?: Promise<this>
  private attemptResolve?: () => void
  private attemptReject?: (e: Error) => void
  private ended = false
  private everConnected = false
  private instrumented = false // plugins present -> capture per-query timings + fire hooks

  constructor(config: ConnectConfig = {}) {
    config = resolveUrl(config) // fold a `url` connection string into defaults (explicit fields still win)
    const user = config.user || process.env.PGUSER || defaultUser()
    const host = config.host || process.env.PGHOST || 'localhost'
    const port = config.port || Number(process.env.PGPORT) || 5432
    const pooled = transactionPoolerDetected(host, port) // gates the `prepare` + `pipeline` auto-defaults
    this.cfg = {
      host,
      port,
      user,
      password: config.password ?? process.env.PGPASSWORD ?? '',
      database: config.database || process.env.PGDATABASE || user,
      ssl: config.ssl && config.ssl !== 'disable' ? config.ssl : false, // 'disable'/falsy -> no TLS
      applicationName: config.applicationName || 'minipg',
      connectTimeout: config.connectTimeout ?? 30000,
      prepare: config.prepare ?? !pooled, // explicit wins; else off behind a pooler
      // Pipelining: default on (auto-off behind a transaction pooler, where multiple in-flight implicit
      // transactions on one client conn confuse the pooler's per-tx server assignment). Explicit wins.
      pipelineDepth: ((p) => (p === false ? 1 : p === true ? DEFAULT_PIPELINE_DEPTH : p == null ? (pooled ? 1 : DEFAULT_PIPELINE_DEPTH) : Math.max(1, p.depth ?? DEFAULT_PIPELINE_DEPTH)))(config.pipeline),
      pipelineFlush: (typeof config.pipeline === 'object' && config.pipeline.flush) || 'microtask', // coalesce same-tick writes by default
      temporal: config.temporal ?? 'date',
      decoders: buildDecoders(config.types, config.jsonBigints),
      // types to auto-upgrade to binary on prepared-statement reuse — the bench-fast set, minus any OID the
      // user overrode via config.types (binary decode bypasses the text override, so leave those as text).
      reuseBinaryOids: new Set([...BINARY_FAST].filter((oid) => !(config.types && oid in config.types))),
      reconnect: ((rc) => {
        const o = rc && typeof rc === 'object' ? rc : {}
        return { enabled: rc === true || (rc != null && typeof rc === 'object'), base: o.baseMs ?? 100, max: o.maxMs ?? 5000, maxRetries: o.maxRetries ?? null }
      })(config.reconnect),
      socket: config.socket,
      path: config.path,
      plugins: config.plugins ?? [],
    }
    this.instrumented = this.cfg.plugins.length > 0
    this.mapperFactory = buildMapperFactory(config.decode) // 'auto' (default): jit where eval available, else interpreted
    // keep the password out of console.log / JSON / inspection of the connection
    Object.defineProperty(this.cfg, 'password', { value: this.cfg.password, enumerable: false, writable: true, configurable: true })
  }

  private rejectTask(t: Task, err: Error): void {
    if (t.stream) t.streamError?.(err); else t.reject?.(err)
  }

  // ---- telemetry helpers (no-ops unless instrumented / metrics requested) ----
  private beginPerf(t: Task, params: unknown[], metrics: boolean | 'ms' | 'us' | undefined): void {
    if (!this.instrumented && !metrics) return
    t.wantMetrics = !!metrics
    t.metricsUnit = metrics === 'us' ? 'us' : 'ms'
    t.metricsRound = metrics === 'ms' || metrics === 'us' // 'ms'/'us' -> integer; true -> keep sub-ms float
    t.perf = { t0: performance.now(), decodeMs: 0, sent: 0, recv: 0 }
    t.info = { sql: t.sql, statementName: t.name, paramCount: Array.isArray(params) ? params.length : 0, database: this.cfg.database, host: this.cfg.host, prepared: false }
  }
  private fireStart(t: Task): void {
    const p = t.perf; if (!p) return
    if (t.info) { t.info.backendPid = this.backendKey?.pid; }
    if (this.instrumented && p.states === undefined) p.states = this.cfg.plugins.map((pl) => pl.onQueryStart?.(t.info!)) // once, even across a retry
  }
  // Compute metrics + fire onQueryEnd/onQueryError exactly once. Returns the metrics (for result attach).
  private fireEnd(t: Task, err?: Error): QueryMetrics | undefined {
    const p = t.perf; if (!p || t._instrDone) return p?.metrics
    t._instrDone = true
    const end = performance.now()
    const start = p.tStart ?? p.t0, wrote = p.tWrite ?? start
    // performance.now() is float ms, so 'us' MULTIPLIES by 1000 (no division anywhere); 'ms' rounds; true
    // keeps the float. 1000/1e6 aren't powers of two, so there's no bit-shift shortcut — but none is needed.
    const unit = t.metricsUnit ?? 'ms'
    const c = t.metricsRound ? (unit === 'us' ? (x: number) => Math.round(x * 1000) : (x: number) => Math.round(x)) : (x: number) => x
    const m: QueryMetrics = {
      unit,
      queueWait: c(start - p.t0), write: c(wrote - start),
      ttfb: c(p.tFirst !== undefined ? p.tFirst - wrote : 0),
      download: c(p.tFirst !== undefined ? end - p.tFirst : 0),
      decode: c(p.decodeMs), total: c(end - p.t0),
      bytesSent: p.sent, bytesReceived: p.recv,
      rowCount: t.rowCount ?? (t.rows ? t.rows.length : null), columnCount: t.fields?.length ?? 0,
      command: t.command ?? null, ...(err ? { error: err } : {}),
    }
    p.metrics = m
    const info = t.info!
    for (let i = 0; i < this.cfg.plugins.length; i++) {
      const pl = this.cfg.plugins[i]!, st = p.states?.[i]
      try { if (err) pl.onQueryError?.(st, info, err, m); else pl.onQueryEnd?.(st, info, m) } catch { /* plugin errors never break the query */ }
    }
    return m
  }

  connect(): Promise<this> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.establish().then(
      () => this,
      (err: Error) => { if (!this.everConnected) { this.state = 'closed'; this.rejectQueue(err) } throw err },
    )
    return this.connectPromise
  }

  // Open one socket, negotiate SSL/auth, and resolve on the first ReadyForQuery.
  // Used by connect() and the reconnect loop.
  private establish(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.attemptResolve = resolve; this.attemptReject = reject
      this.connecting = true; this.state = this.everConnected ? 'reconnecting' : 'connecting'
      this.parser = new Parser(); this.scramState = undefined // fresh per attempt
      this.connTimer = setTimeout(() => this.failAttempt(new Error(`connect timeout after ${this.cfg.connectTimeout}ms`)), this.cfg.connectTimeout)
      // One transport path for all runtimes: config.socket (Deno/CF/WS/unix/custom) or the node:net/tls
      // transport the Node entry registered. The factory returns an already-connected (TLS-negotiated) duplex.
      const ac = new AbortController(); this.connectAbort = ac // failAttempt() aborts -> transport destroys its pending socket
      const factory = this.cfg.socket ?? (defaultTransport ? () => defaultTransport!(this.cfg, ac.signal) : null)
      if (!factory) return this.failAttempt(Object.assign(new Error('no transport: import from minipg/node (net/tls) or pass config.socket'), { fatal: true }))
      Promise.resolve(factory()).then((sock) => {
        if (!this.connecting) { try { sock.destroy() } catch { /* superseded/aborted while connecting */ } return }
        this.socket = sock
        this.attachSocket(sock)
        this.afterTransport()
      }, (e) => this.failAttempt(e as Error))
    })
  }

  // 'close' fires exactly once per socket; 'error' is captured (prevents an uncaught
  // throw) and surfaced via close. Guard on identity so a superseded socket is ignored.
  private attachSocket(sock: Duplex): void {
    let lastErr: Error | undefined
    sock.on('error', (e: Error) => { lastErr = e })
    sock.on('close', () => { if (sock === this.socket) this.onSocketDown(lastErr ?? new Error('connection terminated unexpectedly')) })
  }

  private afterTransport(): void {
    const sock = this.socket!
    sock.on('data', (c: Buffer) => this.onData(c))
    sock.write(W.startup({ user: this.cfg.user, database: this.cfg.database, application_name: this.cfg.applicationName, client_encoding: 'UTF8' }))
  }

  private onData(chunk: Buffer | Uint8Array): void {
    // Web-stream transports (Deno/Cloudflare) deliver Uint8Array; wrap as a Buffer view (no copy).
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
    const cur = this.current
    // always-on live RTT: this query's first response byte -> round-trip = time-to-first-byte (network + the
    // server's time to first byte). One sample per query (guarded), independent of the metrics/telemetry path.
    if (cur && cur._wAt !== undefined && cur._rttDone !== true) { cur._rttDone = true; this.recordRtt(performance.now() - cur._wAt) }
    const p = cur?.perf; if (p) { if (p.tFirst === undefined) p.tFirst = performance.now(); p.recv += buf.length }
    let messages
    try { messages = this.parser.push(buf) } catch (e) { return this.onSocketDown(e as Error) }
    // A throw in a handler (protocol desync) tears down the socket; recoverable per-row
    // decode errors are caught inside dataRow() and never reach here.
    for (const m of messages) {
      try { this.handle(m.type, m.body) } catch (e) { return this.onSocketDown(e as Error) }
    }
  }

  private handle(type: string, body: Buffer): void {
    switch (type) {
      case 'R': return this.auth(body)
      case 'S': { const z = body.indexOf(0); this.serverParams[body.toString('utf8', 0, z)] = body.toString('utf8', z + 1, body.indexOf(0, z + 1)); return }
      case 'K': this.backendKey = { pid: body.readInt32BE(0), secret: body.readInt32BE(4) }; return
      case 'Z': return this.ready(body)
      // CopyIn/CopyBoth: abort instead of hanging. CopyFail ends copy mode (server
      // replies ErrorResponse); the trailing Sync guarantees a ReadyForQuery, since
      // the Sync we already sent was swallowed by copy-in mode.
      case 'G': case 'W': { try { this.socket?.write(Buffer.concat([W.copyFail('COPY is not supported by minipg'), W.sync()])) } catch { /* */ } return }
      case 'T': if (this.current) { this.current.fields = parseRowDescription(body); if (!this.current._typed) { const bin = this.current.binary; try { const cols = this.current.fields.map((f) => (bin ? { name: f.name, oid: f.dataTypeOid, format: 'binary' as const } : { name: f.name, oid: f.dataTypeOid })); this.assignMapper(this.current, cols) } catch (e) { this.current.error = e as Error } /* e.g. { binary: true } on a type with no binary decoder */ } if (this.current._cacheName) this.prepared.set(this.current._cacheName, { sql: this.current.sql, fields: this.current.fields }) } return
      case 'n': if (this.current) { this.current.fields = []; if (this.current._cacheName) this.prepared.set(this.current._cacheName, { sql: this.current.sql, fields: [] }) } return
      case 'D': return this.dataRow(body)
      case 'C': if (this.current) { const tag = firstCstr(body); this.current.command = tag.split(' ')[0]; const m = tag.match(/(\d+)\s*$/); this.current.rowCount = m ? parseInt(m[1]!, 10) : null } return
      case 'E': { const err = new PgError(parseErrorFields(body)); if (this.connecting) return this.failAttempt(err); if (this.current) this.current.error = err; return }
      case 'N': return // NoticeResponse — ignored
      case 'A': return // NotificationResponse — ignored (no LISTEN/NOTIFY)
      default: return // ParseComplete/BindComplete/CloseComplete/ParameterDescription/PortalSuspended
    }
  }

  private auth(body: Buffer): void {
    const code = body.readInt32BE(0)
    const sock = this.socket!
    switch (code) {
      case 0: this.pong(); return // AuthenticationOk — records the md5/cleartext password leg (SCRAM already recorded at 12)
      case 3: this.markPing(); sock.write(W.password(this.cfg.password)); return // cleartext
      case 5: this.markPing(); sock.write(W.password(md5Password(this.cfg.user, this.cfg.password, body.subarray(4, 8)))); return // md5
      case 10: { // SASL
        const mechs = readCstrings(body.subarray(4))
        if (!mechs.includes('SCRAM-SHA-256')) return this.failAttempt(Object.assign(new Error('unsupported SASL mechanisms: ' + mechs.join(', ')), { fatal: true }))
        this.scramState = scram(this.cfg.password)
        this.markPing(); sock.write(W.saslInitial(this.scramState.mechanism, this.scramState.clientFirst)); return
      }
      case 11: this.pong(); this.markPing(); sock.write(W.saslResponse(this.scramState!.continue(body.subarray(4).toString('utf8')))); return // SASLContinue: record client-first→server-first, send client-final
      case 12: { this.pong(); try { this.scramState!.final(body.subarray(4).toString('utf8')) } catch (e) { this.failAttempt(Object.assign(e as Error, { fatal: true })) } return } // SASLFinal: record client-final→server-final
      default: return this.failAttempt(Object.assign(new Error('unsupported authentication request: ' + code), { fatal: true }))
    }
  }

  private ready(body: Buffer): void {
    this.txStatus = body.length ? String.fromCharCode(body[0]!) : 'I' // 'I' idle | 'T' in tx | 'E' failed tx
    if (this.connecting) {
      this.connecting = false; this.everConnected = true; this.state = 'ready'; clearTimeout(this.connTimer)
      this.connectAbort = undefined // connected: the socket is live, don't abort it
      if (this.instrumented) for (const pl of this.cfg.plugins) try { pl.onConnect?.({ database: this.cfg.database, host: this.cfg.host, backendPid: this.backendKey?.pid }) } catch { /* */ }
      const r = this.attemptResolve; this.attemptResolve = this.attemptReject = undefined
      r?.(); this.processQueue() // queued tasks (incl. ones enqueued while reconnecting) now run
    } else {
      this.finishTask()
    }
  }

  // Build (cached) the row mapper for a shape. array/object only; buffer/raw fall through to makeRow.
  // Serves BOTH the standard path (cols from RowDescription fields) and queryTyped (cols from caller).
  // Normalize a shape/typed column plan against connection config before building the mapper AND choosing
  // result formats (callers use this for both, so they agree). temporal:'string' forces date/timestamp(tz)
  // columns with no explicit target to the exact-string TEXT decode — dropping any binary upgrade, since
  // binary can't yield the PG text (lossless for µs/BC/infinity that a JS Date can't represent).
  private resolveCols(cols: CodegenCol[]): CodegenCol[] {
    if (this.cfg.temporal !== 'string') return cols
    return cols.map((c) => (!c.js && !c.json && INSTANT_OIDS.has(c.oid) ? { ...c, js: 'string', format: 'text' } : c))
  }

  // Build + assign the row mapper for a task, capturing the resolved column plan when { debug: true }.
  private assignMapper(t: Task, cols: CodegenCol[]): void {
    t.mapper = this.getMapper(cols, t.mode)
    if (t.debug) t._debugCols = cols
  }

  // Assemble the { debug: true } decode plan from what the task resolved to.
  private buildDebug(t: Task): QueryDebug {
    const src = (t.mapper as (RowMapper & { source?: string }) | undefined)?.source // compileRow attaches .source; interpreted has none
    const cols = t._debugCols ?? (t.fields ?? []).map((f) => ({ name: f.name, oid: f.dataTypeOid } as CodegenCol))
    return {
      sql: t.sql, mode: t.mode, statementName: t.name, reusedPreparedStatement: !!t._reused,
      ...(t._repreparedSqlChanged ? { repreparedSqlChanged: true } : {}),
      decode: t.mapper ? (src ? 'jit' : 'interpreted') : 'none', // buffer/raw have no mapper
      ...(src ? { mapperSource: src } : {}),
      columns: cols.map((c) => ({
        name: c.name, oid: c.oid, format: c.format === 'binary' ? 'binary' : 'text',
        ...(c.js ? { js: c.js } : {}), ...(c.json ? { json: true } : {}),
      })),
      ...(t._retries ? { retries: t._retries, retriedErrors: t._retryErrors } : {}),
    }
  }

  private getMapper(cols: CodegenCol[], mode: ResultMode): RowMapper | undefined {
    if (mode !== 'array' && mode !== 'object') return undefined
    cols = this.resolveCols(cols)
    // NB: encode the whole json marker (its declared shape), not just "has json" — two shapes that differ
    // only inside a Json()/JsonArray() must get different mappers, else the first one is wrongly reused.
    const key = mode + '|' + cols.map((c) => `${c.name}:${c.oid}:${c.format ?? 't'}:${c.js ?? ''}:${c.json ? JSON.stringify(c.json) : ''}`).join(',')
    let m = this.mapperCache.get(key)
    if (!m) { m = this.mapperFactory(cols, mode, this.cfg.decoders); this.mapperCache.set(key, m) }
    return m
  }

  private dataRow(body: Buffer): void {
    const t = this.current
    if (!t || t.cancelled || t.error || t.settled) return
    let row: unknown
    const p = t.perf
    // A decoder (built-in or user-supplied via config.types) can throw; capture it
    // as the task error so the query rejects on ReadyForQuery — never an uncaught crash.
    try {
      if (p) { const s = performance.now(); row = t.mapper ? t.mapper(body) : makeRow(parseDataRow(body), body, t.mode, t.fields ?? [], this.cfg.decoders); p.decodeMs += performance.now() - s }
      else row = t.mapper ? t.mapper(body) : makeRow(parseDataRow(body), body, t.mode, t.fields ?? [], this.cfg.decoders)
    } catch (e) { t.error = e as Error; return }
    if (t.onRow) t.onRow(row); else t.rows.push(row)
  }

  // Dispatch as many queued tasks as the pipeline depth allows, then flush them in one socket write.
  // Depth 1 (pipelining off) makes this a strict one-at-a-time gate — identical to the pre-pipelining
  // behaviour. A serialize failure rejects that task and the loop moves on (no re-entrancy needed).
  private processQueue(): void {
    if (this.state !== 'ready') return
    while (this.queue.length && this.inflight.length < this.cfg.pipelineDepth && this.canStartNext()) {
      this.startTask(this.queue.shift()!)
    }
    // Coalesce writes: in 'microtask' mode, defer the flush so queries issued across SEPARATE query() calls
    // in the same tick (e.g. Promise.all) leave in ONE socket write. Flush synchronously when pipelining is
    // off (depth 1 — nothing to coalesce) or once the batch is already large (bound memory, start the wire
    // transfer). A microtask that fires after a sync flush just finds an empty batch and no-ops.
    if (this.cfg.pipelineFlush === 'microtask' && this.cfg.pipelineDepth > 1 && this.outbuf.mark() < FLUSH_THRESHOLD) this.scheduleFlush()
    else this.flushWrites()
  }

  // Defer one flush to the microtask checkpoint (end of the current synchronous stack, before any I/O), so
  // queries issued back-to-back in this tick batch into a single write. Only one is ever outstanding.
  private scheduleFlush(): void {
    if (this.flushScheduled) return
    this.flushScheduled = true
    queueMicrotask(() => { this.flushScheduled = false; this.flushWrites() })
  }

  // Barrier for stream queries: a stream pauses the socket for backpressure, which would stall every
  // pipelined query behind it, so a stream runs SOLO — nothing else in flight while it runs, and it
  // waits for the pipeline to drain before starting. (FIFO: we never dispatch past a queued stream.)
  private canStartNext(): boolean {
    if (this.inflight.length === 0) return true
    if (this.inflight[0]!.stream) return false // a stream owns the connection until it ends
    return !this.queue[0]!.stream               // don't start a stream while other queries are in flight
  }

  // Flush the accumulated outbound batch as one socket write. The buffer is copied (Node may retain the
  // reference under backpressure) so the reusable Writer can be reset and refilled immediately.
  private flushWrites(): void {
    if (this.outbuf.mark() === 0) return
    const bytes = Buffer.from(this.outbuf.slice())
    this.outbuf.reset()
    this._flushes++
    this.socket?.write(bytes)
  }

  private startTask(t: Task): void {
    // Serialize into the shared outbound batch. Param/SQL encoding can throw (e.g. NUL bytes); we snapshot
    // the batch offset first and rewind to it on a throw, so a bad task leaves earlier batched tasks intact
    // and can't wedge the queue. The task only joins `inflight` once it has valid bytes.
    if (t.perf) t.perf.tStart = performance.now() // execution begins (queue wait ends here)
    const w = this.outbuf
    const mark = w.mark()
    try {
      if (!Array.isArray(t.params)) throw new TypeError('params must be an array')
      const enc = t.params.map(encodeParam)
      const name = t.name ?? ''
      let reuse = false
      if (t.name) {
        const cached = this.prepared.get(t.name)
        if (cached && cached.sql === t.sql) {
          reuse = true; t.fields = cached.fields
          if (t._typed) {
            // shape / queryTyped: the caller already chose per-column formats + mapper in query(); keep them.
          } else if (t.binary) {
            this.assignMapper(t, cached.fields.map((f) => ({ name: f.name, oid: f.dataTypeOid, format: 'binary' as const }))) // { binary: true }: all binary
          } else if (t.mode === 'array' || t.mode === 'object') {
            // Plain query (no shape), 2nd+ execution: the result OIDs are now known from the first roundtrip's
            // RowDescription, so request BINARY for the bench-fast types (same decoded value as text, smaller +
            // faster). The first execution went out text. resolveCols honors temporal:'string'.
            const cols = this.resolveCols(cached.fields.map((f) => (this.cfg.reuseBinaryOids.has(f.dataTypeOid) ? { name: f.name, oid: f.dataTypeOid, format: 'binary' as const } : { name: f.name, oid: f.dataTypeOid })))
            this.assignMapper(t, cols)
            t.resultFormat = cols.map((c) => (c.format === 'binary' ? 1 : 0))
          } else {
            this.assignMapper(t, cached.fields.map((f) => ({ name: f.name, oid: f.dataTypeOid }))) // buffer/raw: text bytes
          }
        }
        else if (cached) { writeClose(w, 'S', t.name); this.prepared.delete(t.name); t._repreparedSqlChanged = true } // same name, different SQL -> deallocate + re-Parse
      }
      if (!reuse) {
        // a name invalidated by DDL still exists server-side (0A000) — deallocate it before re-Parsing, or
        // the Parse errors 42P05 "already exists". Close of an absent statement (26000 case) is a harmless no-op.
        if (t.name && this.staleStatements.has(t.name)) { writeClose(w, 'S', t.name); this.staleStatements.delete(t.name) }
        writeParse(w, name, t.sql); writeDescribe(w, 'S', name); if (t.name) t._cacheName = t.name
      }
      t._reused = reuse; if (t.info) t.info.prepared = reuse
      writeBind(w, '', name, enc, t.resultFormat ?? 0); writeExecute(w, '', 0); writeSync(w)
      if (t.perf) t.perf.sent = w.mark() - mark // bytes this query contributed to the batch
    } catch (e) {
      w.rewind(mark) // discard this task's partial bytes; earlier batched tasks stay intact
      this.rejectTask(t, e as Error)
      return
    }
    this.inflight.push(t)
    if (this.inflight.length > this._maxInflight) this._maxInflight = this.inflight.length
    if (t.timeout != null) t.timer = setTimeout(() => this.cancelTask(t, Object.assign(new Error(`query timed out after ${t.timeout}ms`), { code: 'QUERY_TIMEOUT' })), t.timeout)
    t._wAt = performance.now() // stamp send time for the always-on live RTT sample (reused as tWrite when instrumented)
    if (t.perf) { t.perf.tWrite = t._wAt; this.fireStart(t) } // query now in flight (the batch is flushed by processQueue)
  }

  // Arm an AbortSignal on a task. Returns true if it was already aborted (and settled now).
  private armSignal(task: Task): boolean {
    const sig = task.signal
    if (!sig) return false
    const settle = (r: Error) => { task.settled = true; this.rejectTask(task, r) }
    if (sig.aborted) { settle((sig.reason as Error) ?? Object.assign(new Error('query aborted'), { name: 'AbortError' })); return true }
    const onAbort = () => this.cancelTask(task, (sig.reason as Error) ?? Object.assign(new Error('query aborted'), { name: 'AbortError' }))
    sig.addEventListener('abort', onAbort, { once: true })
    task.signalCleanup = () => sig.removeEventListener('abort', onAbort)
    return false
  }

  // Cancel a task: settle the caller immediately, and if it's in flight, send an
  // out-of-band CancelRequest and keep draining to ReadyForQuery (hard fallback: destroy).
  private cancelTask(task: Task, reason: Error): void {
    if (task.settled) return
    task.settled = true
    if (task.timer) clearTimeout(task.timer)
    const idx = this.inflight.indexOf(task)
    if (idx >= 0) {
      // Already on the wire: settle the caller now; its result still streams back and finishTask discards
      // it (it's settled). An out-of-band CancelRequest cancels whatever the backend is CURRENTLY running —
      // accurate only for the head (idx 0); for a query queued behind the head in the pipeline we just let
      // it run and drop the result (cancelling would hit the wrong query).
      this.fireEnd(task, reason)
      this.rejectTask(task, reason)
      if (idx === 0) {
        this.sendCancelRequest()
        task.graceTimer = setTimeout(() => { try { this.socket?.destroy() } catch { /* */ } }, 5000) // dead-network fallback
      }
    } else {
      const i = this.queue.indexOf(task); if (i >= 0) this.queue.splice(i, 1)
      task.signalCleanup?.()
      this.rejectTask(task, reason)
    }
  }

  // Best-effort out-of-band CancelRequest. The node transport registers nodeCancel; transports without
  // a registered canceller (e.g. cloudflare:sockets) rely on the grace-timer socket.destroy() fallback.
  private sendCancelRequest(): void {
    const key = this.backendKey
    if (!key) return
    defaultCancel?.(this.cfg, key)
  }

  private finishTask(): void {
    const t = this.inflight.shift() ?? null // the head completed (its ReadyForQuery arrived); the next in-flight becomes head
    if (!t) { this.processQueue(); return }
    // A prepared statement invalidated by DDL — 0A000 "cached plan must not change result type" or 26000
    // "prepared statement does not exist" — is dropped from the cache and transparently RE-RUN once. Both
    // are raised before execution (no rows affected), so the retry is side-effect-safe. It Close+re-Parses
    // (staleStatements) and, for a plain query, goes back out TEXT since the new OIDs are unknown again.
    if (t.name && t.error) {
      const code = (t.error as PgError).code
      if (code === '0A000' || code === '26000') {
        this.prepared.delete(t.name); this.staleStatements.add(t.name)
        // NOT for _typed (shape/queryTyped): the caller-declared mapper isn't rebuilt from the fresh
        // RowDescription, so a schema change would silently decode with the stale plan — surface instead.
        // NOT inside a (now-aborted) transaction: the retry would only hit 25P02; surface the real 0A000.
        if (!t.settled && !t.stream && !t._typed && !this.inTransaction && (t._retries ?? 0) < 1) {
          t._retries = (t._retries ?? 0) + 1
            ; (t._retryErrors ??= []).push(code)
          if (t.timer) clearTimeout(t.timer) // old timer cleared here; startTask re-arms a fresh one (else it leaks)
          if (t.graceTimer) clearTimeout(t.graceTimer)
          t.error = undefined; t.rows = []; t.command = undefined; t.rowCount = undefined; t.fields = undefined; t._debugCols = undefined; t._reused = undefined
          if (t.perf) t.perf.tFirst = undefined // measure the retry's ttfb, not the failed attempt's error response
          if (!t.binary) t.resultFormat = undefined // plain query: re-run as text; { binary: true } keeps all-binary
          // Re-run at the BACK of the queue (not the front) — one uniform mechanic for gated AND pipelined
          // transport: under pipelining a front re-run can't preempt queries already in flight anyway, and
          // the failed attempt had no side effects (0A000/26000 fire pre-execution) so the later position is
          // correctness-safe. For the sequential-await case the queue is otherwise empty, so it's a no-op.
          this.queue.push(t); this.processQueue(); return // onQueryStart/End still fire exactly once
        }
      }
    }
    if (t.timer) clearTimeout(t.timer)
    if (t.graceTimer) clearTimeout(t.graceTimer)
    t.signalCleanup?.()
    if (!t.settled) { // a timed-out/aborted task was already settled by the caller
      if (t.error) { if (t.debug) (t.error as PgError & { debug?: QueryDebug }).debug = this.buildDebug(t); this.fireEnd(t, t.error); this.rejectTask(t, t.error) }
      else if (t.stream) { this.fireEnd(t); t.streamEnd?.({ command: t.command, rowCount: t.rowCount }) }
      else {
        const m = this.fireEnd(t)
        const res: QueryResult<never> = { rows: t.rows as never[], columns: (t.fields ?? []).map((f) => f.name), rowCount: t.rowCount ?? null, command: t.command ?? null }
        if (t.wantMetrics && m) res.metrics = m
        if (t.debug) res.debug = this.buildDebug(t)
        t.resolve?.(res)
      }
    }
    this.processQueue()
  }

  // Settle (reject) the in-flight query and everything queued — the "every
  // terminal event settles, no hung promise" invariant. Shared by fatal() and end().
  private settlePending(e: Error): void {
    this.outbuf.reset() // drop any batched-but-unflushed bytes
    // Reject every in-flight query. Prefer an informative server error already received for one (e.g. a
    // FATAL 57P01 admin_shutdown that arrives just before the socket closes); the rest take the given error.
    while (this.inflight.length) { const t = this.inflight.shift()!; if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? e; this.fireEnd(t, reason); this.rejectTask(t, reason) } }
    while (this.queue.length) { const t = this.queue.shift()!; t.signalCleanup?.(); if (!t.settled) { this.rejectTask(t, e) } }
  }

  // reject only the queued (not-yet-sent) tasks; the in-flight one is handled separately
  private rejectQueue(err: Error): void {
    while (this.queue.length) { const t = this.queue.shift()!; this.rejectTask(t, err) }
  }

  // An establish attempt (initial connect or a reconnect try) failed.
  private failAttempt(err: Error): void {
    if (!this.connecting) return
    this.connecting = false; clearTimeout(this.connTimer)
    try { this.connectAbort?.abort() } catch { /* */ } // tear down a pending transport socket (stalled handshake)
    this.connectAbort = undefined
    const dead = this.socket; this.socket = null
    try { dead?.destroy() } catch { /* */ }
    const rj = this.attemptReject; this.attemptResolve = this.attemptReject = undefined
    rj?.(err)
  }

  // A socket died. During an attempt -> fail the attempt. On a live connection ->
  // reject the in-flight query (NEVER replay) and either reconnect or close.
  private onSocketDown(err: Error): void {
    if (this.state === 'closed') return
    if (this.connecting) return this.failAttempt(err)
    const dead = this.socket; this.socket = null // claim the death; further events from `dead` are ignored
    try { dead?.destroy() } catch { /* */ }
    this.outbuf.reset() // drop any batched-but-unflushed bytes
    // Reject every in-flight query (NEVER replay). The head may carry a server error; the rest take `err`.
    while (this.inflight.length) { const t = this.inflight.shift()!; if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? err; this.fireEnd(t, reason); this.rejectTask(t, reason) } }
    if (this.cfg.reconnect.enabled && !this.ended) {
      this.state = 'reconnecting'
      this.prepared.clear(); this.staleStatements.clear() // server-side prepared statements are gone after a drop/restart
      void this.startReconnect(err)
    } else {
      this.state = 'closed'
      this.rejectQueue(err)
    }
  }

  // Single connection's reconnect loop: backoff+jitter until ready, or give up on a fatal error.
  private async startReconnect(lastErr: Error): Promise<void> {
    const { base, max, maxRetries } = this.cfg.reconnect
    for (let attempt = 0; ; attempt++) {
      if (this.ended || this.state === 'closed') return
      if (maxRetries != null && attempt >= maxRetries) { this.state = 'closed'; this.rejectQueue(new Error(`reconnect failed after ${attempt} attempts: ${lastErr.message}`)); return }
      await sleep(Math.min(max, base * 2 ** attempt) * (0.5 + Math.random() * 0.5))
      if (this.ended || (this.state as string) === 'closed') return // end() may have run during the backoff
      if (this.instrumented) for (const pl of this.cfg.plugins) try { pl.onReconnect?.({ attempt, error: lastErr }) } catch { /* */ }
      try { await this.establish(); return } // success -> ready() flips to 'ready' and drains the queue
      catch (e) { lastErr = e as Error; if (isFatalAuth(e)) { this.state = 'closed'; this.rejectQueue(e as Error); return } }
    }
  }

  // ---- public query API ----
  // a shape (without an explicit non-object mode) decodes to objects — matches the runtime default.
  // generic over the shape's column names so editors autocomplete each value to the known type list.
  query<K extends string>(sql: string | readonly string[], params: unknown[], opts: { shape: ShapeOf<K> | ShapeMapper; mode?: 'object'; name?: string; metrics?: boolean | 'ms' | 'us'; debug?: boolean; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Record<string, unknown>>>
  query(sql: string | readonly string[], params?: unknown[], opts?: { name?: string; mode?: 'array'; metrics?: boolean | 'ms' | 'us'; debug?: boolean; timeout?: number; signal?: AbortSignal; trace?: boolean; shape?: ShapeSpec | ShapeMapper; binary?: boolean }): Promise<QueryResult<unknown[]>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'object'; metrics?: boolean | 'ms' | 'us'; debug?: boolean; timeout?: number; signal?: AbortSignal; trace?: boolean; shape?: ShapeSpec | ShapeMapper; binary?: boolean }): Promise<QueryResult<Record<string, unknown>>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'buffer'; metrics?: boolean | 'ms' | 'us'; debug?: boolean; timeout?: number; signal?: AbortSignal; trace?: boolean; binary?: boolean }): Promise<QueryResult<(Buffer | null)[]>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'raw'; metrics?: boolean | 'ms' | 'us'; debug?: boolean; timeout?: number; signal?: AbortSignal; trace?: boolean; binary?: boolean }): Promise<QueryResult<Buffer>>
  query(sql: string | readonly string[], params: unknown[] = [], opts: QueryOptions = {}): Promise<QueryResult<never>> {
    const p = new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      let text: string, name = opts.name
      if (typeof sql === 'string') { text = sql } else {
        // builder chunks: resolve (joined SQL + auto prepared-statement name) by array identity
        let e = this.chunkCache.get(sql)
        if (!e) { e = { name: '_c' + (this.chunkSeq++), sql: buildChunkSql(sql) }; this.chunkCache.set(sql, e) }
        text = e.sql; name = e.name
      }
      if (!this.cfg.prepare) name = undefined // pooler-safe: never a server-side named prepared statement
      const mode = opts.mode ?? (opts.shape ? 'object' : 'array') // a shape implies named columns -> object
      const task: Task = { sql: text, params, name, mode, rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal, debug: opts.debug }
      if (opts.shape) {
        // declared column shape: decode with the SAME cached jit/interpreted mapper as any query, and
        // request binary wire format for any column marked format:'binary'. (mode is array|object here.)
        const cols = this.resolveCols(typeof opts.shape === 'function' ? (opts.shape.$cols as CodegenCol[]) : shapeCols(opts.shape))
        this.assignMapper(task, cols)
        task.resultFormat = cols.map((c) => (c.format === 'binary' ? 1 : 0)) // shapeCols upgraded boost types
        task._typed = true
      } else if (opts.binary) {
        // force BINARY for every column; the mapper is built binary from RowDescription OIDs in the 'T'
        // handler (a scalar resultFormat of 1 tells the server "all columns binary").
        task.binary = true
        task.resultFormat = 1
      }
      this.beginPerf(task, params, opts.metrics)
      if (this.armSignal(task)) return
      this.queue.push(task)
      this.processQueue()
    })
    return opts.trace ? this.retraced(p) : p
  }

  /** Run a query whose result columns are declared upfront (name + wire OID + per-column format/target).
   *  Requests the given wire formats from the server (binary for `format:'binary'` columns) and decodes
   *  with a typed mapper — enabling the binary result format for supported types without a Describe round
   *  trip. `columns` MUST match the SELECT's result columns (count + order). */
  queryTyped(sql: string, params: unknown[], columns: CodegenCol[], opts: { mode?: 'array' | 'object'; metrics?: boolean | 'ms' | 'us'; timeout?: number; signal?: AbortSignal; debug?: boolean; trace?: boolean } = {}): Promise<QueryResult<never>> {
    const p = new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      const mode = opts.mode ?? 'object'
      const cols = this.resolveCols(columns)
      const task: Task = {
        sql, params, mode, rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal, debug: opts.debug,
        mapper: this.getMapper(cols, mode), resultFormat: cols.map((c) => (c.format === 'binary' ? 1 : 0)), _typed: true,
      }
      if (opts.debug) task._debugCols = cols
      this.beginPerf(task, params, opts.metrics)
      if (this.armSignal(task)) return
      this.queue.push(task)
      this.processQueue()
    })
    return opts.trace ? this.retraced(p) : p
  }

  // `trace`: run the query promise through an async wrapper so a rejection is re-thrown as a fresh error born
  // INSIDE the awaited chain — the runtime's async stack traces then splice in the caller's frames (a raw
  // reject from onData/a timer can't carry them). One extra async hop per query; rebuilds the error only on
  // failure. No built-in links an IO-callback rejection to its origin (verified: async stack traces cover only
  // await/then; AsyncLocalStorage can't attribute a shared socket listener; console.createTask is DevTools-only
  // and absent on Bun/Deno), so moving error creation into the await chain is the one portable way.
  private async retraced<T>(p: Promise<T>): Promise<T> {
    try { return await p }
    catch (err) {
      if (!(err instanceof Error))throw err
      const fresh = new Error(err.message) // NEW error -> its stack is captured here, linked to the awaiting caller
      Object.setPrototypeOf(fresh, Object.getPrototypeOf(err) as object) // preserve PgError/AbortError instanceof
      Object.assign(fresh, err) // copy enumerable own props (code, severity, detail, debug, …); message/stack aren't enumerable
      fresh.name = err.name
      const inner = (err.stack ?? '').split('\n').slice(1).join('\n') // keep where it actually raised, as a tail
      if (inner && fresh.stack) fresh.stack += '\n    --- driver internals ---\n' + inner
      throw fresh
    }
  }

  stream<Row = unknown[]>(sql: string, params: unknown[] = [], opts: StreamOptions = {}): AsyncIterableIterator<Row> {
    const self = this
    const buf: Row[] = []
    let waiting: { resolve: (r: IteratorResult<Row>) => void; reject: (e: Error) => void } | null = null
    let done = false, err: Error | null = null, paused = false
    const HWM = opts.highWaterMark ?? 200
    const t: Task = {
      sql, params, name: opts.name, mode: opts.mode ?? 'array', stream: true, rows: [], timeout: opts.timeout, signal: opts.signal,
      onRow(row) {
        buf.push(row as Row)
        if (waiting) { const w = waiting; waiting = null; w.resolve({ value: buf.shift()!, done: false }) }
        else if (buf.length > HWM && self.socket && !paused) { paused = true; self.socket.pause() }
      },
      streamEnd() { done = true; if (waiting) { const w = waiting; waiting = null; w.resolve({ value: undefined, done: true }) } },
      streamError(e) { err = e; if (waiting) { const w = waiting; waiting = null; w.reject(e) } },
    }
    if (this.state === 'closed') err = new Error('connection is closed')
    else if (!this.armSignal(t)) { this.queue.push(t); this.processQueue() }
    return {
      [Symbol.asyncIterator]() { return this },
      next(): Promise<IteratorResult<Row>> {
        if (buf.length) { const v = buf.shift()!; if (paused && buf.length < HWM / 2) { paused = false; self.socket?.resume() } return Promise.resolve({ value: v, done: false }) }
        if (err) return Promise.reject(err)
        if (done) return Promise.resolve({ value: undefined, done: true })
        return new Promise<IteratorResult<Row>>((resolve, reject) => { waiting = { resolve, reject } })
      },
      return(): Promise<IteratorResult<Row>> { t.cancelled = true; if (paused) { paused = false; try { self.socket?.resume() } catch { /* */ } } return Promise.resolve({ value: undefined, done: true }) },
    }
  }

  async end(): Promise<void> {
    if (this.state === 'closed') return
    this.ended = true // stop any reconnect loop
    clearTimeout(this.connTimer)
    const e = new Error('connection ended (end() called)')
    try { this.socket?.write(W.terminate()) } catch { /* */ }
    this.state = 'closed'
    if (this.connecting) { this.connecting = false; const rj = this.attemptReject; this.attemptResolve = this.attemptReject = undefined; rj?.(e) }
    this.settlePending(e) // never leave an in-flight or queued query hanging
    await new Promise<void>((r) => { let fin = false; const done = () => { if (!fin) { fin = true; r() } }; try { this.socket?.end(done) } catch { done() } setTimeout(done, 1000) })
    try { this.socket?.destroy() } catch { /* */ }
  }
}
