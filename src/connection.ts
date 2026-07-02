// A single PostgreSQL connection: transport (net/tls), auth, and the extended
// query protocol. One query in flight at a time (queries queue). No LISTEN/NOTIFY.
import type { Duplex } from 'node:stream'
import { W, Writer, writeParse, writeDescribe, writeBind, writeExecute, writeClose, writeSync, Parser, parseRowDescription, parseDataRow } from './protocol.ts'
import { md5Password, scram, type Scram } from './auth.ts'
import { buildDecoders, decoderFor, encodeParam } from './codec.ts'
import { PgError, parseErrorFields } from './errors.ts'
import type { ConnectConfig, Decoder, Field, QueryOptions, QueryResult, ResultMode, StreamOptions } from './types.ts'
import type { CodegenCol } from './decode2.ts'
import { buildMapperFactory, type RowMapper, type RowMapperFactory } from './mapper.ts'
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
  reconnect: { enabled: boolean; base: number; max: number; maxRetries: number | null }
  socket?: () => Duplex | Promise<Duplex>
  path?: string
  plugins: Plugin[]
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

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
  _instrDone?: boolean // guard so onQueryEnd/Error fires exactly once across the settle paths
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

  private socket: Duplex | null = null
  private parser = new Parser()
  private writer = new Writer() // reused per task: one growable buffer, no per-message allocs
  private queue: Task[] = []
  private current: Task | null = null
  private prepared = new Map<string, { sql: string; fields: Field[] }>()
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
    const user = config.user || process.env.PGUSER || defaultUser()
    const host = config.host || process.env.PGHOST || 'localhost'
    const port = config.port || Number(process.env.PGPORT) || 5432
    this.cfg = {
      host,
      port,
      user,
      password: config.password ?? process.env.PGPASSWORD ?? '',
      database: config.database || process.env.PGDATABASE || user,
      ssl: config.ssl && config.ssl !== 'disable' ? config.ssl : false, // 'disable'/falsy -> no TLS
      applicationName: config.applicationName || 'minipg',
      connectTimeout: config.connectTimeout ?? 30000,
      prepare: config.prepare ?? !transactionPoolerDetected(host, port), // explicit wins; else off behind a pooler
      decoders: buildDecoders(config.types, config.jsonBigints),
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

  // ---- telemetry helpers (no-ops unless instrumented / metrics requested) ----
  private beginPerf(t: Task, params: unknown[], wantMetrics: boolean): void {
    if (!this.instrumented && !wantMetrics) return
    t.wantMetrics = wantMetrics
    t.perf = { t0: performance.now(), decodeMs: 0, sent: 0, recv: 0 }
    t.info = { sql: t.sql, statementName: t.name, paramCount: Array.isArray(params) ? params.length : 0, database: this.cfg.database, host: this.cfg.host, prepared: false }
  }
  private fireStart(t: Task): void {
    const p = t.perf; if (!p) return
    if (t.info) { t.info.backendPid = this.backendKey?.pid; }
    if (this.instrumented) p.states = this.cfg.plugins.map((pl) => pl.onQueryStart?.(t.info!))
  }
  // Compute metrics + fire onQueryEnd/onQueryError exactly once. Returns the metrics (for result attach).
  private fireEnd(t: Task, err?: Error): QueryMetrics | undefined {
    const p = t.perf; if (!p || t._instrDone) return p?.metrics
    t._instrDone = true
    const end = performance.now()
    const start = p.tStart ?? p.t0, wrote = p.tWrite ?? start
    const m: QueryMetrics = {
      queueWaitMs: start - p.t0, writeMs: wrote - start,
      ttfbMs: p.tFirst !== undefined ? p.tFirst - wrote : 0,
      downloadMs: p.tFirst !== undefined ? end - p.tFirst : 0,
      decodeMs: p.decodeMs, totalMs: end - p.t0,
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
    const p = this.current?.perf; if (p) { if (p.tFirst === undefined) p.tFirst = performance.now(); p.recv += buf.length }
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
      case 'T': if (this.current) { this.current.fields = parseRowDescription(body); if (!this.current._typed) this.current.mapper = this.getMapper(this.current.fields.map((f) => ({ name: f.name, oid: f.dataTypeOid })), this.current.mode); if (this.current._cacheName) this.prepared.set(this.current._cacheName, { sql: this.current.sql, fields: this.current.fields }) } return
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
      case 0: return // AuthenticationOk
      case 3: sock.write(W.password(this.cfg.password)); return // cleartext
      case 5: sock.write(W.password(md5Password(this.cfg.user, this.cfg.password, body.subarray(4, 8)))); return // md5
      case 10: { // SASL
        const mechs = readCstrings(body.subarray(4))
        if (!mechs.includes('SCRAM-SHA-256')) return this.failAttempt(Object.assign(new Error('unsupported SASL mechanisms: ' + mechs.join(', ')), { fatal: true }))
        this.scramState = scram(this.cfg.password)
        sock.write(W.saslInitial(this.scramState.mechanism, this.scramState.clientFirst)); return
      }
      case 11: sock.write(W.saslResponse(this.scramState!.continue(body.subarray(4).toString('utf8')))); return // SASLContinue
      case 12: { try { this.scramState!.final(body.subarray(4).toString('utf8')) } catch (e) { this.failAttempt(Object.assign(e as Error, { fatal: true })) } return } // SASLFinal
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
  private getMapper(cols: CodegenCol[], mode: ResultMode): RowMapper | undefined {
    if (mode !== 'array' && mode !== 'object') return undefined
    const key = mode + '|' + cols.map((c) => `${c.name}:${c.oid}:${c.format ?? 't'}:${c.js ?? ''}:${c.json ? 'j' : ''}`).join(',')
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

  private processQueue(): void {
    if (this.state !== 'ready' || this.current || !this.queue.length) return
    this.startTask(this.queue.shift()!)
  }

  private startTask(t: Task): void {
    // Serialize first: param/SQL encoding can throw (e.g. NUL bytes). Only commit
    // `this.current` once we have bytes to write, so a throw can't wedge the queue.
    let payload: Buffer
    if (t.perf) t.perf.tStart = performance.now() // execution begins (queue wait ends here)
    try {
      if (!Array.isArray(t.params)) throw new TypeError('params must be an array')
      const enc = t.params.map(encodeParam)
      const name = t.name ?? ''
      let reuse = false
      const w = this.writer
      w.reset()
      if (t.name) {
        const cached = this.prepared.get(t.name)
        if (cached && cached.sql === t.sql) { reuse = true; t.fields = cached.fields; t.mapper = this.getMapper(cached.fields.map((f) => ({ name: f.name, oid: f.dataTypeOid })), t.mode) }
        else if (cached) { writeClose(w, 'S', t.name); this.prepared.delete(t.name) }
      }
      if (!reuse) { writeParse(w, name, t.sql); writeDescribe(w, 'S', name); if (t.name) t._cacheName = t.name }
      if (t.info) t.info.prepared = reuse
      writeBind(w, '', name, enc, t.resultFormat ?? 0); writeExecute(w, '', 0); writeSync(w)
      // Safe to hand the reusable slice to write(): one query is in flight at a time,
      // so the buffer isn't reset until ReadyForQuery (i.e. after the bytes flushed).
      payload = w.slice()
      if (t.perf) t.perf.sent = payload.length
    } catch (e) {
      if (t.stream) t.streamError?.(e as Error); else t.reject?.(e as Error)
      queueMicrotask(() => this.processQueue())
      return
    }
    this.current = t
    if (t.timeout != null) t.timer = setTimeout(() => this.cancelTask(t, Object.assign(new Error(`query timed out after ${t.timeout}ms`), { code: 'QUERY_TIMEOUT' })), t.timeout)
    this.socket!.write(payload)
    if (t.perf) { t.perf.tWrite = performance.now(); this.fireStart(t) } // query now in flight
  }

  // Arm an AbortSignal on a task. Returns true if it was already aborted (and settled now).
  private armSignal(task: Task): boolean {
    const sig = task.signal
    if (!sig) return false
    const settle = (r: Error) => { task.settled = true; if (task.stream) task.streamError?.(r); else task.reject?.(r) }
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
    if (this.current === task) {
      this.fireEnd(task, reason)
      if (task.stream) task.streamError?.(reason); else task.reject?.(reason)
      this.sendCancelRequest()
      task.graceTimer = setTimeout(() => { try { this.socket?.destroy() } catch { /* */ } }, 5000) // dead-network fallback
    } else {
      const i = this.queue.indexOf(task); if (i >= 0) this.queue.splice(i, 1)
      task.signalCleanup?.()
      if (task.stream) task.streamError?.(reason); else task.reject?.(reason)
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
    const t = this.current; this.current = null
    if (t) {
      if (t.timer) clearTimeout(t.timer)
      if (t.graceTimer) clearTimeout(t.graceTimer)
      t.signalCleanup?.()
      if (!t.settled) { // a timed-out/aborted task was already settled by the caller
        if (t.error) { this.fireEnd(t, t.error); if (t.stream) t.streamError?.(t.error); else t.reject?.(t.error) }
        else if (t.stream) { this.fireEnd(t); t.streamEnd?.({ command: t.command, rowCount: t.rowCount }) }
        else {
          const m = this.fireEnd(t)
          const res: QueryResult<never> = { rows: t.rows as never[], columns: (t.fields ?? []).map((f) => f.name), rowCount: t.rowCount ?? null, command: t.command ?? null }
          if (t.wantMetrics && m) res.metrics = m
          t.resolve?.(res)
        }
      }
    }
    this.processQueue()
  }

  // Settle (reject) the in-flight query and everything queued — the "every
  // terminal event settles, no hung promise" invariant. Shared by fatal() and end().
  private settlePending(e: Error): void {
    // Prefer an informative server error already received for the in-flight query
    // (e.g. a FATAL 57P01 admin_shutdown that arrives just before the socket closes).
    if (this.current) { const t = this.current; this.current = null; if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? e; this.fireEnd(t, reason); if (t.stream) t.streamError?.(reason); else t.reject?.(reason) } }
    while (this.queue.length) { const t = this.queue.shift()!; t.signalCleanup?.(); if (!t.settled) { if (t.stream) t.streamError?.(e); else t.reject?.(e) } }
  }

  // reject only the queued (not-yet-sent) tasks; the in-flight one is handled separately
  private rejectQueue(err: Error): void {
    while (this.queue.length) { const t = this.queue.shift()!; if (t.stream) t.streamError?.(err); else t.reject?.(err) }
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
    const t = this.current; this.current = null
    if (t) { if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? err; this.fireEnd(t, reason); if (t.stream) t.streamError?.(reason); else t.reject?.(reason) } }
    if (this.cfg.reconnect.enabled && !this.ended) {
      this.state = 'reconnecting'
      this.prepared.clear() // server-side prepared statements are gone after a drop/restart
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
  query(sql: string | readonly string[], params?: unknown[], opts?: { name?: string; mode?: 'array'; metrics?: boolean; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<unknown[]>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'object'; metrics?: boolean; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Record<string, unknown>>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'buffer'; metrics?: boolean; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<(Buffer | null)[]>>
  query(sql: string | readonly string[], params: unknown[], opts: { name?: string; mode: 'raw'; metrics?: boolean; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Buffer>>
  query(sql: string | readonly string[], params: unknown[] = [], opts: QueryOptions = {}): Promise<QueryResult<never>> {
    return new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      let text: string, name = opts.name
      if (typeof sql === 'string') { text = sql } else {
        // builder chunks: resolve (joined SQL + auto prepared-statement name) by array identity
        let e = this.chunkCache.get(sql)
        if (!e) { e = { name: '_c' + (this.chunkSeq++), sql: buildChunkSql(sql) }; this.chunkCache.set(sql, e) }
        text = e.sql; name = e.name
      }
      if (!this.cfg.prepare) name = undefined // pooler-safe: never a server-side named prepared statement
      const task: Task = { sql: text, params, name, mode: opts.mode ?? 'array', rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal }
      this.beginPerf(task, params, !!opts.metrics)
      if (this.armSignal(task)) return
      this.queue.push(task)
      this.processQueue()
    })
  }

  /** Run a query whose result columns are declared upfront (name + wire OID + per-column format/target).
   *  Requests the given wire formats from the server (binary for `format:'binary'` columns) and decodes
   *  with a typed mapper — enabling the binary result format for supported types without a Describe round
   *  trip. `columns` MUST match the SELECT's result columns (count + order). */
  queryTyped(sql: string, params: unknown[], columns: CodegenCol[], opts: { mode?: 'array' | 'object'; metrics?: boolean; timeout?: number; signal?: AbortSignal } = {}): Promise<QueryResult<never>> {
    return new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      const mode = opts.mode ?? 'object'
      const task: Task = {
        sql, params, mode, rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal,
        mapper: this.getMapper(columns, mode), resultFormat: columns.map((c) => (c.format === 'binary' ? 1 : 0)), _typed: true,
      }
      this.beginPerf(task, params, !!opts.metrics)
      if (this.armSignal(task)) return
      this.queue.push(task)
      this.processQueue()
    })
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
