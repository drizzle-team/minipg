// A single PostgreSQL connection: transport (net/tls), auth, and the extended
// query protocol. One query in flight at a time (queries queue). No LISTEN/NOTIFY.
import net from 'node:net'
import tls from 'node:tls'
import os from 'node:os'
import type { Duplex } from 'node:stream'
import { W, Writer, writeParse, writeDescribe, writeBind, writeExecute, writeClose, writeSync, Parser, parseRowDescription, parseDataRow } from './protocol.ts'
import { md5Password, scram, type Scram } from './auth.ts'
import { buildDecoders, decoderFor, encodeParam } from './codec.ts'
import { PgError, parseErrorFields } from './errors.ts'
import type { ConnectConfig, Decoder, Field, QueryOptions, QueryResult, ResultMode, StreamOptions } from './types.ts'
import { compileRow, type RowBuilder } from './codegen.ts'
import { compileRow as compileRowTyped, type CodegenCol } from './decode2.ts'

type ConnState = 'idle' | 'connecting' | 'ready' | 'reconnecting' | 'closed'

interface NormalizedConfig {
  host: string; port: number; user: string; password: string; database: string
  ssl: NonNullable<ConnectConfig['ssl']> | false
  applicationName: string; connectTimeout: number; decoders: Map<number, Decoder>
  reconnect: { enabled: boolean; base: number; max: number; maxRetries: number | null }
  socket?: () => Duplex
  path?: string
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))
// Errors that won't fix themselves on retry (bad auth/config) — stop reconnecting.
function isFatalAuth(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code
  if (code === '28P01' || code === '28000' || code === '3D000') return true
  return !!(err as { fatal?: boolean } | null)?.fatal
}

interface Task {
  sql: string
  params: unknown[]
  name?: string | undefined
  mode: ResultMode
  rows: unknown[]
  fields?: Field[]
  builder?: RowBuilder
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
  try { return os.userInfo().username } catch { return process.env.USER || process.env.USERNAME || 'postgres' }
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

  private socket: net.Socket | tls.TLSSocket | Duplex | null = null
  private parser = new Parser()
  private writer = new Writer() // reused per task: one growable buffer, no per-message allocs
  private queue: Task[] = []
  private current: Task | null = null
  private prepared = new Map<string, { sql: string; fields: Field[] }>()
  private rowCache = new Map<string, RowBuilder>()
  private scramState?: Scram

  private connecting = false
  private connTimer?: ReturnType<typeof setTimeout>
  private connectPromise?: Promise<this>
  private attemptResolve?: () => void
  private attemptReject?: (e: Error) => void
  private ended = false
  private everConnected = false

  constructor(config: ConnectConfig = {}) {
    const user = config.user || process.env.PGUSER || defaultUser()
    this.cfg = {
      host: config.host || process.env.PGHOST || 'localhost',
      port: config.port || Number(process.env.PGPORT) || 5432,
      user,
      password: config.password ?? process.env.PGPASSWORD ?? '',
      database: config.database || process.env.PGDATABASE || user,
      ssl: config.ssl ?? false,
      applicationName: config.applicationName || 'minipg',
      connectTimeout: config.connectTimeout ?? 30000,
      decoders: buildDecoders(config.types, config.jsonBigints),
      reconnect: ((rc) => {
        const o = rc && typeof rc === 'object' ? rc : {}
        return { enabled: rc === true || (rc != null && typeof rc === 'object'), base: o.baseMs ?? 100, max: o.maxMs ?? 5000, maxRetries: o.maxRetries ?? null }
      })(config.reconnect),
      socket: config.socket,
      path: config.path,
    }
    // keep the password out of console.log / JSON / inspection of the connection
    Object.defineProperty(this.cfg, 'password', { value: this.cfg.password, enumerable: false, writable: true, configurable: true })
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
      if (this.cfg.socket) { // custom transport: an already-connected duplex (no net.connect / SSL)
        const sock = this.cfg.socket()
        this.socket = sock
        this.attachSocket(sock)
        queueMicrotask(() => this.afterTransport())
        return
      }
      // unix-domain socket (cfg.path) bypasses host/port and SSL; otherwise TCP
      const sock = this.cfg.path ? net.connect({ path: this.cfg.path }) : net.connect({ host: this.cfg.host, port: this.cfg.port })
      this.socket = sock
      this.attachSocket(sock)
      sock.once('connect', () => (this.cfg.ssl && !this.cfg.path ? this.startSSL() : this.afterTransport()))
    })
  }

  // 'close' fires exactly once per socket; 'error' is captured (prevents an uncaught
  // throw) and surfaced via close. Guard on identity so a superseded socket is ignored.
  private attachSocket(sock: net.Socket | tls.TLSSocket | Duplex): void {
    let lastErr: Error | undefined
    sock.on('error', (e: Error) => { lastErr = e })
    sock.on('close', () => { if (sock === this.socket) this.onSocketDown(lastErr ?? new Error('connection terminated unexpectedly')) })
  }

  private startSSL(): void {
    const sock = this.socket as net.Socket
    sock.write(W.sslRequest())
    sock.once('data', (buf: Buffer) => {
      const res = String.fromCharCode(buf[0]!)
      if (res === 'S') {
        const base = { socket: sock, servername: net.isIP(this.cfg.host) ? undefined : this.cfg.host }
        const opts = typeof this.cfg.ssl === 'object' ? { ...base, ...this.cfg.ssl } : { ...base, rejectUnauthorized: false }
        const tlsSock = tls.connect(opts, () => this.afterTransport())
        this.socket = tlsSock
        this.attachSocket(tlsSock)
      } else if (res === 'N') {
        if (this.cfg.ssl === true || this.cfg.ssl === 'require') return this.failAttempt(Object.assign(new Error('server does not support SSL'), { fatal: true }))
        this.afterTransport()
      } else return this.failAttempt(Object.assign(new Error('unexpected SSL response byte: ' + res), { fatal: true }))
    })
  }

  private afterTransport(): void {
    const sock = this.socket!
    sock.on('data', (c: Buffer) => this.onData(c))
    sock.write(W.startup({ user: this.cfg.user, database: this.cfg.database, application_name: this.cfg.applicationName, client_encoding: 'UTF8' }))
  }

  private onData(chunk: Buffer): void {
    let messages
    try { messages = this.parser.push(chunk) } catch (e) { return this.onSocketDown(e as Error) }
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
      case 'T': if (this.current) { this.current.fields = parseRowDescription(body); if (!this.current._typed) this.current.builder = this.builderFor(this.current.fields, this.current.mode); if (this.current._cacheName) this.prepared.set(this.current._cacheName, { sql: this.current.sql, fields: this.current.fields }) } return
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
      const r = this.attemptResolve; this.attemptResolve = this.attemptReject = undefined
      r?.(); this.processQueue() // queued tasks (incl. ones enqueued while reconnecting) now run
    } else {
      this.finishTask()
    }
  }

  private builderFor(fields: Field[], mode: ResultMode): RowBuilder | undefined {
    if (mode !== 'array' && mode !== 'object') return undefined
    const key = mode + '|' + fields.map((f) => f.name + ':' + f.dataTypeOid).join(',')
    let b = this.rowCache.get(key)
    if (!b) { b = compileRow(fields.map((f) => ({ name: f.name, oid: f.dataTypeOid })), mode, this.cfg.decoders); this.rowCache.set(key, b) }
    return b
  }

  private dataRow(body: Buffer): void {
    const t = this.current
    if (!t || t.cancelled || t.error || t.settled) return
    let row: unknown
    // A decoder (built-in or user-supplied via config.types) can throw; capture it
    // as the task error so the query rejects on ReadyForQuery — never an uncaught crash.
    try {
      row = t.builder ? t.builder(body) : makeRow(parseDataRow(body), body, t.mode, t.fields ?? [], this.cfg.decoders)
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
    try {
      if (!Array.isArray(t.params)) throw new TypeError('params must be an array')
      const enc = t.params.map(encodeParam)
      const name = t.name ?? ''
      let reuse = false
      const w = this.writer
      w.reset()
      if (t.name) {
        const cached = this.prepared.get(t.name)
        if (cached && cached.sql === t.sql) { reuse = true; t.fields = cached.fields; t.builder = this.builderFor(cached.fields, t.mode) }
        else if (cached) { writeClose(w, 'S', t.name); this.prepared.delete(t.name) }
      }
      if (!reuse) { writeParse(w, name, t.sql); writeDescribe(w, 'S', name); if (t.name) t._cacheName = t.name }
      writeBind(w, '', name, enc, t.resultFormat ?? 0); writeExecute(w, '', 0); writeSync(w)
      // Safe to hand the reusable slice to write(): one query is in flight at a time,
      // so the buffer isn't reset until ReadyForQuery (i.e. after the bytes flushed).
      payload = w.slice()
    } catch (e) {
      if (t.stream) t.streamError?.(e as Error); else t.reject?.(e as Error)
      queueMicrotask(() => this.processQueue())
      return
    }
    this.current = t
    if (t.timeout != null) t.timer = setTimeout(() => this.cancelTask(t, Object.assign(new Error(`query timed out after ${t.timeout}ms`), { code: 'QUERY_TIMEOUT' })), t.timeout)
    this.socket!.write(payload)
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
      if (task.stream) task.streamError?.(reason); else task.reject?.(reason)
      this.sendCancelRequest()
      task.graceTimer = setTimeout(() => { try { this.socket?.destroy() } catch { /* */ } }, 5000) // dead-network fallback
    } else {
      const i = this.queue.indexOf(task); if (i >= 0) this.queue.splice(i, 1)
      task.signalCleanup?.()
      if (task.stream) task.streamError?.(reason); else task.reject?.(reason)
    }
  }

  // Open a throwaway connection and send CancelRequest for our backend (mirrors SSL).
  private sendCancelRequest(): void {
    const key = this.backendKey
    if (!key) return
    const cancel = W.cancelRequest(key.pid, key.secret)
    const send = (s: net.Socket | tls.TLSSocket) => { try { s.write(cancel); s.end() } catch { /* */ } }
    const plain = net.connect({ host: this.cfg.host, port: this.cfg.port }, () => {
      if (!this.cfg.ssl) return send(plain)
      plain.write(W.sslRequest())
      plain.once('data', (b: Buffer) => {
        if (String.fromCharCode(b[0]!) === 'S') {
          const opts = typeof this.cfg.ssl === 'object' ? { socket: plain, ...this.cfg.ssl } : { socket: plain, rejectUnauthorized: false }
          const t = tls.connect(opts, () => send(t)); t.on('error', () => { /* */ })
        } else send(plain) // server declined SSL; try plaintext cancel
      })
    })
    plain.on('error', () => { /* best-effort */ })
  }

  private finishTask(): void {
    const t = this.current; this.current = null
    if (t) {
      if (t.timer) clearTimeout(t.timer)
      if (t.graceTimer) clearTimeout(t.graceTimer)
      t.signalCleanup?.()
      if (!t.settled) { // a timed-out/aborted task was already settled by the caller
        if (t.error) { if (t.stream) t.streamError?.(t.error); else t.reject?.(t.error) }
        else if (t.stream) t.streamEnd?.({ command: t.command, rowCount: t.rowCount })
        else t.resolve?.({ rows: t.rows as never[], columns: (t.fields ?? []).map((f) => f.name), rowCount: t.rowCount ?? null, command: t.command ?? null })
      }
    }
    this.processQueue()
  }

  // Settle (reject) the in-flight query and everything queued — the "every
  // terminal event settles, no hung promise" invariant. Shared by fatal() and end().
  private settlePending(e: Error): void {
    // Prefer an informative server error already received for the in-flight query
    // (e.g. a FATAL 57P01 admin_shutdown that arrives just before the socket closes).
    if (this.current) { const t = this.current; this.current = null; if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? e; if (t.stream) t.streamError?.(reason); else t.reject?.(reason) } }
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
    if (t) { if (t.timer) clearTimeout(t.timer); if (t.graceTimer) clearTimeout(t.graceTimer); t.signalCleanup?.(); if (!t.settled) { const reason = t.error ?? err; if (t.stream) t.streamError?.(reason); else t.reject?.(reason) } }
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
      try { await this.establish(); return } // success -> ready() flips to 'ready' and drains the queue
      catch (e) { lastErr = e as Error; if (isFatalAuth(e)) { this.state = 'closed'; this.rejectQueue(e as Error); return } }
    }
  }

  // ---- public query API ----
  query(sql: string, params?: unknown[], opts?: { name?: string; mode?: 'array'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<unknown[]>>
  query(sql: string, params: unknown[], opts: { name?: string; mode: 'object'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Record<string, unknown>>>
  query(sql: string, params: unknown[], opts: { name?: string; mode: 'buffer'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<(Buffer | null)[]>>
  query(sql: string, params: unknown[], opts: { name?: string; mode: 'raw'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Buffer>>
  query(sql: string, params: unknown[] = [], opts: QueryOptions = {}): Promise<QueryResult<never>> {
    return new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      const task: Task = { sql, params, name: opts.name, mode: opts.mode ?? 'array', rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal }
      if (this.armSignal(task)) return
      this.queue.push(task)
      this.processQueue()
    })
  }

  // ---- ORM binary flow: columns known upfront ----
  private typedCache = new Map<string, RowBuilder>()
  private typedBuilder(cols: CodegenCol[], mode: 'array' | 'object'): RowBuilder {
    const key = mode + '|' + cols.map((c) => `${c.name}:${c.oid}:${c.format ?? 't'}:${c.js ?? ''}:${c.json ? 'j' : ''}`).join(',')
    let b = this.typedCache.get(key)
    if (!b) { b = compileRowTyped(cols, mode, this.cfg.decoders) as unknown as RowBuilder; this.typedCache.set(key, b) }
    return b
  }

  /** Run a query whose result columns are declared upfront (name + wire OID + per-column format/target).
   *  Requests the given wire formats from the server (binary for `format:'binary'` columns) and decodes
   *  with a typed mapper — enabling the binary result format for supported types without a Describe round
   *  trip. `columns` MUST match the SELECT's result columns (count + order). */
  queryTyped(sql: string, params: unknown[], columns: CodegenCol[], opts: { mode?: 'array' | 'object'; timeout?: number; signal?: AbortSignal } = {}): Promise<QueryResult<never>> {
    return new Promise<QueryResult<never>>((resolve, reject) => {
      if (this.state === 'closed') return reject(new Error('connection is closed'))
      const mode = opts.mode ?? 'object'
      const task: Task = {
        sql, params, mode, rows: [], resolve, reject, timeout: opts.timeout, signal: opts.signal,
        builder: this.typedBuilder(columns, mode), resultFormat: columns.map((c) => (c.format === 'binary' ? 1 : 0)), _typed: true,
      }
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
