// Logical replication client (`replication()`): a walsender connection speaking the
// replication grammar over the simple protocol + CopyBoth pgoutput streaming. Validated
// end-to-end by playground/replication/ before this port. Reuses the driver's transports
// (node net/tls or config.socket), full auth (SCRAM/md5/cleartext), and the decoder catalog —
// tuple values decode exactly like plain text-format query cells.
//
//   const repl = await replication({ host, port, user, password, database })
//   const slot = await repl.createSlot('pulse', { temporary: true, snapshot: 'export' })
//   // gapless backfill: pin the snapshot with a cursor BEFORE the next command here —
//   //   const cur = pool.cursor({ sql: 'select * from t', snapshot: slot.snapshot, fullScan: true }); await cur.open()
//   for await (const e of repl.start({ slot: slot.slot, publications: ['pub'] })) { … repl.ack(e.endLsn) }
import type { Duplex } from 'node:stream'
import { W, Parser, parseRowDescription, parseDataRow, type RawMessage } from './protocol.ts'
import { md5Password, scramForChannel, parseSaslMechanisms, peerCertDer, type Scram } from './auth.ts'
import { PgError, parseErrorFields } from './errors.ts'
import { getDefaultTransport, type NormalizedConfig } from './connection.ts'
import { resolveUrl } from './url.ts'
import { buildDecoders } from './decode.ts'
import { pickDecoder, replBinaryFor, replBinaryForCol, type CellDecoder, type CodegenCol } from './decode.ts'
import { shapeCols, type TypeSpec } from './spec.ts'
import type { JsonMarker, TransformMarker, SpecEntries } from './json.ts'
import type { CustomMarker } from './registry.ts'
import type { ConnectConfig, Decoder } from './types.ts'

const PG_EPOCH_US = 946684800000000n // 2000-01-01T00:00:00Z in µs

// ---- LSNs: strings ('16/B374D848') publicly, bigint internally ----
export const lsnToString = (lsn: bigint): string => `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xffffffffn).toString(16).toUpperCase()}`
export const lsnFromString = (s: string): bigint => { const [hi, lo] = s.split('/'); return (BigInt(parseInt(hi!, 16)) << 32n) | BigInt(parseInt(lo!, 16)) }
const toLsn = (v: string | bigint): bigint => (typeof v === 'bigint' ? v : lsnFromString(v))

// ---- event model ----
export interface ReplicationRelation { schema: string; table: string; replicaIdentity: 'd' | 'n' | 'f' | 'i'; columns: { name: string; oid: number; key: boolean }[] }
export type Row = Record<string, unknown>
// LSN taxonomy: begin.finalLsn === commit.lsn (the commit RECORD's own position) < commit.endLsn
// (the first position AFTER the record). ACK endLsn — see ack().
export type ReplicationEvent =
  | {
      kind: 'begin'; xid: number; commitTime: Date
      /** Same instant as commitTime, as epoch microseconds — carries pgoutput's full precision where Date floors to milliseconds. */
      commitTimeUs: number
      /** The tx's commit-record LSN — identical to the matching commit event's `lsn`. */
      finalLsn: string
    }
  | {
      kind: 'commit'
      /** The commit record's OWN position (=== begin.finalLsn). Not the ack target. */
      lsn: string
      /** First LSN AFTER the commit record — `repl.ack(e.endLsn)` acknowledges this tx.
       *  (ack() normalizes a value inside [lsn, endLsn) of the LAST delivered commit, so acking
       *  `lsn` by mistake can't silently gate the idle-keepalive advance.) */
      endLsn: string
      commitTime: Date
      /** Same instant as commitTime, as epoch microseconds — carries pgoutput's full precision where Date floors to milliseconds. */
      commitTimeUs: number
    }
  | { kind: 'insert'; schema: string; table: string; new: Row }
  // update's old tuple is discriminated on oldKind: 'key'/'full' ALWAYS carries a Row (REPLICA
  // IDENTITY key columns / full old row), null means no old tuple was on the wire (IDENTITY DEFAULT).
  // `unchanged`: unmodified TOASTed columns pgoutput did not resend. With REPLICA IDENTITY FULL the
  // driver fills them into `new` from the old tuple (and still lists them here); otherwise they are
  // ABSENT from `new` — not null (null would collide with SQL NULL) — and genuinely unrecoverable.
  | ({ kind: 'update'; schema: string; table: string; new: Row; unchanged: string[] }
      & ({ oldKind: 'key' | 'full'; old: Row } | { oldKind: null; old: null }))
  | { kind: 'delete'; schema: string; table: string; old: Row; oldKind: 'key' | 'full' }
  | { kind: 'truncate'; tables: { schema: string; table: string }[]; cascade: boolean; restartIdentity: boolean }
  | { kind: 'message'; transactional: boolean; prefix: string; content: Buffer; lsn: string }
  | { kind: 'relation'; relation: ReplicationRelation } // schema (re-)announced — fires again on DDL changes

export type ReplicationConfig = Pick<ConnectConfig,
  'url' | 'host' | 'port' | 'user' | 'password' | 'database' | 'ssl' | 'path' | 'socket' | 'applicationName' | 'connectTimeout' | 'types' | 'jsonBigints' | 'options' | 'channelBinding'> & {
  /** Enable TCP keepalive (SO_KEEPALIVE) on the underlying socket — a liveness signal independent
   *  of receiveTimeoutMs. `true` uses the OS default initial delay; the object form sets it
   *  explicitly. Applied as an explicit setKeepAlive() call after connect rather than a
   *  tls.connect() option, because tls.connect() silently drops keepalive options
   *  (nodejs/node#62003). A no-op on unix-socket (`path`) connections and on custom `socket`
   *  transports that don't expose setKeepAlive (web-stream/CF/Deno) — TCP keepalive is a
   *  property of the TCP socket, which those transports don't have one of. */
  keepAlive?: boolean | { initialDelayMs?: number }
}

/** One table's declared decode shape for `start({ shapes })`. An ARRAY of these (not a keyed
 *  object) so schema/table names containing dots or quotes never need escaping. */
export interface TableShape {
  /** Table schema; default 'public'. */
  schema?: string
  table: string
  /** shape key -> SQL column name, for keys that differ from the column (e.g. drizzle JS
   *  property names: `{ bigIntCol: 'big_int_col' }`). Keys absent here read the column named
   *  like the key. Every entry's key must exist in `shape` (a typo throws). */
  columns?: Record<string, string>
  /** OUTPUT key -> spec: the SAME grammar (and the same key semantics) as query()'s { shape } —
   *  TypeSpec strings ('int8:number', 'numeric:bigint', 'int8[]:number', …), Json()/Jsonb()
   *  markers, Transform(), and defineType()/minipg-geometry markers; object literal or ordered
   *  [key, spec] entries. Collect() groups several result columns and can't apply to a
   *  replication row — it throws at start(). */
  shape: Record<string, TypeSpec | JsonMarker | TransformMarker | CustomMarker> | SpecEntries<TypeSpec | JsonMarker | TransformMarker | CustomMarker>
}

export interface StartOptions {
  slot: string
  /** Publications to subscribe. A missing publication rejects with PublicationMissing before
   *  START_REPLICATION is written (a driver-side catalog probe — pgoutput itself only reports a
   *  missing publication lazily, and PG18 no longer reports it to the client at all). The probe
   *  only answers "does it exist NOW": a publication created after the slot can still be invisible
   *  to the slot's historical catalog snapshot even once this check passes. */
  publications: string[]
  /** Per-table decode shapes, matched by (schema, table) when the relation is announced.
   *  Declared columns decode exactly like the same spec in a query() shape, and — like a
   *  query() shape — the row is keyed by the SHAPE keys (`columns` maps a key to its SQL
   *  column when they differ; `unchanged` uses the same keys — those columns were NOT resent
   *  (unmodified TOAST): absent from `new` unless REPLICA IDENTITY FULL let the driver fill
   *  them from `old`). Undeclared columns and
   *  unlisted tables keep the default catalog decoding (int8 -> BigInt, temporal -> Date)
   *  under their SQL names. 'unknown' defers a column to its live relation oid. NEVER
   *  silent: a declared column the live relation doesn't have, two keys mapping to one
   *  column, and an output-key collision all throw (also on a mid-stream DDL re-announce).
   *  Applies to new AND old (REPLICA IDENTITY) tuples. */
  shapes?: TableShape[]
  /** LSN to start from; default = the server resumes from the slot's confirmed position. */
  from?: string | bigint
  /** Deliver pg_logical_emit_message() events (default true). */
  messages?: boolean
  /** Standby-status heartbeat interval, ms (default 10_000). */
  statusIntervalMs?: number
  /** Fail the stream if no message (including keepalives) arrives for this many ms — a
   *  black-holed TCP connection otherwise parks forever. Off by default; undefined, zero, and
   *  negative all disarm it. The server's wal_sender_timeout (default 60s) pings the client at
   *  half that interval once it has gone quiet, but the client's OWN status updates
   *  (statusIntervalMs) postpone that ping, so there is no fixed cadence to rely on — a value at
   *  or below wal_sender_timeout can false-fire on a genuinely healthy, fully idle stream. Size
   *  it comfortably above wal_sender_timeout. */
  receiveTimeoutMs?: number
  /** Ceiling on buffered-but-undelivered message bytes before the socket is paused (resumed once
   *  the queue drains below half) — bounds memory when the consumer stops reading. Default 64 MiB;
   *  `Infinity` disables the ceiling; zero, negative, and non-finite values fall back to the
   *  default, so a misconfigured value can never leave the socket paused forever. Counts each
   *  message's body length only — the parser hands out those bodies as views into the socket's
   *  accumulated read buffer, so one retained message pins its whole source chunk and real
   *  retention runs higher; the ceiling exists to stop unbounded growth, not measure it exactly. */
  maxQueueBytes?: number
  /** BINARY tuple values (PG14+ pgoutput option). Default 'auto': enabled when the server is 14+
   *  AND the driver can binary-decode every published column and every shaped target (one
   *  pre-start catalog probe — a capability check, nothing more). Under binary, arrays decode to
   *  REAL JS arrays and float4 to the exact stored f32 (text mode: raw '{…}' literals / the
   *  canonical shortest number). Anything binary can't honor — a type with no binary decoder
   *  (interval, extension types) or a text-only shape target (':string' temporals, bare float4,
   *  defineType()/geometry markers) — simply keeps the whole stream on text: auto never crashes.
   *  `true` forces binary; an undecodable column then errors loudly naming table.column.
   *  `false` = text. Caveat under 'auto'/'true': DDL AFTER the stream starts isn't re-probed — a
   *  new column of an undecodable type errors when it is announced (the Relation message), not
   *  deferred to its first value. */
  binary?: boolean | 'auto'
  /** When idle with nothing unacked, advance the flushed LSN to the server's keepalive
   *  position so an idle slot doesn't retain WAL forever (default true). */
  idleAck?: boolean
  /** Abort = the consumer's own stop: the iterator finishes CLEANLY (no throw) and the
   *  connection closes — same as calling end() while parked in next(). */
  signal?: AbortSignal
  /** Called ONCE when the server accepts START_REPLICATION (CopyBothResponse) — the moment the slot
   *  shows active in pg_stat_activity. start() is lazy (nothing runs before the first next()), so
   *  this is the observable "streaming established" hook; a slot-already-active PgError fires before
   *  it, and a throw from the callback fails the stream's current next(). */
  onReady?: () => void
  /** The library's only warning channel — non-fatal conditions detected at start() (an empty
   *  publication, for example) that are worth surfacing but aren't errors. Called synchronously,
   *  zero or more times, before the stream begins. Absent = the warning is simply not delivered;
   *  minipg never writes warnings to the process streams itself. Later diagnostics reuse this
   *  same field with new ReplicationWarning `kind` members rather than adding a second channel. */
  onWarning?: (w: ReplicationWarning) => void
}

/** The SERVER ended the replication stream (CopyDone): clean primary shutdown, failover, or a
 *  pooler closing the copy. The consumer must react (re-start() or reconnect) — the slot keeps
 *  retaining WAL either way — so this is a THROW, not a clean return: `for await` discards
 *  generator return values, and a clean end must stay reserved for the consumer's own break /
 *  end()/signal. The connection has finished the CopyDone handshake and is usable for a
 *  follow-up start(). */
export class ReplicationStreamEnded extends Error {
  readonly reason = 'copy-done' as const
  constructor() { super('minipg: server ended the replication stream (CopyDone) — start() again or reconnect to resume') }
}

/** No message — not even a keepalive — arrived for `receiveTimeoutMs`: the connection is presumed
 *  dead. The slot keeps retaining WAL, so nothing is lost; reconnect and start() again. If this
 *  fires on a healthy link, raise receiveTimeoutMs relative to the server's wal_sender_timeout. */
export class ReplicationReceiveTimeout extends Error {
  readonly reason = 'receive-timeout' as const
  constructor(readonly ms: number) { super(`minipg: no message received for ${ms}ms (receiveTimeoutMs) — the connection is presumed dead; reconnect, or raise receiveTimeoutMs relative to the server's wal_sender_timeout`) }
}

// Postgres restricts slot names to [a-z0-9_]{1,63} (ReplicationSlotValidateName) so the name can
// double as a directory name on every supported OS. Rejecting beats escaping — there is no escape
// form the walsender grammar accepts for a name outside that set.
const SLOT_NAME = /^[a-z0-9_]{1,63}$/

/** A replication slot name outside Postgres's allowed set ([a-z0-9_], 1-63 chars) — rejected
 *  before any CREATE_REPLICATION_SLOT / DROP_REPLICATION_SLOT / START_REPLICATION command is sent. */
export class InvalidSlotName extends Error {
  readonly reason = 'invalid-slot-name' as const
  constructor(readonly slot: string) { super(`minipg: invalid replication slot name ${JSON.stringify(slot)} — Postgres allows only lower-case letters, digits and underscore, 1 to 63 characters`) }
}
const checkSlot = (s: string): string => { if (!SLOT_NAME.test(s)) throw new InvalidSlotName(s); return s }

/** command() or a second start() was called while a start() stream is active. Both would drain the
 *  same message queue, so either one would eat CopyData frames the live stream is waiting on.
 *  Finish the stream (break/return the iterator) or end() the connection, then retry. */
export class ReplicationBusy extends Error {
  readonly reason = 'streaming' as const
  constructor() { super('minipg: a start() stream is already active on this connection — finish the stream (break/return the iterator) or end() the connection before another start() or command()') }
}

/** A named publication in start()'s publications list does not exist right now — caught by an
 *  unconditional driver-side catalog probe before START_REPLICATION is sent, because pgoutput
 *  itself only reports this lazily (from the change callback at the first decoded change,
 *  PG14-17) or not at all (PG18 downgrades it to a WARNING the client never sees). See
 *  StartOptions.publications for the historical-snapshot case this probe cannot cover. */
export class PublicationMissing extends Error {
  readonly reason = 'publication-missing' as const
  constructor(readonly publications: string[]) {
    super(`minipg: publication ${publications.map((p) => JSON.stringify(p)).join(', ')} does not exist — create it before start(), or remove it from publications`)
  }
}

/** A non-fatal condition detected at start() — the library's only warning channel (see
 *  StartOptions.onWarning). A union of one member today; later phases add new `kind` values
 *  without reshaping this field. */
export type ReplicationWarning = { kind: 'publication-empty'; publication: string; message: string }

/** Open a logical-replication connection (walsender). One purpose per connection: this cannot
 *  run extended-protocol queries — use a normal connect() alongside it. */
export async function replication(config: string | ReplicationConfig = {}): Promise<ReplicationConnection> {
  const c = new ReplicationConnection(typeof config === 'string' ? { url: config } : config)
  await c.connect()
  return c
}

interface RelEntry { info: ReplicationRelation; names: string[]; decoders: CellDecoder[]; bin: (CellDecoder | null)[] | null } // names[i] = row OUTPUT key for column i (shape key when shaped, else the SQL name)

export class ReplicationConnection {
  private cfg: NormalizedConfig
  private decoders: Map<number, Decoder>
  private socket: Duplex | null = null
  private parser = new Parser()
  private q: RawMessage[] = []
  private wake: (() => void) | null = null
  private err: Error | null = null
  private ended = false
  private scramState?: Scram
  private relations = new Map<number, RelEntry>()
  private shaped = new Map<string, { cols: CodegenCol[]; columns?: Record<string, string> }>() // '<schema>\0<table>' -> resolved shape for the active start() stream
  private lastReceived = 0n
  private flushed = 0n
  private lastDeliveredEnd = 0n   // highest commit endLsn handed to the consumer (guards idleAck)
  private lastDeliveredStart = 0n // that same commit's lsn — ack() normalizes values inside [start, end)
  private binaryMode = false    // resolved binary decision for the active start() stream
  private serverMajor = 0       // from the server_version ParameterStatus at startup
  private lastMessageAt = 0     // stamped on every raw socket read — the receiveTimeoutMs liveness clock
  private qPaused = false       // set by maxQueueBytes backpressure; a paused socket has no liveness signal to offer
  private qBytes = 0            // buffered-but-undelivered message bytes — the maxQueueBytes ceiling
  private maxQueue = Infinity   // resolved from opts.maxQueueBytes at the top of start()
  private streaming = false     // true from just before START_REPLICATION is sent until the stream ends — guards command()
  private copyOpen = false      // true once CopyBothResponse is accepted; a consumer that stops iterating without
                                 // reaching a server CopyDone leaves this true — command() exits copy mode first
  private keepAlive?: boolean | { initialDelayMs?: number }

  constructor(config: ReplicationConfig = {}) {
    const rc = resolveUrl(config)
    if (rc.channelBinding === 'require') { // satisfiable ONLY on the node TLS transport
      if (!rc.ssl || rc.ssl === 'disable') throw new Error('minipg: channel_binding=require needs TLS — the binding is a property of the TLS channel; enable ssl or use channel_binding=prefer')
      if (rc.socket) throw new Error('minipg: channel_binding=require — a custom socket transport cannot expose the server certificate (node TLS only); use channel_binding=prefer')
    }
    const user = rc.user || process.env.PGUSER || process.env.USER || process.env.USERNAME || 'postgres'
    this.decoders = buildDecoders(rc.types, rc.jsonBigints)
    this.keepAlive = rc.keepAlive
    this.cfg = {
      host: rc.host || process.env.PGHOST || 'localhost',
      port: rc.port || Number(process.env.PGPORT) || 5432,
      user,
      password: rc.password ?? process.env.PGPASSWORD ?? '',
      database: rc.database || process.env.PGDATABASE || user,
      ssl: rc.ssl && rc.ssl !== 'disable' ? rc.ssl : false,
      applicationName: rc.applicationName || 'minipg-replication',
      connectTimeout: rc.connectTimeout ?? 30000,
      // startup-packet GUCs — e.g. '-c wal_sender_timeout=10s' bounds a stranded walsender when the
      // consumer dies without end() (serverless eviction); NUL-checked here so it throws at construction
      options: ((o) => { if (o?.includes('\0')) throw new Error('minipg: startup parameter options contains NUL byte (0x00)'); return o })(rc.options),
      channelBinding: rc.channelBinding,
      path: rc.path,
      socket: rc.socket,
      // unused by the transport, present to satisfy NormalizedConfig:
      decoders: this.decoders, prepare: false, binaryParams: false, pipelineDepth: 1, pipelineFlush: 'sync',
      temporal: 'date', reuseBinaryOids: new Set(), reconnect: { enabled: false, base: 0, max: 0, maxRetries: 0 }, plugins: [],
    }
  }

  /** Current stream position (highest LSN seen) and acknowledged position, as 'X/Y' strings. */
  get lastReceivedLsn(): string { return lsnToString(this.lastReceived) }
  get flushedLsn(): string { return lsnToString(this.flushed) }

  async connect(): Promise<void> {
    const transport = this.cfg.socket ? null : getDefaultTransport()
    if (!this.cfg.socket && !transport) throw new Error('minipg: no transport — import minipg (node) or pass config.socket')
    const ac = new AbortController()
    const timer = setTimeout(() => { ac.abort(); this.fail(new Error(`replication connect timed out after ${this.cfg.connectTimeout}ms`)) }, this.cfg.connectTimeout)
    try {
      this.socket = this.cfg.socket ? (await this.cfg.socket()) as unknown as Duplex : await transport!(this.cfg, ac.signal)
      if (this.keepAlive) {
        const delay = typeof this.keepAlive === 'object' ? this.keepAlive.initialDelayMs ?? 0 : 0
        try { (this.socket as unknown as { setKeepAlive?: (e: boolean, d?: number) => unknown }).setKeepAlive?.(true, delay) } catch { /* */ }
      }
      this.socket.on('data', (d: Buffer) => this.onData(d))
      this.socket.on('error', (e: Error) => this.fail(e))
      this.socket.on('close', () => { if (!this.ended) this.fail(this.err ?? new Error('replication connection closed unexpectedly')) })
      // THE one special startup parameter: replication=database selects the logical walsender
      this.socket.write(W.startup({ user: this.cfg.user, database: this.cfg.database, application_name: this.cfg.applicationName, replication: 'database', options: this.cfg.options }))
      for (;;) {
        const m = await this.next()
        if (!m) throw new Error('minipg: replication connection ended during startup')
        if (m.type === 'R') { this.auth(m.body); continue }
        if (m.type === 'E') throw new PgError(parseErrorFields(m.body))
        if (m.type === 'Z') break // ParameterStatus/BackendKeyData skipped by next()
      }
    } finally { clearTimeout(timer) }
  }

  private auth(body: Buffer): void {
    const code = body.readInt32BE(0)
    const sock = this.socket!
    switch (code) {
      case 0: return
      case 3: sock.write(W.password(this.cfg.password)); return
      case 5: sock.write(W.password(md5Password(this.cfg.user, this.cfg.password, body.subarray(4, 8)))); return
      case 10: { // SASL: channel-bind when the TLS transport exposes the cert and the server offers -PLUS
        this.scramState = scramForChannel(this.cfg.password, parseSaslMechanisms(body), this.cfg.channelBinding ?? 'prefer', peerCertDer(this.socket))
        sock.write(W.saslInitial(this.scramState.mechanism, this.scramState.clientFirst)); return
      }
      case 11: sock.write(W.saslResponse(this.scramState!.continue(body.subarray(4).toString('utf8')))); return
      case 12: this.scramState!.final(body.subarray(4).toString('utf8')); return
      default: throw new Error('unsupported authentication request: ' + code)
    }
  }

  private onData(d: Buffer): void {
    this.lastMessageAt = Date.now() // raw bytes prove liveness even when no full message parsed yet
    let msgs: RawMessage[]
    try { msgs = this.parser.push(Buffer.from(d)) } catch (e) { this.fail(e as Error); return }
    if (msgs.length) {
      this.q.push(...msgs)
      for (const m of msgs) this.qBytes += m.body.length + 5 // +5 = the type byte + int32 length the parser strips
      if (this.qBytes > this.maxQueue && !this.qPaused) {
        this.qPaused = true
        try { (this.socket as unknown as { pause?: () => void } | null)?.pause?.() } catch { /* */ }
      }
    }
    const w = this.wake; this.wake = null; w?.()
  }
  private fail(e: Error): void { this.err ??= e; const w = this.wake; this.wake = null; w?.() }
  private forceResume(): void {
    if (!this.qPaused) return
    this.qPaused = false
    try { (this.socket as unknown as { resume?: () => void } | null)?.resume?.() } catch { /* */ }
  }
  private param(b: Buffer): void { // ParameterStatus: name\0value\0 — we only care about the version
    let z = 0; while (b[z] !== 0) z++
    if (b.toString('latin1', 0, z) !== 'server_version') return
    let e = z + 1; while (b[e] !== 0) e++
    this.serverMajor = parseInt(b.toString('latin1', z + 1, e), 10) || 0
  }
  /** Next protocol message; null once end() was called and the queue is drained — so a parked
   *  consumer finishes DETERMINISTICALLY instead of waiting for the keepalive timer to trip
   *  over the dead socket. */
  private async next(): Promise<RawMessage | null> {
    for (;;) {
      const m = this.q.shift()
      if (m) {
        this.qBytes -= m.body.length + 5
        if (this.qPaused && this.qBytes < this.maxQueue / 2) this.forceResume()
        if (m.type === 'S') { this.param(m.body); continue } if (m.type === 'K' || m.type === 'N' || m.type === 'A') continue; return m
      }
      if (this.err) throw this.err
      if (this.ended) return null
      await new Promise<void>((r) => { this.wake = r })
    }
  }
  private frame(type: string, payload: Buffer): void {
    const head = Buffer.allocUnsafe(5)
    head.write(type, 0, 'latin1'); head.writeInt32BE(payload.length + 4, 1)
    this.socket?.write(Buffer.concat([head, payload]))
  }

  // A stream a consumer stopped reading (break/return()) without a server CopyDone leaves the
  // WALSENDER thinking copy mode is still active — a plain 'Q' into that state desyncs the
  // protocol and the server drops the connection. Exchange the client-side CopyDone lazily,
  // right before the next command needs the connection back in simple-query mode; ack()-after-
  // break never touches this (it only ever writes standby-status CopyData, which stays valid
  // regardless), so that flow keeps working unpaused.
  private async exitCopyModeIfOpen(): Promise<void> {
    if (!this.copyOpen) return
    this.copyOpen = false
    try {
      this.frame('c', Buffer.alloc(0))
      for (;;) { const n = await this.next(); if (!n || n.type === 'Z') break }
    } catch { /* connection unusable — the command below surfaces the failure */ }
  }

  /** Simple-protocol command: the walsender grammar (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, …)
   *  or plain SQL — the logical walsender accepts both. Rows come back as text. */
  async command(sql: string): Promise<{ columns: string[]; rows: (string | null)[][] }> {
    if (this.streaming) throw new ReplicationBusy()
    await this.exitCopyModeIfOpen()
    this.frame('Q', Buffer.from(sql + '\0', 'utf8'))
    let columns: string[] = []
    const rows: (string | null)[][] = []
    let err: Error | null = null
    for (;;) {
      const m = await this.next()
      if (!m) throw new Error('minipg: replication connection ended')
      if (m.type === 'T') columns = parseRowDescription(m.body).map((f) => f.name)
      else if (m.type === 'D') rows.push(parseDataRow(m.body).map((c) => (c === null ? null : c.toString('utf8'))))
      else if (m.type === 'E') err = new PgError(parseErrorFields(m.body))
      else if (m.type === 'Z') break
      else if (m.type === 'W') { this.q.unshift(m); break } // CopyBothResponse — streaming begins
    }
    if (err) throw err
    return { columns, rows }
  }

  async identify(): Promise<{ systemId: string; timeline: number; xlogpos: string; dbname: string | null }> {
    const r = await this.command('IDENTIFY_SYSTEM')
    const row = Object.fromEntries(r.columns.map((c, i) => [c, r.rows[0]![i]]))
    return { systemId: row.systemid as string, timeline: Number(row.timeline), xlogpos: row.xlogpos as string, dbname: (row.dbname as string | null) ?? null }
  }

  /** Create a logical slot (pgoutput). `snapshot: 'export'` returns a snapshot name a NORMAL
   *  connection can pin (`begin isolation level repeatable read; set transaction snapshot '…'`)
   *  for a GAPLESS backfill — valid only until this connection's next command. Uses the legacy
   *  keyword syntax (works on PG 10-17). The return type follows the option: 'export' always
   *  yields a snapshot name; 'nothing'/omitted always yields null. */
  async createSlot(name: string, opts: { temporary?: boolean; snapshot: 'export' }): Promise<{ slot: string; consistentPoint: string; snapshot: string }>
  async createSlot(name: string, opts?: { temporary?: boolean; snapshot?: 'nothing' }): Promise<{ slot: string; consistentPoint: string; snapshot: null }>
  async createSlot(name: string, opts: { temporary?: boolean; snapshot?: 'export' | 'nothing' } = {}): Promise<{ slot: string; consistentPoint: string; snapshot: string | null }> {
    checkSlot(name)
    const snap = opts.snapshot === 'export' ? 'EXPORT_SNAPSHOT' : 'NOEXPORT_SNAPSHOT'
    const r = await this.command(`CREATE_REPLICATION_SLOT ${name}${opts.temporary ? ' TEMPORARY' : ''} LOGICAL pgoutput ${snap}`)
    const row = Object.fromEntries(r.columns.map((c, i) => [c, r.rows[0]![i]]))
    return { slot: row.slot_name as string, consistentPoint: row.consistent_point as string, snapshot: (row.snapshot_name as string | null) ?? null }
  }

  async dropSlot(name: string, opts: { wait?: boolean } = {}): Promise<void> {
    checkSlot(name)
    await this.command(`DROP_REPLICATION_SLOT ${name}${opts.wait ? ' WAIT' : ''}`)
  }

  /** Consumer acknowledgement: everything <= the value is durably processed — ack a commit's
   *  `endLsn`. Forgiving: a value inside the LAST delivered commit's record ([lsn, endLsn))
   *  counts as that commit's endLsn, so the natural mistake of acking `e.lsn` still fully
   *  acknowledges a sequentially-consumed stream instead of silently gating the idle-keepalive
   *  advance. Advances the slot's confirmed_flush (releases WAL) on the next status message. */
  ack(lsn: string | bigint): void {
    let v = toLsn(lsn)
    if (v >= this.lastDeliveredStart && v < this.lastDeliveredEnd) v = this.lastDeliveredEnd
    if (v > this.flushed) { this.flushed = v; this.sendStatus() }
  }

  /** START_REPLICATION: an async stream of decoded pgoutput events (proto v1 + messages).
   *  Tuple values decode via the driver's text decoder catalog (config.types honored).
   *  At-least-once: unacked events replay after a reconnect — dedupe by commit LSN. */
  /** Whether the ACTIVE start() stream negotiated binary tuples (resolved 'auto' included). */
  get binaryTuples(): boolean { return this.binaryMode }

  async *start(opts: StartOptions): AsyncGenerator<ReplicationEvent> {
    // before ANY state mutation: shaped/relations/maxQueue below belong to the live stream, and
    // clearing them mid-flight desyncs its decode long before a probe round trip could reject
    if (this.streaming) throw new ReplicationBusy()
    checkSlot(opts.slot) // lazy by construction: an async generator's body runs on the first next(), so nothing is sent before this
    const mqb = opts.maxQueueBytes ?? 64 * 1024 * 1024
    this.maxQueue = Number.isFinite(mqb) && mqb > 0 ? mqb : mqb === Infinity ? Infinity : 64 * 1024 * 1024
    const onAbort = (): void => this.end()
    if (opts.signal) { if (opts.signal.aborted) return; opts.signal.addEventListener('abort', onAbort, { once: true }) }
    const from = opts.from !== undefined ? toLsn(opts.from) : 0n
    const pubs = opts.publications.map((p) => `"${p.replace(/"/g, '""')}"`).join(',')
    this.shaped.clear()
    this.relations.clear() // relations re-announce per stream; stale entries would carry the previous stream's shapes
    for (const s of opts.shapes ?? []) {
      const schema = s.schema ?? 'public'
      if (this.shaped.has(schema + '\0' + s.table)) throw new Error(`minipg: duplicate replication shape for ${schema}.${s.table}`)
      const cols = shapeCols(s.shape) // resolves + validates specs NOW (unknown types/targets fail before streaming)
      for (const c of cols) if (c.path) throw new Error(`minipg: replication shape for ${schema}.${s.table}: Collect() groups several result columns and can't apply to a replication row (group ${JSON.stringify(c.path[0])})`)
      const keys = new Set(cols.map((c) => c.name))
      for (const k of Object.keys(s.columns ?? {})) if (!keys.has(k)) throw new Error(`minipg: replication shape for ${schema}.${s.table}: columns maps ${JSON.stringify(k)} but the shape has no such key`)
      this.shaped.set(schema + '\0' + s.table, { cols, columns: s.columns })
    }
    // RAW-06: unconditional catalog probe — pgoutput itself only reports a missing publication
    // lazily (first decoded change) or not at all on PG18 (downgraded to a WARNING); see
    // PublicationMissing and StartOptions.publications for what this probe can and cannot promise.
    // Runs before this.streaming is set, so command() is still legal here.
    if (opts.publications.length === 0) {
      opts.onWarning?.({ kind: 'publication-empty', publication: '', message: 'minipg: publications is empty — the stream will decode no tables until at least one publication is set' })
    } else {
      const pubLits = opts.publications.map((p) => `'${p.replace(/'/g, "''")}'`).join(',')
      const pubProbe = await this.command(
        'select n.name, p.oid is not null as present, coalesce(t.cnt, 0)::text'
        + ` from unnest(array[${pubLits}]::text[]) as n(name)`
        + ' left join pg_publication p on p.pubname = n.name'
        + ' left join (select pubname, count(*)::int as cnt from pg_publication_tables group by 1) t on t.pubname = n.name')
      const missingPubs: string[] = []
      for (const row of pubProbe.rows) {
        const [name, present, tables] = row as [string, string, string]
        if (present !== 't') { missingPubs.push(name); continue }
        if (tables === '0') opts.onWarning?.({ kind: 'publication-empty', publication: name, message: `minipg: publication ${JSON.stringify(name)} exists but publishes no tables — the stream will start but deliver nothing until it does` })
      }
      if (missingPubs.length) throw new PublicationMissing(missingPubs)
    }
    const want = opts.binary ?? 'auto'
    let bin = want === true
    if (want === 'auto' && this.serverMajor >= 14) {
      const lits = opts.publications.map((p) => `'${p.replace(/'/g, "''")}'`).join(',')
      const probe = await this.command(
        'select distinct a.atttypid from pg_publication_tables pt'
        + ' join pg_namespace n on n.nspname = pt.schemaname'
        + ' join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename'
        + ' join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped'
        + ` where pt.pubname in (${lits})`)
      // capability gate, nothing more: every published column AND every shaped target must have a
      // binary decoder (shaped 'unknown' cols defer to the relation default the probe already vets).
      // One undecodable column -> the whole stream stays text (pgoutput binary is stream-wide).
      bin = probe.rows.length > 0
        && probe.rows.every((r) => replBinaryFor(Number(r[0]), this.decoders) !== null)
        && [...this.shaped.values()].every(({ cols }) => cols.every((c) => c.oid === 0 || replBinaryForCol(c, this.decoders) !== null))
    }
    this.binaryMode = bin
    const sql = `START_REPLICATION SLOT ${opts.slot} LOGICAL ${lsnToString(from)} (proto_version '1', publication_names '${pubs}'${opts.messages === false ? '' : ", messages 'true'"}${bin ? ", binary 'true'" : ''})`
    // set BEFORE the frame write, with the try opened right after: command() must stay guarded
    // across the setup handshake too, and cleared on every exit including a setup-loop PgError
    // (slot already active) — a flag that survives that path deadlocks command() permanently.
    this.streaming = true
    let status: ReturnType<typeof setInterval> | undefined
    let recv: ReturnType<typeof setInterval> | null = null
    try {
      this.frame('Q', Buffer.from(sql + '\0', 'utf8'))
      let setupErr: PgError | null = null
      for (;;) {
        const m = await this.next()
        if (!m) return // end()/abort during setup — clean finish
        // copy mode is open from the moment the server says so — a throw out of onReady() below
        // must not leave the driver believing otherwise, or the next command() writes a plain 'Q'
        // into an open CopyBoth stream and desyncs the connection
        if (m.type === 'W') { this.copyOpen = true; break }
        // simple-protocol errors are always followed by ReadyForQuery — drain to 'Z' before
        // throwing, or the connection's next command() reads this rejection's leftover 'Z'
        // instead of its own results (identify() right after a slot-conflict start() otherwise
        // sees an empty reply)
        if (m.type === 'E') { setupErr = new PgError(parseErrorFields(m.body)); continue }
        if (m.type === 'Z') { throw setupErr ?? new Error('minipg: START_REPLICATION returned no CopyBothResponse') }
      }
      // START_REPLICATION accepted (CopyBothResponse): the slot is active server-side NOW. This is the
      // "streaming established" moment start()'s laziness otherwise hides — a slot-conflict PgError above
      // fires BEFORE this, and a throw from the callback fails this next() like any stream error.
      opts.onReady?.()
      const idleAck = opts.idleAck !== false
      status = setInterval(() => this.sendStatus(), opts.statusIntervalMs ?? 10_000)
      this.lastMessageAt = Date.now()
      const recvMs = opts.receiveTimeoutMs ?? 0
      recv = recvMs > 0
        ? setInterval(() => {
            if (this.qPaused) { this.lastMessageAt = Date.now(); return } // a paused socket has no liveness signal to offer
            if (Date.now() - this.lastMessageAt >= recvMs) this.fail(new ReplicationReceiveTimeout(recvMs))
          }, Math.max(250, Math.min(recvMs / 2, 5_000)))
        : null
      for (;;) {
        const m = await this.next()
        if (!m) return // end()/abort — the consumer's own stop: clean finish through the finally
        if (m.type === 'E') throw new PgError(parseErrorFields(m.body))
        if (m.type === 'c') { // SERVER CopyDone (shutdown/failover): handshake out of copy mode, then THROW — a clean return must stay reserved for the consumer's own stop
          this.copyOpen = false
          try {
            this.frame('c', Buffer.alloc(0))
            for (;;) { const n = await this.next(); if (!n || n.type === 'Z') break } // CommandComplete etc. skipped
          } catch { /* connection died mid-handshake — the stream ending is still the story */ }
          throw new ReplicationStreamEnded()
        }
        if (m.type !== 'd') continue
        const p = m.body
        if (p[0] === 0x6b) { // 'k' keepalive: walEnd, clock, replyRequested
          const walEnd = p.readBigUInt64BE(1)
          if (walEnd > this.lastReceived) this.lastReceived = walEnd
          // nothing unacked -> let the idle slot's confirmed position follow the server
          if (idleAck && this.flushed >= this.lastDeliveredEnd && walEnd > this.flushed) this.flushed = walEnd
          if (p[17] === 1) this.sendStatus()
          continue
        }
        if (p[0] !== 0x77) continue // not XLogData
        const walStart = p.readBigUInt64BE(1)
        if (walStart > this.lastReceived) this.lastReceived = walStart
        const e = this.decode(p.subarray(25)) // 'w' + 3x int64 header
        if (e) {
          if (e.kind === 'commit') { const end = lsnFromString(e.endLsn); if (end > this.lastDeliveredEnd) { this.lastDeliveredStart = lsnFromString(e.lsn); this.lastDeliveredEnd = end } }
          yield e
        }
      }
    } finally {
      this.streaming = false
      this.forceResume()
      // reset BOTH — maxQueue alone would leave a residual byte count from this stream biasing
      // the next start() on this same connection's first pause decision
      this.maxQueue = Infinity
      this.qBytes = 0
      if (status) clearInterval(status)
      if (recv) clearInterval(recv)
      opts.signal?.removeEventListener('abort', onAbort)
    }
  }

  private sendStatus(): void {
    const b = Buffer.allocUnsafe(1 + 8 * 4 + 1)
    b.write('r', 0, 'latin1')
    b.writeBigUInt64BE(this.lastReceived, 1)
    b.writeBigUInt64BE(this.flushed, 9)  // <- the ack: confirmed_flush advances to this
    b.writeBigUInt64BE(this.flushed, 17)
    b.writeBigUInt64BE(BigInt(Date.now()) * 1000n - PG_EPOCH_US, 25)
    b[33] = 0
    try { this.frame('d', b) } catch { /* socket gone; the stream loop surfaces it */ }
  }

  // ---- pgoutput (proto v1 + messages) ----
  private decode(b: Buffer): ReplicationEvent | null {
    const tag = String.fromCharCode(b[0]!)
    let off = 1
    const cstr = (): string => { let e = off; while (b[e] !== 0) e++; const s = b.toString('utf8', off, e); off = e + 1; return s }
    const ts = (us: bigint): Date => new Date(Number((us + PG_EPOCH_US) / 1000n))
    const tsUs = (us: bigint): number => Number(us + PG_EPOCH_US)
    const tuple = (rel: RelEntry): { row: Row; unchanged: string[] } => {
      const n = b.readInt16BE(off); off += 2
      const row: Row = {}
      const unchanged: string[] = []
      for (let i = 0; i < n; i++) {
        const name = rel.names[i] ?? String(i)
        const k = b[off]!; off += 1
        if (k === 0x6e) row[name] = null                                  // 'n' NULL
        else if (k === 0x75) unchanged.push(name)                         // 'u' unchanged TOAST — absent, NOT null
        else {
          const len = b.readInt32BE(off); off += 4
          if (k === 0x62) { // 'b' binary (start({ binary: true }))
            const bd = rel.bin?.[i]
            if (!bd) throw new Error(`minipg: binary tuple value for ${rel.info.schema}.${rel.info.table}.${name} (oid ${rel.info.columns[i]?.oid}) has no binary decoder for its declared/default decode — start() without binary:true, use a binary-capable shape target, or override config.types for this type`)
            row[name] = bd(b, off, len)
          } else { // 't' text
            const dec = rel.decoders[i]
            row[name] = dec ? dec(b, off, len) : b.toString('utf8', off, off + len)
          }
          off += len
        }
      }
      return { row, unchanged }
    }
    const rel = (id: number): RelEntry => {
      const r = this.relations.get(id)
      if (!r) throw new Error(`minipg: unannounced relation ${id} — no Relation message was received for it (protocol desync)`)
      return r
    }
    switch (tag) {
      case 'B': return { kind: 'begin', finalLsn: lsnToString(b.readBigUInt64BE(1)), commitTime: ts(b.readBigUInt64BE(9) ), commitTimeUs: tsUs(b.readBigUInt64BE(9)), xid: b.readInt32BE(17) }
      case 'C': return { kind: 'commit', lsn: lsnToString(b.readBigUInt64BE(2)), endLsn: lsnToString(b.readBigUInt64BE(10)), commitTime: ts(b.readBigUInt64BE(18)), commitTimeUs: tsUs(b.readBigUInt64BE(18)) }
      case 'R': {
        const id = b.readInt32BE(off); off += 4
        const schema = cstr(), table = cstr()
        const replicaIdentity = String.fromCharCode(b[off]!) as ReplicationRelation['replicaIdentity']; off += 1
        const n = b.readInt16BE(off); off += 2
        const columns: ReplicationRelation['columns'] = []
        for (let i = 0; i < n; i++) { const key = b[off]! === 1; off += 1; const cname = cstr(); const oid = b.readInt32BE(off); off += 8; columns.push({ name: cname, oid, key }) }
        const info: ReplicationRelation = { schema, table, replicaIdentity, columns }
        this.relations.set(id, this.relEntry(info))
        return { kind: 'relation', relation: info }
      }
      case 'I': { const r = rel(b.readInt32BE(off)); off += 5; const t = tuple(r); return { kind: 'insert', schema: r.info.schema, table: r.info.table, new: t.row } }
      case 'U': {
        const r = rel(b.readInt32BE(off)); off += 4
        let old: Row | null = null, oldKind: 'key' | 'full' | null = null
        let k = String.fromCharCode(b[off]!); off += 1
        if (k === 'K' || k === 'O') { oldKind = k === 'K' ? 'key' : 'full'; old = tuple(r).row; k = String.fromCharCode(b[off]!); off += 1 }
        const t = tuple(r) // k === 'N'
        // TOAST fill: an unmodified TOASTed value is omitted from the new tuple ('u'), but with
        // REPLICA IDENTITY FULL the very same value is in the old tuple — copy it over (lossless by
        // definition of "unchanged"). `unchanged` KEEPS listing the filled columns, so consumers can
        // still tell retransmitted from filled.
        if (old && oldKind === 'full') for (const name of t.unchanged) if (name in old) t.row[name] = old[name]
        const base = { kind: 'update' as const, schema: r.info.schema, table: r.info.table, new: t.row, unchanged: t.unchanged }
        return old ? { ...base, old, oldKind: oldKind as 'key' | 'full' } : { ...base, old: null, oldKind: null }
      }
      case 'D': { const r = rel(b.readInt32BE(off)); off += 4; const k = String.fromCharCode(b[off]!); off += 1; return { kind: 'delete', schema: r.info.schema, table: r.info.table, old: tuple(r).row, oldKind: k === 'K' ? 'key' : 'full' } }
      case 'T': {
        const n = b.readInt32BE(off); off += 4
        const flags = b[off]!; off += 1
        const tables: { schema: string; table: string }[] = []
        for (let i = 0; i < n; i++) { const r = rel(b.readInt32BE(off)); off += 4; tables.push({ schema: r.info.schema, table: r.info.table }) }
        return { kind: 'truncate', tables, cascade: (flags & 1) !== 0, restartIdentity: (flags & 2) !== 0 }
      }
      case 'M': {
        const transactional = b[off]! === 1; off += 1
        const lsn = lsnToString(b.readBigUInt64BE(off)); off += 8
        const prefix = cstr()
        const len = b.readInt32BE(off); off += 4
        return { kind: 'message', transactional, prefix, content: Buffer.from(b.subarray(off, off + len)), lsn }
      }
      case 'O': case 'Y': return null // origin / type metadata — not surfaced
      default: throw new Error(`minipg: unknown pgoutput message '${tag}'`)
    }
  }

  /** Per-column decoders + output keys for an announced relation. Shape-declared columns decode
   *  via their declared spec — the SAME resolution as a query() { shape } — and the row is keyed
   *  by the SHAPE key (TableShape.columns maps key -> SQL column when they differ); the rest via
   *  pickDecoder's defaults (int8 -> BigInt, temporal -> Date, config.types, jsonBigints). */
  private relEntry(info: ReplicationRelation): RelEntry {
    const shape = this.shaped.get(info.schema + '\0' + info.table)
    const names = info.columns.map((c) => c.name)
    const bySql = new Map<string, CodegenCol>() // SQL column name -> shape col (the OUTPUT key rides in col.name)
    if (shape) {
      for (const sc of shape.cols) {
        const sql = shape.columns?.[sc.name] ?? sc.name
        const i = info.columns.findIndex((c) => c.name === sql)
        if (i < 0) throw new Error(`minipg: replication shape for ${info.schema}.${info.table} declares column ${JSON.stringify(sc.name)}${sql !== sc.name ? ` (-> ${JSON.stringify(sql)})` : ''} but the relation has: ${info.columns.map((c) => c.name).join(', ')}`)
        const prev = bySql.get(sql)
        if (prev) throw new Error(`minipg: replication shape for ${info.schema}.${info.table}: ${JSON.stringify(prev.name)} and ${JSON.stringify(sc.name)} both map to column ${JSON.stringify(sql)}`)
        bySql.set(sql, sc)
        names[i] = sc.name
      }
      const dup = names.find((n, i) => names.indexOf(n) !== i)
      if (dup !== undefined) throw new Error(`minipg: replication shape for ${info.schema}.${info.table}: output key ${JSON.stringify(dup)} collides with another column — map one of them via columns`)
    }
    const xf = (d: CellDecoder, f: (v: unknown) => unknown): CellDecoder => (b, o, l) => f(d(b, o, l))
    const decoders = info.columns.map((c) => {
      const sc = bySql.get(c.name)
      if (!sc) return pickDecoder({ name: c.name, oid: c.oid }, this.decoders)
      // oid 0 = 'unknown': defer to the live relation oid. format:'binary' is a QUERY wire
      // request (BINARY_FAST auto-request in resolveLeaf) — these tuple values arrive as text.
      const d = pickDecoder({ ...sc, oid: sc.oid === 0 ? c.oid : sc.oid, format: undefined }, this.decoders)
      return sc.xform ? xf(d, sc.xform) : d
    })
    const bin = this.binaryMode ? info.columns.map((c) => {
      const sc = bySql.get(c.name)
      if (!sc) return replBinaryFor(c.oid, this.decoders)
      const d = sc.oid === 0 && !sc.json ? replBinaryFor(c.oid, this.decoders) : replBinaryForCol(sc, this.decoders)
      return d && sc.xform ? xf(d, sc.xform) : d
    }) : null
    if (bin) {
      const i = bin.findIndex((d) => d === null)
      if (i >= 0) throw new Error(`minipg: binary tuple value for ${info.schema}.${info.table}.${names[i]} (oid ${info.columns[i]?.oid}) has no binary decoder for its declared/default decode — start() without binary:true, use a binary-capable shape target, or override config.types for this type`)
    }
    return { info, names, decoders, bin }
  }

  end(): void {
    this.ended = true
    this.forceResume() // a paused socket left paused after the stream ends would never drain
    try { this.socket?.write(W.terminate()) } catch { /* */ }
    try { this.socket?.end() } catch { /* */ }
    const w = this.wake; this.wake = null; w?.() // wake a parked next() so an in-flight start() iterator finishes NOW
  }
}
