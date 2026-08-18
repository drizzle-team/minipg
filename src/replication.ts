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
  | ({
      kind: 'update'; schema: string; table: string; new: Row
      /** SHAPE keys of TOASTed columns pgoutput did not resend because they are unmodified since
       *  the last change — listed here in BOTH modes. With StartOptions.hydrateToast (default
       *  true) and a FULL old row, the driver fills these into `new` from `old`; otherwise they
       *  are ABSENT from `new`, not null (null would collide with SQL NULL), and genuinely
       *  unrecoverable — a follow-up query reads the CURRENT row, a different row than this event
       *  describes. */
      unchanged: string[]
      /** Present only when TableShape.key was declared for this table. Under `oldKind: 'full'`
       *  the declared key columns are compared (see `sameValue`). Under `oldKind: 'key'` the
       *  answer is `true` with no comparison — the tuple's presence is itself the signal. Under
       *  `oldKind: null` the answer is `false` with no comparison. The 'key'/null answers are
       *  exact when the declared key equals the replica identity key. For a declared key that is
       *  a proper subset of the identity, 'key' over-reports: Postgres sends the old tuple
       *  whenever ANY identity column changed, so `true` can appear when the declared columns
       *  themselves did not change. The StartOptions.onWarning replica-identity warning covers
       *  every table whose old row is not full — a superset of the affected tables, not a precise
       *  list. */
      keyChanged?: boolean
    } & (
      | {
          /** Under identity DEFAULT/INDEX, 'key' appears only when the identity key changed —
           *  an exact key-change signal with no declared key needed. Under 'key' the old row is
           *  full width with `null` in every non-identity column; those nulls mean "not carried
           *  on the wire", not "was SQL NULL". Under FULL every column is flagged `key: true` in
           *  ReplicationRelation.columns, so that flag cannot identify the real key — `keyChanged`
           *  needs a declared TableShape.key. */
          oldKind: 'key' | 'full'
          old: Row
        }
      | { oldKind: null; old: null }
    ))
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
  /** SHAPE keys (not SQL column names — same rule as `columns` and `unchanged`: "row is keyed by
   *  the SHAPE keys") that make up this table's identity for `keyChanged`. Every entry must exist
   *  in `shape` (a typo throws). Declaring updates for this table then carry `keyChanged`. A
   *  consumer wanting `keyChanged` with otherwise-default decoding declares a shape containing
   *  only the key columns — undeclared columns keep default catalog decoding under their SQL
   *  names, so this costs nothing. Array- and json-valued key columns always compare as changed —
   *  the comparator has no recursive branch — so a key made of such a column reads `keyChanged:
   *  true` on every update; prefer scalar key columns. An empty array throws — an empty key
   *  cannot change, so omit `key` instead. */
  key?: string[]
  /** OUTPUT key -> spec: the SAME grammar (and the same key semantics) as query()'s { shape } —
   *  TypeSpec strings ('int8:number', 'numeric:bigint', 'int8[]:number', …), Json()/Jsonb()
   *  markers, Transform(), and defineType()/minipg-geometry markers; object literal or ordered
   *  [key, spec] entries. Collect() groups several result columns and can't apply to a
   *  replication row — it throws at start(). */
  shape: Record<string, TypeSpec | JsonMarker | TransformMarker | CustomMarker> | SpecEntries<TypeSpec | JsonMarker | TransformMarker | CustomMarker>
}

export interface StartOptions {
  slot: string
  /** Publications to subscribe. An empty list rejects with PublicationEmpty (pgoutput has no wire
   *  form for it) and a missing publication with PublicationMissing, both before START_REPLICATION
   *  is written (a driver-side catalog probe — pgoutput itself only reports a missing publication
   *  lazily, and PG18 no longer reports it to the client at all). One that exists but publishes no
   *  tables yet is legal: it warns through onWarning and streams. The probe only answers "does it
   *  exist NOW": a publication created after the slot can still be invisible to the slot's
   *  historical catalog snapshot even once this check passes, and tables added to a publication
   *  after start() stream normally. */
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
  /** Fills unchanged TOASTed columns into `new` from a REPLICA IDENTITY FULL old row (default
   *  true). `false` restores the sparse WAL-faithful shape where those columns are ABSENT from
   *  `new`; `unchanged` lists them in BOTH modes. Only a FULL old row can fill — under identity
   *  DEFAULT/INDEX the value is simply not on the wire either way. */
  hydrateToast?: boolean
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
  /** The library's only warning channel — non-fatal conditions detected before the stream begins
   *  (a publication that publishes no tables yet, for example) and, for event-triggered
   *  diagnostics, from inside the stream as the triggering event is decoded. Called synchronously,
   *  zero or more times. Absent = the warning is simply not delivered; minipg never writes the
   *  warnings themselves to the process streams. A throw from this callback is caught and reported
   *  to stderr, and the stream keeps going: a diagnostic must not take down the thing it reports
   *  on. This is where onWarning differs from onReady, whose throw does fail the current next().
   *  Later diagnostics reuse this same field with new ReplicationWarning `kind` members rather
   *  than adding a second channel. */
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

/** command() or a second start() was called while a start() stream holds the connection. A
 *  start() generator reserves the connection from the moment it begins running (its first
 *  next()) — not merely once the stream goes live — because both a second start() and a plain
 *  command() would drain the same message queue the reservation's own catalog probes are using.
 *  A created-but-never-iterated generator reserves nothing.
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

/** `start({ publications: [] })` — no publication at all to subscribe. There is no wire form for
 *  this: pgoutput reads an empty publication_names list as absent ("publication_names parameter
 *  missing"), and the binary capability probe would emit an empty IN list that is itself a syntax
 *  error. A publication that EXISTS but publishes no tables is a different, legal case — it warns
 *  (see ReplicationWarning) and streams. */
export class PublicationEmpty extends Error {
  readonly reason = 'publications-empty' as const
  constructor() { super('minipg: publications is empty — pgoutput needs at least one publication to stream') }
}

/** A non-fatal condition detected before or during the stream — the library's only warning
 *  channel (see StartOptions.onWarning). `publication-empty`: a named publication exists but
 *  publishes no tables right now, so the stream starts and delivers nothing until tables are
 *  added to it (a FOR ALL TABLES publication over an empty database, say). `replica-identity`: an
 *  update or delete arrived without a full old row, so some prior state could not be reconstructed
 *  — fires once per table per stream, from inside the stream as the triggering event is decoded.
 *  `partition-relation`: events for this table arrive under a leaf partition's own name because
 *  its publication does not set publish_via_partition_root — fires once per table per stream, at
 *  the table's Relation announce. A table can warn once per stream PER KIND: one shared `warned`
 *  set keyed on kind too, so a leaf partition's announce-time partition-relation warning never
 *  suppresses its own replica-identity warning (a Relation always precedes the first tuple). Later
 *  phases add new `kind` members without reshaping this field. */
export type ReplicationWarning =
  | { kind: 'publication-empty'; publication: string; message: string }
  | { kind: 'replica-identity'; schema: string; table: string; replicaIdentity: 'd' | 'n' | 'f' | 'i'; message: string }
  | { kind: 'partition-relation'; schema: string; table: string; publication: string; message: string }

/** Open a logical-replication connection (walsender). One purpose per connection: this cannot
 *  run extended-protocol queries — use a normal connect() alongside it. */
export async function replication(config: string | ReplicationConfig = {}): Promise<ReplicationConnection> {
  const c = new ReplicationConnection(typeof config === 'string' ? { url: config } : config)
  await c.connect()
  return c
}

// onWarning is diagnostic: a throw from it must not take down the stream it is reporting on, so
// the callback's own failure goes to stderr instead of failing the consumer's next(). This is the
// one thing minipg writes to a process stream, and it is a consumer bug rather than a warning.
const deliverWarning = (cb: ((w: ReplicationWarning) => void) | undefined, w: ReplicationWarning): void => {
  if (!cb) return
  try { cb(w) } catch (e) { console.error('minipg: onWarning callback threw', e) }
}

interface RelEntry { info: ReplicationRelation; names: string[]; decoders: CellDecoder[]; bin: (CellDecoder | null)[] | null; key?: string[] } // names[i] = row OUTPUT key for column i (shape key when shaped, else the SQL name)

// Key-column equality for keyChanged: Object.is covers primitives/string/bigint/same-ref; Date compares
// by getTime() (so two Invalid Dates read equal); ArrayBuffer views (Buffer included) compare byte-wise.
// Arrays and json values always read as changed — no recursive branch; that case is exotic enough as a
// declared key that the cost of getting it wrong outweighs the cost of documenting the limitation.
const sameValue = (a: unknown, b: unknown): boolean => {
  if (Object.is(a, b)) return true
  if (a instanceof Date && b instanceof Date) return Object.is(a.getTime(), b.getTime())
  if (ArrayBuffer.isView(a) && ArrayBuffer.isView(b)) {
    const x = new Uint8Array(a.buffer, a.byteOffset, a.byteLength)
    const y = new Uint8Array(b.buffer, b.byteOffset, b.byteLength)
    if (x.length !== y.length) return false
    for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false
    return true
  }
  return false
}

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
  private shaped = new Map<string, { cols: CodegenCol[]; columns?: Record<string, string>; key?: string[] }>() // '<schema>\0<table>' -> resolved shape for the active start() stream
  private lastReceived = 0n
  private flushed = 0n
  private lastDeliveredEnd = 0n   // highest commit endLsn handed to the consumer (guards idleAck)
  private lastDeliveredStart = 0n // that same commit's lsn — ack() normalizes values inside [start, end)
  private binaryMode = false    // resolved binary decision for the active start() stream
  private hydrateToast = true   // resolved TOAST-fill decision for the active start() stream
  private warn?: (w: ReplicationWarning) => void // onWarning for the active start() stream — cleared in the finally
  private warned = new Set<string>()             // 'kind\0schema\0table' — cleared at the top of start() beside relations.clear()
  private leafPartitions = new Map<string, string>() // 'schema\0table' -> publication, from the pre-start probe; empty when opts.onWarning is absent
  private serverMajor = 0       // from the server_version ParameterStatus at startup
  private lastMessageAt = 0     // stamped on every raw socket read — the receiveTimeoutMs liveness clock
  private qPaused = false       // set by maxQueueBytes backpressure; a paused socket has no liveness signal to offer
  private qBytes = 0            // buffered-but-undelivered message bytes — the maxQueueBytes ceiling
  private maxQueue = Infinity   // resolved from opts.maxQueueBytes at the top of start()
  private streaming = false     // held from the top of an iterated start() until the stream ends — guards command() and a second start() alike
  private copyOpen = false      // true once CopyBothResponse is accepted; a consumer that stops iterating without
                                 // reaching a server CopyDone leaves this true — command() exits copy mode first
  // held on the instance, not in start()'s frame: a generator suspended at a yield has nobody
  // parked in next(), so end() can never reach its finally — only end() itself can stop these
  private timers: ReturnType<typeof setInterval>[] = []
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
    return this.run(sql)
  }

  // guard-free body of command(): used above, and by start()'s own pre-stream probes once
  // start() already holds the reservation those probes run under.
  private async run(sql: string): Promise<{ columns: string[]; rows: (string | null)[][] }> {
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
      else if (m.type === 'W') { this.q.unshift(m); this.qBytes += m.body.length + 5; break } // CopyBothResponse — streaming begins
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
    // reserved before the first await: an async generator's body runs synchronously from entry
    // to its first await, so setting the flag on the statement right after the guard closes the
    // window entirely — a second start() or a command() sees the guard before any catalog probe
    // (or any other await) can run. The guard must stay OUTSIDE the try below: a rejected second
    // caller must never reach the finally, or it would release the FIRST caller's reservation.
    if (this.streaming) throw new ReplicationBusy()
    this.streaming = true
    const onAbort = (): void => this.end()
    try {
      checkSlot(opts.slot) // lazy by construction: an async generator's body runs on the first next(), so nothing is sent before this
      if (opts.signal?.aborted) return
      const from = opts.from !== undefined ? toLsn(opts.from) : 0n
      // publication_names is a comma-separated identifier list carried inside a single-quoted
      // walsender option literal, so each name is escaped twice: once for the identifier parser,
      // once for the literal it rides in. The catalog probe below quotes the same names its own way.
      const pubs = opts.publications.map((p) => `"${p.replace(/"/g, '""')}"`).join(',').replace(/'/g, "''")
      this.shaped.clear()
      this.relations.clear() // relations re-announce per stream; stale entries would carry the previous stream's shapes
      this.warned.clear() // per-stream warning dedupe; a re-start() after a reconnect warns again
      for (const s of opts.shapes ?? []) {
        const schema = s.schema ?? 'public'
        if (this.shaped.has(schema + '\0' + s.table)) throw new Error(`minipg: duplicate replication shape for ${schema}.${s.table}`)
        const cols = shapeCols(s.shape) // resolves + validates specs NOW (unknown types/targets fail before streaming)
        for (const c of cols) if (c.path) throw new Error(`minipg: replication shape for ${schema}.${s.table}: Collect() groups several result columns and can't apply to a replication row (group ${JSON.stringify(c.path[0])})`)
        const keys = new Set(cols.map((c) => c.name))
        for (const k of Object.keys(s.columns ?? {})) if (!keys.has(k)) throw new Error(`minipg: replication shape for ${schema}.${s.table}: columns maps ${JSON.stringify(k)} but the shape has no such key`)
        if (s.key && s.key.length === 0) throw new Error(`minipg: replication shape for ${schema}.${s.table}: key is an empty array — declare at least one shape key or omit key`)
        for (const k of s.key ?? []) if (!keys.has(k)) throw new Error(`minipg: replication shape for ${schema}.${s.table}: key lists ${JSON.stringify(k)} but the shape has no such key`)
        this.shaped.set(schema + '\0' + s.table, { cols, columns: s.columns, key: s.key })
      }
      // unconditional catalog probe — pgoutput itself only reports a missing publication
      // lazily (first decoded change) or not at all on PG18 (downgraded to a WARNING); see
      // PublicationMissing and StartOptions.publications for what this probe can and cannot promise.
      // Uses the guard-free run(): start() already holds the reservation this probe runs under.
      if (opts.publications.length === 0) throw new PublicationEmpty()
      const pubLits = opts.publications.map((p) => `'${p.replace(/'/g, "''")}'`).join(',')
      const pubProbe = await this.run(
        'select n.name, p.oid is not null as present, coalesce(t.cnt, 0)::text'
        + ` from unnest(array[${pubLits}]::text[]) as n(name)`
        + ' left join pg_publication p on p.pubname = n.name'
        + ' left join (select pubname, count(*)::int as cnt from pg_publication_tables group by 1) t on t.pubname = n.name')
      const missingPubs: string[] = []
      for (const row of pubProbe.rows) {
        const [name, present, tables] = row as [string, string, string]
        if (present !== 't') { missingPubs.push(name); continue }
        // legal and worth streaming: FOR ALL TABLES over a database with no tables yet, or a
        // publication whose tables are added after start() — those changes reach the stream
        if (tables === '0') deliverWarning(opts.onWarning, { kind: 'publication-empty', publication: name, message: `minipg: publication ${JSON.stringify(name)} exists but publishes no tables — the stream will start but deliver nothing until it does` })
      }
      if (missingPubs.length) throw new PublicationMissing(missingPubs)
      // leaf-partition detection for the partition-relation warning: skipped when nobody asked for
      // warnings (no round trip for a consumer who never passed onWarning). Uses run(): start()
      // already holds the reservation.
      const leaves = new Map<string, string>()
      if (opts.onWarning) {
        const leafProbe = await this.run(
          'select distinct pt.schemaname, pt.tablename, p.pubname'
          + ' from pg_publication p'
          + ' join pg_publication_tables pt on pt.pubname = p.pubname'
          + ' join pg_namespace n on n.nspname = pt.schemaname'
          + ' join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename'
          + ` where not p.pubviaroot and c.relispartition and p.pubname in (${pubLits})`)
        for (const row of leafProbe.rows) {
          const [schema, table, pub] = row as [string, string, string]
          leaves.set(schema + '\0' + table, pub)
        }
      }
      this.leafPartitions = leaves
      const want = opts.binary ?? 'auto'
      let bin = want === true
      if (want === 'auto' && this.serverMajor >= 14) {
        const lits = opts.publications.map((p) => `'${p.replace(/'/g, "''")}'`).join(',')
        const probe = await this.run(
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
      this.hydrateToast = opts.hydrateToast !== false
      this.warn = opts.onWarning
      // as late as possible, and only once nothing above can still throw: an abort listener left on
      // the consumer's signal by a failed start() would end a connection they went on to re-start(),
      // and a stale ceiling would govern this connection's plain command() traffic
      if (opts.signal?.aborted) return
      const mqb = opts.maxQueueBytes ?? 64 * 1024 * 1024
      this.maxQueue = Number.isFinite(mqb) && mqb > 0 ? mqb : mqb === Infinity ? Infinity : 64 * 1024 * 1024
      opts.signal?.addEventListener('abort', onAbort, { once: true })
      const sql = `START_REPLICATION SLOT ${opts.slot} LOGICAL ${lsnToString(from)} (proto_version '1', publication_names '${pubs}'${opts.messages === false ? '' : ", messages 'true'"}${bin ? ", binary 'true'" : ''})`
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
      this.timers.push(setInterval(() => this.sendStatus(), opts.statusIntervalMs ?? 10_000))
      this.lastMessageAt = Date.now()
      const recvMs = opts.receiveTimeoutMs ?? 0
      if (recvMs > 0) this.timers.push(setInterval(() => {
        if (this.qPaused) { this.lastMessageAt = Date.now(); return } // a paused socket has no liveness signal to offer
        if (Date.now() - this.lastMessageAt >= recvMs) this.fail(new ReplicationReceiveTimeout(recvMs))
      }, Math.max(250, Math.min(recvMs / 2, 5_000))))
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
      // every stream entry assigns this.warn before any decode runs, so a stale callback can
      // never actually fire; the clear's effect is releasing the finished consumer's closure at
      // stream end instead of retaining it until the next start()
      this.warn = undefined
      this.forceResume()
      this.maxQueue = Infinity
      // recount rather than zero: a consumer that stopped mid-stream leaves undelivered frames in
      // the queue, and next() still subtracts their bytes as they drain — zeroing here drives the
      // counter negative and loosens the next stream's ceiling by the abandoned residue
      this.qBytes = this.q.reduce((n, m) => n + m.body.length + 5, 0)
      this.clearTimers()
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
        this.warnPartition(schema, table)
        return { kind: 'relation', relation: info }
      }
      case 'I': { const r = rel(b.readInt32BE(off)); off += 5; const t = tuple(r); return { kind: 'insert', schema: r.info.schema, table: r.info.table, new: t.row } }
      case 'U': {
        const r = rel(b.readInt32BE(off)); off += 4
        let old: Row | null = null, oldKind: 'key' | 'full' | null = null
        let k = String.fromCharCode(b[off]!); off += 1
        if (k === 'K' || k === 'O') { oldKind = k === 'K' ? 'key' : 'full'; old = tuple(r).row; k = String.fromCharCode(b[off]!); off += 1 }
        if (oldKind !== 'full') this.warnIdentity(r, 'update')
        const t = tuple(r) // k === 'N'
        // TOAST fill: an unmodified TOASTed value is omitted from the new tuple ('u'), but with
        // REPLICA IDENTITY FULL the very same value is in the old tuple — copy it over (lossless by
        // definition of "unchanged"). `unchanged` KEEPS listing the filled columns, so consumers can
        // still tell retransmitted from filled.
        if (this.hydrateToast && old && oldKind === 'full') for (const name of t.unchanged) if (name in old) t.row[name] = old[name]
        // keyChanged: 'key' means the identity key changed — the tuple's presence IS the signal
        // (it is NULL-padded outside the identity, so comparing it would be wrong). null means it
        // did not. Under 'full', compare the declared key columns, treating a name in `unchanged`
        // as equal WITHOUT reading t.row — that value's presence depends on hydrateToast, which
        // must not change the keyChanged answer.
        let keyChanged: boolean | undefined
        if (r.key) keyChanged = oldKind === 'key' ? true : oldKind === null ? false
          : r.key.some((name) => !t.unchanged.includes(name) && !sameValue(old![name], t.row[name]))
        const base = { kind: 'update' as const, schema: r.info.schema, table: r.info.table, new: t.row, unchanged: t.unchanged, ...(r.key ? { keyChanged } : {}) }
        return old ? { ...base, old, oldKind: oldKind as 'key' | 'full' } : { ...base, old: null, oldKind: null }
      }
      case 'D': {
        const r = rel(b.readInt32BE(off)); off += 4
        const k = String.fromCharCode(b[off]!); off += 1
        const oldKind = k === 'K' ? 'key' : 'full'
        if (oldKind !== 'full') this.warnIdentity(r, 'delete')
        return { kind: 'delete', schema: r.info.schema, table: r.info.table, old: tuple(r).row, oldKind }
      }
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

  // 'n' (NOTHING) is unreachable here — a publication rejects updates/deletes on a table with no
  // replica identity — but the type still allows it, so the message names it for completeness.
  private warnIdentity(r: RelEntry, ctx: 'update' | 'delete'): void {
    if (!this.warn) return
    const { schema, table, replicaIdentity } = r.info
    const key = 'replica-identity\0' + schema + '\0' + table
    if (this.warned.has(key)) return
    this.warned.add(key)
    const detail = ctx === 'update'
      ? 'the old row is absent or carries only identity-key columns (non-key columns read null meaning not-carried), and unchanged TOASTed columns cannot be filled into new'
      : 'the old row carries only the identity key'
    deliverWarning(this.warn, {
      kind: 'replica-identity', schema, table, replicaIdentity,
      message: `minipg: ${schema}.${table} ${ctx} arrived without a full old row (replica identity '${replicaIdentity}') — ${detail}; set REPLICA IDENTITY FULL to receive the full old row`,
    })
  }

  // Delivered at the Relation announce rather than at start(): Relation messages are lazy (only
  // relations that actually produce a change get announced), so this never false-positives on a
  // published-but-idle partition and never floods a FOR ALL TABLES publication with warnings for
  // tables that never change.
  private warnPartition(schema: string, table: string): void {
    if (!this.warn) return
    const pub = this.leafPartitions.get(schema + '\0' + table)
    if (pub === undefined) return
    const key = 'partition-relation\0' + schema + '\0' + table
    if (this.warned.has(key)) return
    this.warned.add(key)
    deliverWarning(this.warn, {
      kind: 'partition-relation', schema, table, publication: pub,
      message: `minipg: ${schema}.${table} events arrive under the partition's own name because publication ${JSON.stringify(pub)} does not set publish_via_partition_root — create it with publish_via_partition_root = true to receive events under the root's name`,
    })
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
    return { info, names, decoders, bin, key: shape?.key }
  }

  private clearTimers(): void { for (const t of this.timers) clearInterval(t); this.timers.length = 0 }

  end(): void {
    this.ended = true
    this.clearTimers()
    this.forceResume() // a paused socket left paused after the stream ends would never drain
    try { this.socket?.write(W.terminate()) } catch { /* */ }
    try { this.socket?.end() } catch { /* */ }
    const w = this.wake; this.wake = null; w?.() // wake a parked next() so an in-flight start() iterator finishes NOW
  }
}

/** done:false carries no commit fields — reading them requires narrowing to done:true, so a
 *  mid-transaction ack is a compile error rather than a runtime mistake. */
export type TransactionBatch =
  | { xid: number; events: ReplicationEvent[]; done: false }
  | {
      xid: number; events: ReplicationEvent[]; done: true
      commitLsn: string; endLsn: string; commitTime: Date; commitTimeUs: number
    }

/** The flat event stream -> per-transaction batches. begin/commit are consumed onto the
 *  envelope, never placed in events[]; the commit fields (commitLsn, endLsn, commitTime,
 *  commitTimeUs) land only on the done:true chunk.
 *
 *  An empty DDL-produced transaction (begin immediately followed by commit) yields an
 *  events-empty done:true batch like any other — acking it keeps the slot releasing WAL through
 *  a DDL burst. relation and truncate events stay in events[]; dropping truncate is consumer
 *  policy, not this helper's. A non-transactional pg_logical_emit_message event arrives outside
 *  any begin/commit pair, so there is no batch to hold it, and it is dropped; a transactional
 *  message stays in events[].
 *
 *  Without opts.maxEvents the batch is unbounded — maxQueueBytes bounds the socket's undelivered
 *  queue, NOT the assembled batch, so a consumer relying on it for batch memory safety is
 *  exposed. With maxEvents set, an oversized transaction chunks into done:false pieces carrying
 *  no commit fields, then a final done:true chunk carrying them; a transaction whose size is an
 *  exact multiple of maxEvents still ends in a done:true chunk, empty, as the delivery vehicle
 *  for those fields.
 *
 *  A throw from the source stream mid-transaction propagates and discards the buffered events —
 *  nothing partial is ackable, so nothing partial is visible. break out of the consumer's
 *  for-await closes the underlying stream: return() propagates through the inner for-await to
 *  the source's return(). */
export async function* batchTransactions(
  stream: AsyncIterable<ReplicationEvent>,
  opts: { maxEvents?: number } = {},
): AsyncGenerator<TransactionBatch> {
  const max = opts.maxEvents && opts.maxEvents > 0 ? opts.maxEvents : Infinity
  let xid = 0
  let events: ReplicationEvent[] = []
  for await (const e of stream) {
    if (e.kind === 'begin') { xid = e.xid; events = []; continue }
    if (e.kind === 'commit') {
      yield { xid, events, done: true, commitLsn: e.lsn, endLsn: e.endLsn, commitTime: e.commitTime, commitTimeUs: e.commitTimeUs }
      events = []
      continue
    }
    if (e.kind === 'message' && !e.transactional) continue // arrives outside any begin/commit — no batch to hold it
    events.push(e)
    if (events.length >= max) { yield { xid, events, done: false }; events = [] }
  }
}
