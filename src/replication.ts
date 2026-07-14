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
import { md5Password, scram, type Scram } from './auth.ts'
import { PgError, parseErrorFields } from './errors.ts'
import { getDefaultTransport, type NormalizedConfig } from './connection.ts'
import { resolveUrl } from './url.ts'
import { buildDecoders } from './decode.ts'
import { pickDecoder, replBinaryFor, replBinaryMatchesText, type CellDecoder } from './decode.ts'
import type { ConnectConfig, Decoder } from './types.ts'

const PG_EPOCH_US = 946684800000000n // 2000-01-01T00:00:00Z in µs

// ---- LSNs: strings ('16/B374D848') publicly, bigint internally ----
export const lsnToString = (lsn: bigint): string => `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xffffffffn).toString(16).toUpperCase()}`
export const lsnFromString = (s: string): bigint => { const [hi, lo] = s.split('/'); return (BigInt(parseInt(hi!, 16)) << 32n) | BigInt(parseInt(lo!, 16)) }
const toLsn = (v: string | bigint): bigint => (typeof v === 'bigint' ? v : lsnFromString(v))

// ---- event model ----
export interface ReplicationRelation { schema: string; table: string; replicaIdentity: 'd' | 'n' | 'f' | 'i'; columns: { name: string; oid: number; key: boolean }[] }
export type Row = Record<string, unknown>
export type ReplicationEvent =
  | { kind: 'begin'; xid: number; commitTime: Date; finalLsn: string }
  | { kind: 'commit'; lsn: string; endLsn: string; commitTime: Date }
  | { kind: 'insert'; schema: string; table: string; new: Row }
  | { kind: 'update'; schema: string; table: string; new: Row; old: Row | null; oldKind: 'key' | 'full' | null; unchanged: string[] }
  | { kind: 'delete'; schema: string; table: string; old: Row; oldKind: 'key' | 'full' }
  | { kind: 'truncate'; tables: { schema: string; table: string }[]; cascade: boolean; restartIdentity: boolean }
  | { kind: 'message'; transactional: boolean; prefix: string; content: Buffer; lsn: string }
  | { kind: 'relation'; relation: ReplicationRelation } // schema (re-)announced — fires again on DDL changes

export type ReplicationConfig = Pick<ConnectConfig,
  'url' | 'host' | 'port' | 'user' | 'password' | 'database' | 'ssl' | 'path' | 'socket' | 'applicationName' | 'connectTimeout' | 'types' | 'jsonBigints'>

export interface StartOptions {
  slot: string
  publications: string[]
  /** LSN to start from; default = the server resumes from the slot's confirmed position. */
  from?: string | bigint
  /** Deliver pg_logical_emit_message() events (default true). */
  messages?: boolean
  /** Standby-status heartbeat interval, ms (default 10_000). */
  statusIntervalMs?: number
  /** BINARY tuple values (PG14+ pgoutput option). Default 'auto': enabled only when the server is
   *  14+ AND a pre-start catalog probe shows every column of the published tables decodes to the
   *  IDENTICAL JS value in binary and text — so auto can never change what your consumer sees.
   *  (float4 and array columns are binary-capable but value-divergent — float4 binary is the exact
   *  f32, arrays become real JS arrays instead of raw '{…}' text — so their presence keeps auto on
   *  text.) `true` forces binary: fixed-width decode, exact numeric strings, JS arrays; a type with
   *  no binary decoder (e.g. interval) then errors loudly naming table.column. `false` = text.
   *  Caveat under 'auto'/'true': DDL AFTER the stream starts isn't re-probed — a new column of an
   *  undecodable type errors on its first binary value. */
  binary?: boolean | 'auto'
  /** When idle with nothing unacked, advance the flushed LSN to the server's keepalive
   *  position so an idle slot doesn't retain WAL forever (default true). */
  idleAck?: boolean
}

/** Open a logical-replication connection (walsender). One purpose per connection: this cannot
 *  run extended-protocol queries — use a normal connect() alongside it. */
export async function replication(config: string | ReplicationConfig = {}): Promise<ReplicationConnection> {
  const c = new ReplicationConnection(typeof config === 'string' ? { url: config } : config)
  await c.connect()
  return c
}

interface RelEntry { info: ReplicationRelation; decoders: CellDecoder[]; bin: (CellDecoder | null)[] | null }

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
  private lastReceived = 0n
  private flushed = 0n
  private lastDeliveredEnd = 0n // highest commit endLsn handed to the consumer (guards idleAck)
  private binaryMode = false    // resolved binary decision for the active start() stream
  private serverMajor = 0       // from the server_version ParameterStatus at startup

  constructor(config: ReplicationConfig = {}) {
    const rc = resolveUrl(config)
    const user = rc.user || process.env.PGUSER || process.env.USER || process.env.USERNAME || 'postgres'
    this.decoders = buildDecoders(rc.types, rc.jsonBigints)
    this.cfg = {
      host: rc.host || process.env.PGHOST || 'localhost',
      port: rc.port || Number(process.env.PGPORT) || 5432,
      user,
      password: rc.password ?? process.env.PGPASSWORD ?? '',
      database: rc.database || process.env.PGDATABASE || user,
      ssl: rc.ssl && rc.ssl !== 'disable' ? rc.ssl : false,
      applicationName: rc.applicationName || 'minipg-replication',
      connectTimeout: rc.connectTimeout ?? 30000,
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
      this.socket = this.cfg.socket ? await this.cfg.socket() : await transport!(this.cfg, ac.signal)
      this.socket.on('data', (d: Buffer) => this.onData(d))
      this.socket.on('error', (e: Error) => this.fail(e))
      this.socket.on('close', () => { if (!this.ended) this.fail(this.err ?? new Error('replication connection closed unexpectedly')) })
      // THE one special startup parameter: replication=database selects the logical walsender
      this.socket.write(W.startup({ user: this.cfg.user, database: this.cfg.database, application_name: this.cfg.applicationName, replication: 'database' }))
      for (;;) {
        const m = await this.next()
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
      case 10: { this.scramState = scram(this.cfg.password); sock.write(W.saslInitial(this.scramState.mechanism, this.scramState.clientFirst)); return }
      case 11: sock.write(W.saslResponse(this.scramState!.continue(body.subarray(4).toString('utf8')))); return
      case 12: this.scramState!.final(body.subarray(4).toString('utf8')); return
      default: throw new Error('unsupported authentication request: ' + code)
    }
  }

  private onData(d: Buffer): void {
    let msgs: RawMessage[]
    try { msgs = this.parser.push(Buffer.from(d)) } catch (e) { this.fail(e as Error); return }
    if (msgs.length) this.q.push(...msgs)
    const w = this.wake; this.wake = null; w?.()
  }
  private fail(e: Error): void { this.err ??= e; const w = this.wake; this.wake = null; w?.() }
  private param(b: Buffer): void { // ParameterStatus: name\0value\0 — we only care about the version
    let z = 0; while (b[z] !== 0) z++
    if (b.toString('latin1', 0, z) !== 'server_version') return
    let e = z + 1; while (b[e] !== 0) e++
    this.serverMajor = parseInt(b.toString('latin1', z + 1, e), 10) || 0
  }
  private async next(): Promise<RawMessage> {
    for (;;) {
      const m = this.q.shift()
      if (m) { if (m.type === 'S') { this.param(m.body); continue } if (m.type === 'K' || m.type === 'N' || m.type === 'A') continue; return m }
      if (this.err) throw this.err
      await new Promise<void>((r) => { this.wake = r })
    }
  }
  private frame(type: string, payload: Buffer): void {
    const head = Buffer.allocUnsafe(5)
    head.write(type, 0, 'latin1'); head.writeInt32BE(payload.length + 4, 1)
    this.socket?.write(Buffer.concat([head, payload]))
  }

  /** Simple-protocol command: the walsender grammar (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, …)
   *  or plain SQL — the logical walsender accepts both. Rows come back as text. */
  async command(sql: string): Promise<{ columns: string[]; rows: (string | null)[][] }> {
    this.frame('Q', Buffer.from(sql + '\0', 'utf8'))
    let columns: string[] = []
    const rows: (string | null)[][] = []
    let err: Error | null = null
    for (;;) {
      const m = await this.next()
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
   *  keyword syntax (works on PG 10-17). */
  async createSlot(name: string, opts: { temporary?: boolean; snapshot?: 'export' | 'nothing' } = {}): Promise<{ slot: string; consistentPoint: string; snapshot: string | null }> {
    const snap = opts.snapshot === 'export' ? 'EXPORT_SNAPSHOT' : 'NOEXPORT_SNAPSHOT'
    const r = await this.command(`CREATE_REPLICATION_SLOT ${name}${opts.temporary ? ' TEMPORARY' : ''} LOGICAL pgoutput ${snap}`)
    const row = Object.fromEntries(r.columns.map((c, i) => [c, r.rows[0]![i]]))
    return { slot: row.slot_name as string, consistentPoint: row.consistent_point as string, snapshot: (row.snapshot_name as string | null) ?? null }
  }

  async dropSlot(name: string, opts: { wait?: boolean } = {}): Promise<void> {
    await this.command(`DROP_REPLICATION_SLOT ${name}${opts.wait ? ' WAIT' : ''}`)
  }

  /** Consumer acknowledgement: everything <= lsn is durably processed. Advances the slot's
   *  confirmed_flush (releases WAL) on the next status message. */
  ack(lsn: string | bigint): void {
    const v = toLsn(lsn)
    if (v > this.flushed) { this.flushed = v; this.sendStatus() }
  }

  /** START_REPLICATION: an async stream of decoded pgoutput events (proto v1 + messages).
   *  Tuple values decode via the driver's text decoder catalog (config.types honored).
   *  At-least-once: unacked events replay after a reconnect — dedupe by commit LSN. */
  /** Whether the ACTIVE start() stream negotiated binary tuples (resolved 'auto' included). */
  get binaryTuples(): boolean { return this.binaryMode }

  async *start(opts: StartOptions): AsyncGenerator<ReplicationEvent> {
    const from = opts.from !== undefined ? toLsn(opts.from) : 0n
    const pubs = opts.publications.map((p) => `"${p.replace(/"/g, '""')}"`).join(',')
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
      bin = probe.rows.length > 0 && probe.rows.every((r) => replBinaryMatchesText(Number(r[0]), this.decoders))
    }
    this.binaryMode = bin
    const sql = `START_REPLICATION SLOT ${opts.slot} LOGICAL ${lsnToString(from)} (proto_version '1', publication_names '${pubs}'${opts.messages === false ? '' : ", messages 'true'"}${bin ? ", binary 'true'" : ''})`
    this.frame('Q', Buffer.from(sql + '\0', 'utf8'))
    for (;;) {
      const m = await this.next()
      if (m.type === 'W') break
      if (m.type === 'E') throw new PgError(parseErrorFields(m.body))
    }
    const idleAck = opts.idleAck !== false
    const status = setInterval(() => this.sendStatus(), opts.statusIntervalMs ?? 10_000)
    try {
      for (;;) {
        const m = await this.next()
        if (m.type === 'E') throw new PgError(parseErrorFields(m.body))
        if (m.type === 'c') return // server CopyDone
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
          if (e.kind === 'commit') { const end = lsnFromString(e.endLsn); if (end > this.lastDeliveredEnd) this.lastDeliveredEnd = end }
          yield e
        }
      }
    } finally { clearInterval(status) }
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
    const tuple = (rel: RelEntry): { row: Row; unchanged: string[] } => {
      const n = b.readInt16BE(off); off += 2
      const row: Row = {}
      const unchanged: string[] = []
      for (let i = 0; i < n; i++) {
        const col = rel.info.columns[i]
        const name = col?.name ?? String(i)
        const k = b[off]!; off += 1
        if (k === 0x6e) row[name] = null                                  // 'n' NULL
        else if (k === 0x75) unchanged.push(name)                         // 'u' unchanged TOAST — absent, NOT null
        else {
          const len = b.readInt32BE(off); off += 4
          if (k === 0x62) { // 'b' binary (start({ binary: true }))
            const bd = rel.bin?.[i]
            if (!bd) throw new Error(`minipg: binary tuple value for ${rel.info.schema}.${rel.info.table}.${name} (oid ${col?.oid}) has no binary decoder — start() without binary:true, or override config.types for this type`)
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
    const rel = (id: number): RelEntry => this.relations.get(id) ?? { info: { schema: '?', table: `?${id}`, replicaIdentity: 'd', columns: [] }, decoders: [], bin: null }
    switch (tag) {
      case 'B': return { kind: 'begin', finalLsn: lsnToString(b.readBigUInt64BE(1)), commitTime: ts(b.readBigUInt64BE(9) ), xid: b.readInt32BE(17) }
      case 'C': return { kind: 'commit', lsn: lsnToString(b.readBigUInt64BE(2)), endLsn: lsnToString(b.readBigUInt64BE(10)), commitTime: ts(b.readBigUInt64BE(18)) }
      case 'R': {
        const id = b.readInt32BE(off); off += 4
        const schema = cstr(), table = cstr()
        const replicaIdentity = String.fromCharCode(b[off]!) as ReplicationRelation['replicaIdentity']; off += 1
        const n = b.readInt16BE(off); off += 2
        const columns: ReplicationRelation['columns'] = []
        for (let i = 0; i < n; i++) { const key = b[off]! === 1; off += 1; const cname = cstr(); const oid = b.readInt32BE(off); off += 8; columns.push({ name: cname, oid, key }) }
        const info: ReplicationRelation = { schema, table, replicaIdentity, columns }
        // per-column decoders via pickDecoder = the SAME defaults as plain query() columns
        // (int8 -> BigInt, temporal -> Date, config.types overrides, jsonBigints for json)
        this.relations.set(id, {
          info,
          decoders: columns.map((c) => pickDecoder({ name: c.name, oid: c.oid }, this.decoders)),
          bin: this.binaryMode ? columns.map((c) => replBinaryFor(c.oid, this.decoders)) : null,
        })
        return { kind: 'relation', relation: info }
      }
      case 'I': { const r = rel(b.readInt32BE(off)); off += 5; const t = tuple(r); return { kind: 'insert', schema: r.info.schema, table: r.info.table, new: t.row } }
      case 'U': {
        const r = rel(b.readInt32BE(off)); off += 4
        let old: Row | null = null, oldKind: 'key' | 'full' | null = null
        let k = String.fromCharCode(b[off]!); off += 1
        if (k === 'K' || k === 'O') { oldKind = k === 'K' ? 'key' : 'full'; old = tuple(r).row; k = String.fromCharCode(b[off]!); off += 1 }
        const t = tuple(r) // k === 'N'
        return { kind: 'update', schema: r.info.schema, table: r.info.table, new: t.row, old, oldKind, unchanged: t.unchanged }
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

  end(): void {
    this.ended = true
    try { this.socket?.write(W.terminate()) } catch { /* */ }
    try { this.socket?.end() } catch { /* */ }
  }
}
