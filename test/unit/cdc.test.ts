// Proves the managed layer's OWN decisions — which commands it issues, in what order, whether it
// acks, whether it reconnects — driven over an in-process fake walsender (cdcBackend, a
// multi-session wrapper around the raw layer's fake-Duplex harness). What this fixture cannot
// prove: anything only Postgres decides (real catalog state, snapshot validity, privileges) —
// those live in test/integration/replication.test.ts.
import { test, expect } from 'bun:test'
import { Duplex } from 'node:stream'
import { frame, rowDescription, dataRow as dataRowBody, type WireCol, type Cell } from '../helpers/wire.ts'
import { replication as rawReplication, batchTransactions as rawBatchTransactions, type ReplicationConfig, type TransactionBatch } from '../../src/index.ts'
import { replicate, SlotInvalidatedError, BackfillTimeoutError, SlotBusyError } from '../../src/cdc.ts'

const i32 = (n: number) => { const b = Buffer.allocUnsafe(4); b.writeInt32BE(n); return b }
const u16 = (n: number) => { const b = Buffer.allocUnsafe(2); b.writeUInt16BE(n); return b }
const i64zero = Buffer.alloc(8)
const i64 = (n: bigint) => { const b = Buffer.allocUnsafe(8); b.writeBigInt64BE(n); return b }

const authOk = () => frame('R', i32(0))
const ready = (s = 'I') => frame('Z', Buffer.from(s, 'latin1'))
const rowDesc = (cols: WireCol[]) => frame('T', rowDescription(cols))
const dataRow = (cells: Cell[]) => frame('D', dataRowBody(cells))
const copyBoth = () => frame('W', Buffer.from([0, 0, 0]))
const xlogData = (payload: Buffer) => frame('d', Buffer.concat([Buffer.from('w', 'latin1'), i64zero, i64zero, i64zero, payload]))
const pgBegin = () => xlogData(Buffer.concat([Buffer.from('B', 'latin1'), i64zero, i64zero, i32(1)]))

const cstrPg = (s: string) => Buffer.concat([Buffer.from(s, 'latin1'), Buffer.from([0])])
const pgRelation = (id: number, schema: string, table: string, identity: string, cols: { name: string; oid: number; key?: boolean }[]) =>
  xlogData(Buffer.concat([
    Buffer.from('R', 'latin1'), i32(id), cstrPg(schema), cstrPg(table), Buffer.from(identity, 'latin1'),
    u16(cols.length),
    ...cols.map((c) => Buffer.concat([Buffer.from([c.key ? 1 : 0]), cstrPg(c.name), i32(c.oid), i32(-1)])),
  ]))
const textCell = (s: string) => Buffer.concat([Buffer.from([0x74]), i32(Buffer.byteLength(s, 'utf8')), Buffer.from(s, 'utf8')])
// pgoutput Insert ('I'): relid + a fixed 'N' tuple-kind byte + the tuple itself (count + cells).
const pgInsert = (relid: number, cells: Buffer[]) =>
  xlogData(Buffer.concat([Buffer.from('I', 'latin1'), i32(relid), Buffer.from('N', 'latin1'), u16(cells.length), ...cells]))
// pgoutput Commit ('C'): one zero flag byte + commit lsn + end lsn + a commit timestamp (unused here).
const pgCommit = (commitLsn: bigint, endLsn: bigint) =>
  xlogData(Buffer.concat([Buffer.from('C', 'latin1'), Buffer.from([0]), i64(commitLsn), i64(endLsn), i64(0n)]))

/** Scans CopyData bodies for standby status updates ('r', 0x72) and returns the most recent
 *  wal-flushed position (byte offset 9) — the layer's own ack output, not a value the fake
 *  backend computes for it. Defaults to 0n: nothing has been acked yet. */
const lastFlushed = (sent: Buffer[]): bigint => {
  for (let i = sent.length - 1; i >= 0; i--) {
    const b = sent[i]!
    if (b[0] === 0x72) return b.readBigUInt64BE(9)
  }
  return 0n
}

/** Polls `check` until it returns true or `timeoutMs` elapses — the fake backend is entirely
 *  in-process, so a session reaching a given wire state (e.g. START_REPLICATION sent) settles in
 *  microtasks, not real network latency; a short poll interval is deliberate. */
async function until(check: () => boolean, timeoutMs = 2000): Promise<void> {
  const t0 = Date.now()
  while (!check()) {
    if (Date.now() - t0 > timeoutMs) throw new Error('minipg test: timed out waiting for condition')
    await Bun.sleep(5)
  }
}

interface CdcSession { dx: Duplex; queries: string[]; sent: Buffer[] }

/** A multi-session fake walsender: each socket() call authenticates a FRESH Duplex and records
 *  its own queries/sent arrays onto `sessions` — cdc.ts's reconnect tests (later plans) need one
 *  session per connection, so nothing here is shared across sessions. The shared default router
 *  answers exactly the commands the managed layer's happy path issues; opts.onQuery replaces it
 *  entirely for tests that need one branch to answer differently. */
function cdcBackend(opts: { onQuery?: (sql: string) => Buffer[] } = {}) {
  const sessions: CdcSession[] = []
  const onQuery = opts.onQuery ?? ((sql: string): Buffer[] => {
    if (sql.startsWith('START_REPLICATION')) return [copyBoth()]
    // 4-column reply, exactly what src/replication.ts's createSlot() destructures
    if (sql.startsWith('CREATE_REPLICATION_SLOT')) {
      const name = sql.split(' ')[1]!
      return [
        rowDesc([{ name: 'slot_name', oid: 25 }, { name: 'consistent_point', oid: 25 }, { name: 'snapshot_name', oid: 25 }, { name: 'output_plugin', oid: 25 }]),
        dataRow([name, '0/10', 'snap-1', 'pgoutput']),
        ready(),
      ]
    }
    // 4-column reply, exactly what identify() destructures (unused by this plan's happy path,
    // kept for forward-compatibility with the durable-slot tests later plans add)
    if (sql.startsWith('IDENTIFY_SYSTEM')) {
      return [
        rowDesc([{ name: 'systemid', oid: 25 }, { name: 'timeline', oid: 25 }, { name: 'xlogpos', oid: 25 }, { name: 'dbname', oid: 25 }]),
        dataRow(['7000000000000000001', '1', '0/10', 'd']),
        ready(),
      ]
    }
    // D-07's derived-timeout probe, run once per connect in S2 (preparing), before createSlot.
    if (sql.includes('wal_sender_timeout')) {
      return [rowDesc([{ name: 'setting', oid: 25 }]), dataRow(['60000']), ready()]
    }
    // must precede the pg_publication branch below: the partition probe SQL also contains
    // 'pg_publication', and would otherwise get misrouted into the 3-column publication response
    if (sql.includes('relispartition')) {
      return [
        rowDesc([{ name: 'schemaname', oid: 25 }, { name: 'tablename', oid: 25 }, { name: 'pubname', oid: 25 }]),
        ready(),
      ]
    }
    // 5-column TEXT reply. Booleans are 't'/'f' — never true/false, never 'true'.
    if (sql.includes('pg_replication_slots')) {
      return [
        rowDesc([{ name: 'active', oid: 25 }, { name: 'active_pid', oid: 25 }, { name: 'wal_status', oid: 25 }, { name: 'confirmed_flush_lsn', oid: 25 }, { name: 'restart_lsn', oid: 25 }]),
        dataRow(['f', null, 'reserved', '0/10', '0/8']),
        ready(),
      ]
    }
    if (sql.includes('pg_publication')) {
      const names = [...sql.matchAll(/'([^']*)'/g)].map((m) => m[1]!)
      return [
        rowDesc([{ name: 'name', oid: 25 }, { name: 'present', oid: 25 }, { name: 'tables', oid: 25 }]),
        ...names.map((n) => dataRow([n, 't', '1'])),
        ready(),
      ]
    }
    return [ready()]
  })

  const socket = (): Duplex => {
    let authenticated = false
    const queries: string[] = []
    const sent: Buffer[] = []
    const dx: Duplex = new Duplex({
      write(chunk: Buffer, _enc, cb) {
        if (!authenticated) {
          authenticated = true // whole startup packet arrives in one write; no type byte to parse
          dx.push(authOk())
          dx.push(ready())
          cb()
          return
        }
        let off = 0
        while (off < chunk.length) {
          const type = String.fromCharCode(chunk[off]!)
          const len = chunk.readInt32BE(off + 1)
          const body = chunk.subarray(off + 5, off + 1 + len)
          off += 1 + len
          if (type === 'Q') {
            let z = 0; while (body[z] !== 0) z++
            const sql = body.toString('utf8', 0, z)
            queries.push(sql)
            for (const f of onQuery(sql)) dx.push(f)
          } else if (type === 'c') {
            dx.push(ready()) // CopyDone -> CommandComplete/ReadyForQuery, as a real walsender answers
          } else if (type === 'd' || type === 'X') {
            sent.push(Buffer.from(body))
          }
        }
        cb()
      },
      read() { /* pushed manually */ },
    })
    const session: CdcSession = { dx, queries, sent }
    sessions.push(session)
    return dx
  }

  return { socket, sessions, get latest(): CdcSession | undefined { return sessions[sessions.length - 1] } }
}

const cfg = (o: Partial<ReplicationConfig> = {}): ReplicationConfig => ({ user: 'u', database: 'd', ...o })

test('cdc exports: replicate is importable and the raw surface is untouched', () => {
  expect(typeof replicate).toBe('function')
  expect(typeof SlotInvalidatedError).toBe('function')
  expect(typeof BackfillTimeoutError).toBe('function')
  expect(typeof SlotBusyError).toBe('function')
  expect(typeof rawReplication).toBe('function') // CDC-01: the raw surface is unchanged underneath
  expect(typeof rawBatchTransactions).toBe('function')
})

test('cdc window order: no command runs between CREATE_REPLICATION_SLOT and START_REPLICATION', async () => {
  const backend = cdcBackend()
  let queriesAtBackfillStart = -1
  let queriesWhilePending = -1
  const handle = replicate({
    url: cfg({ socket: backend.socket }),
    slot: 'temporary',
    publications: ['pub'],
    backfill: async () => {
      queriesAtBackfillStart = backend.latest!.queries.length
      await Bun.sleep(20)
      queriesWhilePending = backend.latest!.queries.length
    },
    onTransaction: () => {},
  })
  await until(() => !!backend.latest?.queries.some((q) => q.startsWith('START_REPLICATION')))
  const queries = backend.latest!.queries
  const settingsIdx = queries.findIndex((q) => q.includes('wal_sender_timeout'))
  const createIdx = queries.findIndex((q) => q.startsWith('CREATE_REPLICATION_SLOT'))
  const startIdx = queries.findIndex((q) => q.startsWith('START_REPLICATION'))
  expect(settingsIdx).toBeGreaterThanOrEqual(0)
  expect(createIdx).toBeGreaterThan(settingsIdx) // fixed admin order: the probe precedes createSlot
  expect(startIdx).toBeGreaterThan(createIdx)
  // The real CDC-11 guarantee: the layer itself issues ZERO commands on this connection while
  // backfill is in flight. start()'s own pre-stream probes (publication, leaf-partition) are
  // legitimate — but only once backfill has already returned, which is why they land AFTER
  // createIdx rather than making createIdx and startIdx wire-adjacent.
  expect(queriesAtBackfillStart).toBe(createIdx + 1)
  expect(queriesWhilePending).toBe(queriesAtBackfillStart)
  await handle.stop()
})

test('cdc ack after: a transaction is acked only once onTransaction resolves', async () => {
  // Part A: a single done:true batch — no ack while the handler is pending, one once it resolves
  const backend = cdcBackend()
  const batches: TransactionBatch[] = []
  const gates: (() => void)[] = []
  const handle = replicate({
    url: cfg({ socket: backend.socket }),
    slot: 'temporary',
    publications: ['pub'],
    backfill: async () => {},
    onTransaction: async (batch) => {
      batches.push(batch)
      await new Promise<void>((resolve) => { gates.push(resolve) })
    },
  })
  await until(() => !!backend.latest?.queries.some((q) => q.startsWith('START_REPLICATION')))
  const session = backend.latest!
  const relId = 5
  const commitLsn = 0x20n, endLsn = 0x30n
  session.dx.push(pgBegin())
  session.dx.push(pgRelation(relId, 'public', 't', 'd', [{ name: 'id', oid: 23, key: true }]))
  session.dx.push(pgInsert(relId, [textCell('1')]))
  session.dx.push(pgCommit(commitLsn, endLsn))

  await until(() => batches.length >= 1)
  expect(batches[0]!.done).toBe(true)
  expect(lastFlushed(session.sent)).toBeLessThan(endLsn) // handler still pending — no ack has run
  gates[0]!()
  await until(() => lastFlushed(session.sent) >= endLsn)
  expect(lastFlushed(session.sent)).toBeGreaterThanOrEqual(endLsn)
  await handle.stop()

  // Part B: maxTransactionEvents 1 — every done:false chunk carries no commit fields (nothing to
  // ack, by construction); only the trailing done:true chunk advances the flushed position.
  const backend2 = cdcBackend()
  const batches2: TransactionBatch[] = []
  const gates2: (() => void)[] = []
  const handle2 = replicate({
    url: cfg({ socket: backend2.socket }),
    slot: 'temporary',
    publications: ['pub'],
    maxTransactionEvents: 1,
    backfill: async () => {},
    onTransaction: async (batch) => {
      batches2.push(batch)
      await new Promise<void>((resolve) => { gates2.push(resolve) })
    },
  })
  await until(() => !!backend2.latest?.queries.some((q) => q.startsWith('START_REPLICATION')))
  const session2 = backend2.latest!
  const relId2 = 6
  const commitLsn2 = 0x40n, endLsn2 = 0x50n
  session2.dx.push(pgBegin())
  session2.dx.push(pgRelation(relId2, 'public', 't2', 'd', [{ name: 'id', oid: 23, key: true }]))
  session2.dx.push(pgInsert(relId2, [textCell('1')]))
  session2.dx.push(pgCommit(commitLsn2, endLsn2))

  await until(() => batches2.length >= 1)
  while (!batches2[batches2.length - 1]!.done) {
    const idx = batches2.length - 1
    expect(batches2[idx]!.done).toBe(false)
    gates2[idx]!() // no ack is even expressible for a done:false chunk
    await until(() => batches2.length > idx + 1)
  }
  const doneIdx = batches2.length - 1
  expect(batches2[doneIdx]!.done).toBe(true)
  expect(lastFlushed(session2.sent)).toBeLessThan(endLsn2) // the done:true chunk's handler still pending
  gates2[doneIdx]!()
  await until(() => lastFlushed(session2.sent) >= endLsn2)
  expect(lastFlushed(session2.sent)).toBeGreaterThanOrEqual(endLsn2)
  await handle2.stop()
})

test('cdc one connection: all slot administration rides the single streaming session', async () => {
  const backend = cdcBackend()
  const handle = replicate({
    url: cfg({ socket: backend.socket }),
    slot: 'temporary',
    publications: ['pub'],
    backfill: async () => {},
    onTransaction: () => {},
  })
  await until(() => !!backend.latest?.queries.some((q) => q.startsWith('START_REPLICATION')))
  expect(backend.sessions.length).toBe(1)
  const queries = backend.latest!.queries
  const settingsIdx = queries.findIndex((q) => q.includes('wal_sender_timeout'))
  const createIdx = queries.findIndex((q) => q.startsWith('CREATE_REPLICATION_SLOT'))
  const startIdx = queries.findIndex((q) => q.startsWith('START_REPLICATION'))
  expect(settingsIdx).toBeGreaterThanOrEqual(0)
  expect(createIdx).toBeGreaterThan(settingsIdx)
  expect(startIdx).toBeGreaterThan(createIdx)
  await handle.stop()
})
