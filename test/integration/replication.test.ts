// replication(): logical replication over the real auth/transport stack (TCP + SCRAM here).
// Requires the local cluster with wal_level=logical (test/setup-pg.sh cluster, reconfigured).
import { test, expect, describe } from 'bun:test'
import { replication, connect, type ReplicationEvent } from '../../src/index.ts'
import { TEST_CONFIG, withConn, testPool, TEST_TIMEOUT } from '../helpers/db.ts'

const K = `repl_${process.pid}`

async function collectUntil(gen: AsyncGenerator<ReplicationEvent>, done: (events: ReplicationEvent[]) => boolean): Promise<ReplicationEvent[]> {
  const events: ReplicationEvent[] = []
  for await (const e of gen) { events.push(e); if (done(events)) break }
  return events
}

describe('replication()', () => {
  test('connects (SCRAM over TCP) and IDENTIFY_SYSTEM works', async () => {
    const repl = await replication(TEST_CONFIG)
    try {
      const sys = await repl.identify()
      expect(sys.timeline).toBeGreaterThan(0)
      expect(sys.xlogpos).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
      expect(sys.dbname).toBe('testdb')
    } finally { repl.end() }
  }, TEST_TIMEOUT)

  test('streams decoded insert/update/delete/message events; temp slot; catalog decoding', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_t(id int4 primary key, name text, ok bool, data jsonb)`)
      await c.query(`create publication ${K}_pub for table ${K}_t`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_slot`, { temporary: true })
          await c.query(`insert into ${K}_t values (1, 'alice', true, '{"a":1}')`)
          await c.query(`update ${K}_t set name = 'bob' where id = 1`)
          await c.query(`delete from ${K}_t where id = 1`)
          await c.query(`select pg_logical_emit_message(true, '${K}', 'marker')`)

          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_pub`] }),
            (es) => es.some((e) => e.kind === 'message'),
          )
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.table).toBe(`${K}_t`)
          expect(ins.new).toEqual({ id: 1, name: 'alice', ok: true, data: { a: 1 } }) // int4->number, bool->boolean, jsonb->object
          const upd = events.find((e) => e.kind === 'update') as Extract<ReplicationEvent, { kind: 'update' }>
          expect(upd.new.name).toBe('bob')
          expect(upd.old).toBeNull() // REPLICA IDENTITY DEFAULT
          const del = events.find((e) => e.kind === 'delete') as Extract<ReplicationEvent, { kind: 'delete' }>
          expect(del.old.id).toBe(1)
          expect(del.oldKind).toBe('key')
          const msg = events.find((e) => e.kind === 'message') as Extract<ReplicationEvent, { kind: 'message' }>
          expect(msg.prefix).toBe(K)
          expect(msg.content.toString()).toBe('marker')
          expect(events.some((e) => e.kind === 'relation')).toBe(true)
        } finally { repl.end() } // temporary slot drops with the connection
      } finally {
        await c.query(`drop publication ${K}_pub`)
        await c.query(`drop table ${K}_t`)
      }
    })
  }, TEST_TIMEOUT)

  test('gapless backfill: exported snapshot excludes post-slot rows; stream delivers them', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_g(id int4 primary key)`)
      await c.query(`create publication ${K}_gpub for table ${K}_g`)
      await c.query(`insert into ${K}_g select generate_series(1, 3)`)
      const repl = await replication(TEST_CONFIG)
      try {
        const slot = await repl.createSlot(`${K}_gslot`, { temporary: true, snapshot: 'export' })
        await c.query(`insert into ${K}_g values (4)`) // after the slot: stream-only
        const seen = await withConn(async (c2) =>
          c2.begin({ isolation: 'repeatable read' }, async (tx) => {
            await tx.query(`set transaction snapshot '${slot.snapshot}'`)
            return tx.query(`select count(*)::int4 from ${K}_g`)
          }))
        expect((seen.rows[0] as unknown[])[0]).toBe(3) // row 4 invisible to the snapshot
        const events = await collectUntil(
          repl.start({ slot: slot.slot, publications: [`${K}_gpub`], from: slot.consistentPoint }),
          (es) => es.some((e) => e.kind === 'commit'),
        )
        const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
        expect(ins.new.id).toBe(4) // …and arrives as the FIRST streamed tx
      } finally {
        repl.end()
        await c.query(`drop publication ${K}_gpub`)
        await c.query(`drop table ${K}_g`)
      }
    })
  }, TEST_TIMEOUT)

  test('cursor + snapshot: eager open() pins per-table backfills, CONCURRENT with streaming', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_b1(id int4 primary key, v text)`)
      await c.query(`create table ${K}_b2(id int4 primary key)`)
      await c.query(`create publication ${K}_bpub for table ${K}_b1, ${K}_b2`)
      await c.query(`insert into ${K}_b1 select g, 'v' || g from generate_series(1, 2500) g`)
      await c.query(`insert into ${K}_b2 select generate_series(1, 7)`)
      const pool = testPool({ max: 2 })
      const repl = await replication(TEST_CONFIG)
      try {
        const slot = await repl.createSlot(`${K}_bslot`, { temporary: true, snapshot: 'export' })
        // pin BOTH tables' cursors eagerly (while the slot tx is open)…
        const bf1 = pool.cursor({ sql: `select * from ${K}_b1`, snapshot: slot.snapshot!, fullScan: true, fetchSize: 1000 })
        const bf2 = pool.cursor({ sql: `select * from ${K}_b2`, snapshot: slot.snapshot!, fullScan: true })
        await Promise.all([bf1.open(), bf2.open()])
        // …then a post-slot write, and START STREAMING while the cursors are still undrained
        await c.query(`insert into ${K}_b1 values (9001, 'streamed')`)
        const streamP = collectUntil(
          repl.start({ slot: slot.slot, publications: [`${K}_bpub`] }),
          (es) => es.some((e) => e.kind === 'commit'),
        )
        const drain = async (cur: AsyncIterable<Record<string, unknown>[]>): Promise<Record<string, unknown>[]> => {
          const all: Record<string, unknown>[] = []
          for await (const batch of cur) all.push(...batch)
          return all
        }
        const [rows1, rows2, events] = await Promise.all([drain(bf1), drain(bf2), streamP])
        expect(rows1.length).toBe(2500)                       // batched (3 fetches of 1000)
        expect(rows1.some((r) => r.id === 9001)).toBe(false)  // post-slot row NOT in the snapshot
        expect(rows1[0]).toEqual({ id: 1, v: 'v1' })          // decoded like normal queries
        expect(rows2.length).toBe(7)
        const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
        expect(ins.new.id).toBe(9001)                          // …and arrives via the stream instead
      } finally {
        repl.end()
        await pool.end()
        await c.query(`drop publication ${K}_bpub`)
        await c.query(`drop table ${K}_b1, ${K}_b2`)
      }
    })
  }, TEST_TIMEOUT)

  test('ack advances confirmed_flush on a durable slot; dropSlot cleans up', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_a(id int4 primary key)`)
      await c.query(`create publication ${K}_apub for table ${K}_a`)
      const repl = await replication(TEST_CONFIG)
      try {
        const slot = await repl.createSlot(`${K}_aslot`) // durable
        await c.query(`insert into ${K}_a values (1)`)
        const events = await collectUntil(
          repl.start({ slot: slot.slot, publications: [`${K}_apub`] }),
          (es) => es.some((e) => e.kind === 'commit'),
        )
        const commit = events.find((e) => e.kind === 'commit') as Extract<ReplicationEvent, { kind: 'commit' }>
        repl.ack(commit.endLsn)
        await Bun.sleep(300)
        const cf = await c.query(`select confirmed_flush_lsn::text from pg_replication_slots where slot_name = '${K}_aslot'`)
        expect((cf.rows[0] as unknown[])[0]).toBe(commit.endLsn)
      } finally {
        repl.end()
        // drop via a FRESH replication connection (the streaming one is stuck in CopyBoth)
        const r2 = await replication(TEST_CONFIG)
        try { await r2.dropSlot(`${K}_aslot`) } finally { r2.end() }
        await c.query(`drop publication ${K}_apub`)
        await c.query(`drop table ${K}_a`)
      }
    })
  }, TEST_TIMEOUT)
})

// keep the import used even if helpers change
void connect
