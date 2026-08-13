// replication(): logical replication over the real auth/transport stack (TCP + SCRAM here).
// Requires the local cluster with wal_level=logical (test/setup-pg.sh cluster, reconfigured).
import { test, expect, describe } from 'bun:test'
import { replication, connect, defineType, Jsonb, Collect, Transform, ReplicationStreamEnded, ReplicationBusy, PgError, type ReplicationEvent, type TableShape } from '../../src/index.ts'
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
        const drain = async (cur: { batches(): AsyncGenerator<Record<string, unknown>[]> }): Promise<Record<string, unknown>[]> => {
          const all: Record<string, unknown>[] = []
          for await (const batch of cur.batches()) all.push(...batch)
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

  test('tuple decode matches query() defaults: int8 -> BigInt, timestamptz -> Date, numeric -> exact string', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_d(id int8 primary key, at timestamptz, price numeric, tags int4[])`)
      await c.query(`create publication ${K}_dpub for table ${K}_d`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_dslot`, { temporary: true })
          await c.query(`insert into ${K}_d values (9007199254740993, '2026-01-02T03:04:05.678Z', 10.50, array[1,2,3])`)
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_dpub`], binary: false }), // pin TEXT: this test is about text-mode defaults
            (es) => es.some((e) => e.kind === 'commit'))
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.new.id).toBe(9007199254740993n)                     // BigInt, exact (was a string before)
          expect(ins.new.at).toBeInstanceOf(Date)
          expect((ins.new.at as Date).getTime()).toBe(Date.UTC(2026, 0, 2, 3, 4, 5, 678))
          expect(ins.new.price).toBe('10.50')                            // exact string, like query()
          expect(ins.new.tags).toBe('{1,2,3}')                           // text mode: raw literal, like a plain query
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_dpub`)
        await c.query(`drop table ${K}_d`)
      }
    })
  }, TEST_TIMEOUT)

  test("binary 'auto' (default): engages when every column is binary-decodable, stays text otherwise", async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_a1(id int8 primary key, price numeric, name text, ns int4[])`) // all decodable (arrays included)
      await c.query(`create table ${K}_a2(id int8 primary key, dur interval)`)                        // interval: no binary decoder
      await c.query(`create publication ${K}_a1pub for table ${K}_a1`)
      await c.query(`create publication ${K}_a2pub for table ${K}_a2`)
      try {
        for (const [pub, tbl, expectBinary] of [[`${K}_a1pub`, `${K}_a1`, true], [`${K}_a2pub`, `${K}_a2`, false]] as const) {
          const repl = await replication(TEST_CONFIG)
          try {
            const slot = await repl.createSlot(`${tbl}_slot`, { temporary: true })
            await c.query(tbl.endsWith('a1') ? `insert into ${tbl} values (1, 10.50, 'x', array[1,2])` : `insert into ${tbl} values (1, interval '1 day')`)
            const events = await collectUntil(
              repl.start({ slot: slot.slot, publications: [pub] }), // binary unset -> 'auto'
              (es) => es.some((e) => e.kind === 'commit'))
            expect(repl.binaryTuples).toBe(expectBinary)
            const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
            expect(ins.new.id).toBe(1n)
            if (expectBinary) { expect(ins.new.price).toBe('10.50'); expect(ins.new.ns).toEqual([1, 2]) } // binary: real JS array
            else expect(ins.new.dur).toBe('1 day') // undecodable column -> whole stream text, interval as PG text
          } finally { repl.end() }
        }
      } finally {
        await c.query(`drop publication ${K}_a1pub`); await c.query(`drop publication ${K}_a2pub`)
        await c.query(`drop table ${K}_a1, ${K}_a2`)
      }
    })
  }, TEST_TIMEOUT)

  test("binary 'auto' + shapes: a text-only shape target keeps the stream on text (negotiation matches decode)", async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_a3(id int8 primary key, at timestamptz)`) // every column binary-decodable by default
      await c.query(`create publication ${K}_a3pub for table ${K}_a3`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_a3slot`, { temporary: true })
          await c.query(`insert into ${K}_a3 values (1, '2024-01-02T03:04:05Z')`)
          const events = await collectUntil(
            repl.start({
              slot: slot.slot, publications: [`${K}_a3pub`],
              shapes: [{ table: `${K}_a3`, shape: { at: 'timestamptz:string' } }], // binary can't produce PG's text
            }),
            (es) => es.some((e) => e.kind === 'commit'))
          expect(repl.binaryTuples).toBe(false) // the shaped target flipped auto to text — no decode-time crash possible
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.new.at).toMatch(/^2024-01-02 /) // decoded as the declared exact PG text
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_a3pub`)
        await c.query(`drop table ${K}_a3`)
      }
    })
  }, TEST_TIMEOUT)

  test('binary tuples: start({ binary: true }) — fixed-width decode, exact numeric, JS arrays', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_b(id int8 primary key, price numeric, u uuid, name text, ok bool, at timestamptz, ns int4[], meta jsonb, raw bytea)`)
      await c.query(`create publication ${K}_bpub for table ${K}_b`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_bslot`, { temporary: true })
          await c.query(
            `insert into ${K}_b values (9007199254740993, 10.50, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'café 😀', true, '2026-01-02T03:04:05.678Z', array[1,null,3], '{"a":1}', '\\xdeadbeef')`)
          await c.query(`insert into ${K}_b values (2, -0.00012, null, null, false, null, array[]::int4[], null, null)`)
          await c.query(`insert into ${K}_b values (3, 'NaN', null, null, false, null, null, null, null)`)
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_bpub`], binary: true }),
            (es) => es.filter((e) => e.kind === 'commit').length >= 3)
          const ins = events.filter((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>[]
          expect(ins[0]!.new.id).toBe(9007199254740993n)
          expect(ins[0]!.new.price).toBe('10.50')                        // binary numeric -> exact text render
          expect(ins[0]!.new.u).toBe('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
          expect(ins[0]!.new.name).toBe('café 😀')
          expect(ins[0]!.new.ok).toBe(true)
          expect((ins[0]!.new.at as Date).getTime()).toBe(Date.UTC(2026, 0, 2, 3, 4, 5, 678))
          expect(ins[0]!.new.ns).toEqual([1, null, 3])                   // binary mode: REAL JS array
          expect(ins[0]!.new.meta).toEqual({ a: 1 })
          expect(ins[0]!.new.raw).toEqual(Buffer.from('deadbeef', 'hex'))
          expect(ins[1]!.new.price).toBe('-0.00012')                     // small-fraction numeric (weight < 0)
          expect(ins[1]!.new.ns).toEqual([])
          expect(ins[2]!.new.price).toBe('NaN')
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_bpub`)
        await c.query(`drop table ${K}_b`)
      }
    })
  }, TEST_TIMEOUT)

  test('start({ shapes }): declared columns decode like a query() shape; undeclared keep defaults', async () => {
    const doubler = defineType(`${K}_doubler`, { ascii: true, targets: { x2: (s) => Number(s) * 2 } })
    await withConn(async (c) => {
      await c.query(`create table ${K}_s(id int4 primary key, big int8, n numeric, at timestamptz, tags int8[], meta jsonb, label text, loc point, dbl int4)`)
      await c.query(`create publication ${K}_spub for table ${K}_s`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_sslot`, { temporary: true })
          await c.query(
            `insert into ${K}_s values (7, 42, '1267650600228229401496703205376', '2024-01-02T03:04:05Z', array[1,2], '{"when":"2024-01-02T03:04:05"}', 'hi', '(1.5,2.5)', 21)`)
          await c.query(`update ${K}_s set label = 'yo' where id = 7`)
          const events = await collectUntil(
            repl.start({
              slot: slot.slot, publications: [`${K}_spub`],
              shapes: [{
                table: `${K}_s`,
                shape: {
                  big: 'int8:number',
                  n: 'numeric:bigint',
                  at: 'timestamptz:ms',
                  tags: 'int8[]:number',
                  meta: Jsonb({ when: 'timestamp:ms' }),
                  label: Transform('text', (s) => (s as string).toUpperCase()),
                  loc: 'point:xy',
                  dbl: doubler('x2'),
                },
              }],
            }),
            (es) => es.filter((e) => e.kind === 'commit').length >= 2)
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.new.id).toBe(7)                                            // undeclared -> default int4
          expect(ins.new.big).toBe(42)                                          // :number -> a JS number, not the default 42n
          expect(ins.new.n).toBe(1267650600228229401496703205376n)              // numeric:bigint = 2^100 exact
          expect(ins.new.at).toBe(Date.UTC(2024, 0, 2, 3, 4, 5))                // :ms epoch number
          expect(ins.new.tags).toEqual([1, 2])                                  // int8[]:number -> JS numbers (default: raw '{1,2}')
          expect(ins.new.meta).toEqual({ when: Date.UTC(2024, 0, 2, 3, 4, 5) }) // Jsonb() field target
          expect(ins.new.label).toBe('HI')                                      // Transform()
          expect(ins.new.loc).toEqual({ x: 1.5, y: 2.5 })                       // point:xy
          expect(ins.new.dbl).toBe(42)                                          // defineType() marker (name-driven, real oid irrelevant)
          const upd = events.find((e) => e.kind === 'update') as Extract<ReplicationEvent, { kind: 'update' }>
          expect(upd.new.label).toBe('YO')                                      // shapes apply to every tuple of the table
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_spub`)
        await c.query(`drop table ${K}_s`)
      }
    })
  }, TEST_TIMEOUT)

  test('start({ shapes }): never silent — bad column, Collect(), duplicates all throw', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_e(id int4 primary key, name text)`)
      await c.query(`create publication ${K}_epub for table ${K}_e`)
      try {
        // Collect() and duplicate entries fail on the first pull, before any streaming
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_eslot`, { temporary: true })
          await expect(
            repl.start({ slot: slot.slot, publications: [`${K}_epub`], shapes: [{ table: `${K}_e`, shape: { g: Collect({ id: 'int4' }) as never } }] }).next(),
          ).rejects.toThrow(/Collect\(\) groups/)
          await expect(
            repl.start({ slot: slot.slot, publications: [`${K}_epub`], shapes: [{ table: `${K}_e`, shape: {} }, { table: `${K}_e`, shape: {} }] }).next(),
          ).rejects.toThrow(/duplicate replication shape/)
          // a declared column the live relation doesn't have throws when the relation is announced
          await c.query(`insert into ${K}_e values (1, 'x')`)
          await expect(
            collectUntil(
              repl.start({ slot: slot.slot, publications: [`${K}_epub`], shapes: [{ table: `${K}_e`, shape: { nope: 'text' } }] }),
              (es) => es.some((e) => e.kind === 'commit')),
          ).rejects.toThrow(/declares column "nope" but the relation has: id, name/)
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_epub`)
        await c.query(`drop table ${K}_e`)
      }
    })
  }, TEST_TIMEOUT)

  test("ack() is forgiving: a commit's lsn counts as its endLsn", async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_f(id int4 primary key)`)
      await c.query(`create publication ${K}_fpub for table ${K}_f`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_fslot`, { temporary: true })
          await c.query(`insert into ${K}_f values (1)`)
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_fpub`] }),
            (es) => es.some((e) => e.kind === 'commit'))
          const commit = events.find((e) => e.kind === 'commit') as Extract<ReplicationEvent, { kind: 'commit' }>
          repl.ack(commit.lsn) // the natural-looking WRONG field
          expect(repl.flushedLsn).toBe(commit.endLsn) // normalized — the idle-keepalive gate opens anyway
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_fpub`)
        await c.query(`drop table ${K}_f`)
      }
    })
  }, TEST_TIMEOUT)

  test('server CopyDone surfaces as a ReplicationStreamEnded THROW, never a silent clean end', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_cd(id int4 primary key)`)
      await c.query(`create publication ${K}_cdpub for table ${K}_cd`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_cdslot`, { temporary: true })
          await c.query(`insert into ${K}_cd values (1)`)
          const gen = repl.start({ slot: slot.slot, publications: [`${K}_cdpub`] })
          for (;;) { const r = await gen.next(); if (r.done || (r.value as ReplicationEvent).kind === 'commit') break } // stream live
          // inject a server CopyDone (+ the ReadyForQuery that ends the handshake), as onData would
          const priv = repl as unknown as { q: { type: string; body: Buffer }[]; wake: (() => void) | null }
          priv.q.push({ type: 'c', body: Buffer.alloc(0) }, { type: 'Z', body: Buffer.from('I') })
          const w = priv.wake; priv.wake = null; w?.()
          const err = await gen.next().then(() => null, (e: unknown) => e)
          expect(err).toBeInstanceOf(ReplicationStreamEnded)
          expect((err as ReplicationStreamEnded).reason).toBe('copy-done')
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_cdpub`)
        await c.query(`drop table ${K}_cd`)
      }
    })
  }, TEST_TIMEOUT)

  test('end() and AbortSignal deterministically finish a parked start() iterator', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_e2(id int4 primary key)`)
      await c.query(`create publication ${K}_e2pub for table ${K}_e2`)
      const finishes = (p: Promise<IteratorResult<ReplicationEvent>>, what: string) =>
        Promise.race([p, new Promise<never>((_, rej) => setTimeout(() => rej(new Error(`${what} did not finish the iterator`)), 2000))])
      try {
        // end(): the parked next() finishes NOW, not when the (here: 60s) status timer would fire
        const r1 = await replication(TEST_CONFIG)
        try {
          const s1 = await r1.createSlot(`${K}_e2slot1`, { temporary: true })
          const gen = r1.start({ slot: s1.slot, publications: [`${K}_e2pub`], statusIntervalMs: 60_000 })
          const pending = gen.next() // no traffic -> parks awaiting messages
          await new Promise((r) => setTimeout(r, 100))
          r1.end()
          expect((await finishes(pending, 'end()')).done).toBe(true)
        } finally { r1.end() }

        // AbortSignal: same clean finish + connection close
        const r2 = await replication(TEST_CONFIG)
        try {
          const s2 = await r2.createSlot(`${K}_e2slot2`, { temporary: true })
          const ac = new AbortController()
          const gen2 = r2.start({ slot: s2.slot, publications: [`${K}_e2pub`], statusIntervalMs: 60_000, signal: ac.signal })
          const pending2 = gen2.next()
          await new Promise((r) => setTimeout(r, 100))
          ac.abort()
          expect((await finishes(pending2, 'abort')).done).toBe(true)
        } finally { r2.end() }
      } finally {
        await c.query(`drop publication ${K}_e2pub`)
        await c.query(`drop table ${K}_e2`)
      }
    })
  }, TEST_TIMEOUT)

  test('start({ shapes }): shape keys are OUTPUT keys — columns maps them to SQL names, like a query() shape', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_k(id int4 primary key, big_int_col int8)`)
      await c.query(`create publication ${K}_kpub for table ${K}_k`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_kslot`, { temporary: true })
          await c.query(`insert into ${K}_k values (1, 42)`)
          await c.query(`update ${K}_k set big_int_col = 43 where id = 1`)
          const events = await collectUntil(
            repl.start({
              slot: slot.slot, publications: [`${K}_kpub`],
              shapes: [{ table: `${K}_k`, columns: { bigIntCol: 'big_int_col' }, shape: { id: 'int4', bigIntCol: 'int8:number' } }],
            }),
            (es) => es.filter((e) => e.kind === 'commit').length >= 2)
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.new).toEqual({ id: 1, bigIntCol: 42 }) // the query()-shape row, not { big_int_col }
          const upd = events.find((e) => e.kind === 'update') as Extract<ReplicationEvent, { kind: 'update' }>
          expect(upd.new).toEqual({ id: 1, bigIntCol: 43 })
        } finally { repl.end() }

        // a columns entry whose key isn't in the shape is a typo: fails on the first pull
        const r2 = await replication(TEST_CONFIG)
        try {
          const s2 = await r2.createSlot(`${K}_kslot2`, { temporary: true })
          await expect(
            r2.start({ slot: s2.slot, publications: [`${K}_kpub`], shapes: [{ table: `${K}_k`, columns: { nope: 'id' }, shape: { id: 'int4' } }] }).next(),
          ).rejects.toThrow(/columns maps "nope" but the shape has no such key/)
          // a mapped column missing from the live relation names BOTH the key and the SQL name
          await c.query(`update ${K}_k set big_int_col = 44 where id = 1`)
          await expect(
            collectUntil(
              r2.start({ slot: s2.slot, publications: [`${K}_kpub`], shapes: [{ table: `${K}_k`, columns: { bigIntCol: 'wrong_col' }, shape: { bigIntCol: 'int8' } }] }),
              (es) => es.some((e) => e.kind === 'commit')),
          ).rejects.toThrow(/declares column "bigIntCol" \(-> "wrong_col"\) but the relation has: id, big_int_col/)
        } finally { r2.end() }

        // output-key collision / two keys on one column: loud at relation arrival (fresh conn each — the throw poisons the stream)
        const bad: Array<[TableShape, RegExp]> = [
          [{ table: `${K}_k`, columns: { big_int_col: 'id' }, shape: { big_int_col: 'int4' } }, /output key "big_int_col" collides/],
          [{ table: `${K}_k`, columns: { a: 'id', b: 'id' }, shape: { a: 'int4', b: 'int4:string' } }, /"a" and "b" both map to column "id"/],
        ]
        for (let i = 0; i < bad.length; i++) {
          const r = await replication(TEST_CONFIG)
          try {
            const s = await r.createSlot(`${K}_kslot_e${i}`, { temporary: true })
            await c.query(`update ${K}_k set big_int_col = big_int_col + 1 where id = 1`)
            await expect(
              collectUntil(
                r.start({ slot: s.slot, publications: [`${K}_kpub`], shapes: [bad[i]![0]] }),
                (es) => es.some((e) => e.kind === 'commit')),
            ).rejects.toThrow(bad[i]![1])
          } finally { r.end() }
        }
      } finally {
        await c.query(`drop publication ${K}_kpub`)
        await c.query(`drop table ${K}_k`)
      }
    })
  }, TEST_TIMEOUT)

  test('start({ shapes, binary: true }): declared targets decode from binary tuples; unhonorable targets error loudly', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_sb(id int8 primary key, n numeric, at timestamptz, ns int8[], meta jsonb)`)
      await c.query(`create publication ${K}_sbpub for table ${K}_sb`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_sbslot`, { temporary: true })
          await c.query(`insert into ${K}_sb values (1, '1267650600228229401496703205376', '2024-01-02T03:04:05Z', array[1,2], '{"when":"2024-01-02T03:04:05"}')`)
          const events = await collectUntil(
            repl.start({
              slot: slot.slot, publications: [`${K}_sbpub`], binary: true,
              shapes: [{ table: `${K}_sb`, shape: { id: 'int8:number', n: 'numeric:bigint', at: 'timestamptz:ms', ns: 'int8[]:number', meta: Jsonb({ when: 'timestamp:ms' }) } }],
            }),
            (es) => es.some((e) => e.kind === 'commit'))
          expect(repl.binaryTuples).toBe(true)
          const ins = events.find((e) => e.kind === 'insert') as Extract<ReplicationEvent, { kind: 'insert' }>
          expect(ins.new.id).toBe(1)
          expect(ins.new.n).toBe(1267650600228229401496703205376n)              // BigInt(exact binary numeric render)
          expect(ins.new.at).toBe(Date.UTC(2024, 0, 2, 3, 4, 5))
          expect(ins.new.ns).toEqual([1, 2])                                    // array_recv with :number elements
          expect(ins.new.meta).toEqual({ when: Date.UTC(2024, 0, 2, 3, 4, 5) }) // binary json payload = the json text
        } finally { repl.end() }

        // 'timestamptz:string' promises PG's exact text — binary bytes can't honor it: loud error on arrival
        const repl2 = await replication(TEST_CONFIG)
        try {
          const slot = await repl2.createSlot(`${K}_sbslot2`, { temporary: true })
          await c.query(`update ${K}_sb set n = 2 where id = 1`)
          await expect(
            collectUntil(
              repl2.start({ slot: slot.slot, publications: [`${K}_sbpub`], binary: true, shapes: [{ table: `${K}_sb`, shape: { at: 'timestamptz:string' } }] }),
              (es) => es.some((e) => e.kind === 'commit')),
          ).rejects.toThrow(/no binary decoder/)
        } finally { repl2.end() }
      } finally {
        await c.query(`drop publication ${K}_sbpub`)
        await c.query(`drop table ${K}_sb`)
      }
    })
  }, TEST_TIMEOUT)

  test('start(): a healthy idle stream with server keepalives does NOT trip a 5s receive timeout (idle)', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_idle(id int4 primary key)`)
      await c.query(`create publication ${K}_idlepub for table ${K}_idle`)
      try {
        const repl = await replication({ ...TEST_CONFIG, options: '-c wal_sender_timeout=2000' })
        try {
          const slot = await repl.createSlot(`${K}_idleslot`, { temporary: true })
          const ac = new AbortController()
          setTimeout(() => ac.abort(), 6500)
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_idlepub`], statusIntervalMs: 30_000, receiveTimeoutMs: 5000, signal: ac.signal }),
            () => false,
          )
          expect(events.length).toBe(0)
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_idlepub`)
        await c.query(`drop table ${K}_idle`)
      }
    })
  }, 10_000)

  test('command(): rejects with ReplicationBusy mid-stream; the stream keeps delivering afterward', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_cmd(id int4 primary key)`)
      await c.query(`create publication ${K}_cmdpub for table ${K}_cmd`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_cmdslot`, { temporary: true })
          const gen = repl.start({ slot: slot.slot, publications: [`${K}_cmdpub`] })
          await c.query(`insert into ${K}_cmd values (1)`)
          let r = await gen.next()
          while (!r.done && r.value.kind !== 'commit') r = await gen.next()
          expect(r.done).toBe(false) // suspended at the yield — the generator body is paused mid-stream

          await expect(repl.command('select 1')).rejects.toThrow(ReplicationBusy)

          await c.query(`insert into ${K}_cmd values (2)`)
          r = await gen.next()
          while (!r.done && r.value.kind !== 'commit') r = await gen.next()
          expect(r.done).toBe(false) // the second insert's commit still arrives — the rejected command() didn't kill the stream

          await gen.return(undefined)
          await repl.command('select 1') // resolves once more — the flag cleared when the consumer stopped
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_cmdpub`)
        await c.query(`drop table ${K}_cmd`)
      }
    })
  }, TEST_TIMEOUT)

  test('command(): a failed start() setup clears the streaming flag — identify() works after', async () => {
    const repl2 = await replication(TEST_CONFIG)
    try {
      const gen = repl2.start({ slot: 'no_such_slot_02x', publications: [`${K}_nosuchpub`] })
      await expect(gen.next()).rejects.toThrow(PgError)
      const sys = await repl2.identify()
      expect(sys.timeline).toBeGreaterThan(0)
    } finally { repl2.end() }
  }, TEST_TIMEOUT)
})

// keep the import used even if helpers change
void connect

  test('config.options reaches the replication startup packet (wal_sender_timeout lever)', async () => {
    const repl = await replication({ ...TEST_CONFIG, options: '-c wal_sender_timeout=54321' })
    try {
      const r = await repl.command('show wal_sender_timeout')
      expect(r.rows[0]![0]).toBe('54321ms') // server-applied at startup; RESET ALL would restore it
    } finally { repl.end() }
  }, TEST_TIMEOUT)

  test('onReady fires when START_REPLICATION is accepted, before any event', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_r2(id int4 primary key)`)
      await c.query(`create publication ${K}_r2pub for table ${K}_r2`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_r2slot`, { temporary: true })
          await c.query(`insert into ${K}_r2 values (1)`)
          const order: string[] = []
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_r2pub`], onReady: () => order.push('ready') }),
            (es) => { if (es.length === 1) order.push('first-event'); return es.some((e) => e.kind === 'commit') })
          expect(order).toEqual(['ready', 'first-event']) // established BEFORE anything yields
          expect(events.length).toBeGreaterThan(0)
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_r2pub`)
        await c.query(`drop table ${K}_r2`)
      }
    })
  }, TEST_TIMEOUT)

  test('TOAST fill: with REPLICA IDENTITY FULL, unchanged columns are filled into new (and still listed)', async () => {
    await withConn(async (c) => {
      await c.query(`create table ${K}_t2(id int4 primary key, fat text, n int4)`)
      await c.query(`alter table ${K}_t2 replica identity full`)
      await c.query(`create publication ${K}_t2pub for table ${K}_t2`)
      try {
        const repl = await replication(TEST_CONFIG)
        try {
          const slot = await repl.createSlot(`${K}_t2slot`, { temporary: true })
          // incompressible-enough payload so it TOASTs out-of-line (>2KB post-compression)
          await c.query(`insert into ${K}_t2 select 1, string_agg(md5(random()::text), ''), 0 from generate_series(1, 2000)`)
          await c.query(`update ${K}_t2 set n = 7 where id = 1`) // fat untouched -> pgoutput omits it ('u')
          const events = await collectUntil(
            repl.start({ slot: slot.slot, publications: [`${K}_t2pub`] }),
            (es) => es.filter((e) => e.kind === 'commit').length >= 2)
          const upd = events.find((e) => e.kind === 'update') as Extract<ReplicationEvent, { kind: 'update' }>
          expect(upd.oldKind).toBe('full')
          expect(upd.unchanged).toEqual(['fat'])                       // still listed: filled ≠ retransmitted
          expect(typeof upd.new.fat).toBe('string')                    // FILLED from the old tuple
          expect((upd.new.fat as string).length).toBe(64000)           // the whole 2000×32-char value
          expect(upd.new.fat).toBe(upd.old!.fat)                       // lossless by definition of "unchanged"
          expect(upd.new.n).toBe(7)
        } finally { repl.end() }
      } finally {
        await c.query(`drop publication ${K}_t2pub`)
        await c.query(`drop table ${K}_t2`)
      }
    })
  }, TEST_TIMEOUT)

  test('channel_binding=require throws loudly on replication connections too', async () => {
    const { host, port, user, password, database } = TEST_CONFIG as { host: string; port: number; user: string; password: string; database: string }
    const url = `postgres://${user}:${password}@${host}:${port}/${database}?channel_binding=require`
    await expect(replication(url)).rejects.toThrow(/channel_binding=require needs TLS/)
  }, TEST_TIMEOUT)
