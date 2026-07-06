// COPY FROM STDIN: copyFrom (raw source, simple protocol, solo gating) and copyMany
// (binary/text row encoding). Requires `bun run test:setup`.
import { test, expect, describe } from 'bun:test'
import { withConn, testPool, caught, PgError, TEST_TIMEOUT } from '../helpers/db.ts'

const K = `cp_${process.pid}`
const COLS = { id: 'int8', name: 'text', qty: 'int4', price: 'float8', ok: 'bool', at: 'timestamptz' } as const
const DDL = (t: string) => `create temp table ${t}(id int8, name text, qty int4, price float8, ok bool, at timestamptz)`
const mkRow = (i: number): unknown[] => [
  i + 1, `n_${i}\ttab "q" \\s`, i % 100, i + 0.25, i % 2 === 0, new Date(Date.UTC(2026, 0, 1) + i * 1000),
]

describe('copyFrom (raw source)', () => {
  test('text chunks with boundaries mid-row; resolves with COPY row count', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_raw(a int4, b text)`)
      const payload = '1\tone\n2\ttwo\n3\tthree\n'
      const chunks = [payload.slice(0, 5), payload.slice(5, 11), payload.slice(11)] // split mid-row on purpose
      const r = await c.copyFrom(`copy ${K}_raw from stdin`, chunks)
      expect(r.command).toBe('COPY')
      expect(r.rowCount).toBe(3)
      const got = await c.query(`select a, b from ${K}_raw order by a`)
      expect(got.rows).toEqual([[1, 'one'], [2, 'two'], [3, 'three']] as never)
    })
  }, TEST_TIMEOUT)

  test('async generator source (backpressure-friendly) works', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_gen(a int4)`)
      async function* gen(): AsyncGenerator<string> {
        for (let i = 0; i < 100; i++) { if (i % 25 === 0) await new Promise((r) => setTimeout(r, 1)); yield `${i}\n` }
      }
      const r = await c.copyFrom(`copy ${K}_gen from stdin`, gen())
      expect(r.rowCount).toBe(100)
    })
  }, TEST_TIMEOUT)

  test('bad payload: server error rejects, connection stays usable (simple-protocol recovery)', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_bad(a int4)`)
      const e = (await caught(() => c.copyFrom(`copy ${K}_bad from stdin`, ['not-a-number\n']))) as PgError
      expect(e.code).toBe('22P02')
      const ok = await c.query('select 41 + 1')
      expect((ok.rows[0] as unknown[])[0]).toBe(42)
    })
  }, TEST_TIMEOUT)

  test('source throwing mid-copy sends CopyFail; rejection carries the reason; connection survives', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_thr(a int4)`)
      function* boom(): Generator<string> { yield '1\n'; throw new Error('upstream exploded') }
      const e = (await caught(() => c.copyFrom(`copy ${K}_thr from stdin`, boom()))) as PgError
      expect(e.code).toBe('57014') // query_canceled: "COPY from stdin failed: …"
      expect(String(e.message)).toContain('upstream exploded')
      const n = await c.query(`select count(*)::int4 from ${K}_thr`)
      expect((n.rows[0] as unknown[])[0]).toBe(0) // all-or-nothing
    })
  }, TEST_TIMEOUT)

  test('COPY runs SOLO: pipelined queries around it stay correct and ordered', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_solo(a int4)`)
      const before = c.query('select 1 as v')
      const copy = c.copyFrom(`copy ${K}_solo from stdin`, ['10\n20\n'])
      const after = c.query(`select count(*)::int4 from ${K}_solo`)
      const [b, cp, a] = await Promise.all([before, copy, after])
      expect((b.rows[0] as unknown[])[0]).toBe(1)
      expect(cp.rowCount).toBe(2)
      expect((a.rows[0] as unknown[])[0]).toBe(2) // dispatched only after the copy finished
    })
  }, TEST_TIMEOUT)

  test("plain query('COPY … FROM STDIN') without a source still fails fast and cleanly", async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_plain(a int4)`)
      const e = (await caught(() => c.query(`copy ${K}_plain from stdin`))) as PgError
      expect(e.code).toBe('57014')
      const ok = await c.query('select 7 as v')
      expect((ok.rows[0] as unknown[])[0]).toBe(7)
    })
  }, TEST_TIMEOUT)
})

describe('copyMany', () => {
  test('binary format roundtrips identically to per-row binary-param inserts', async () => {
    await withConn(async (c) => {
      await c.query(DDL(`${K}_bin`))
      const rows = Array.from({ length: 1000 }, (_, i) => mkRow(i))
      const r = await c.copyMany(`${K}_bin`, COLS, rows)
      expect(r.rowCount).toBe(1000)
      await c.insertMany(`${K}_bin`, COLS, rows.slice(0, 100)) // binary array params as the reference encoding
      const chk = await c.query(`select count(*)::int4, count(distinct (id,name,qty,price,ok,at))::int4 from ${K}_bin`)
      expect((chk.rows[0] as unknown[])[0]).toBe(1100)
      expect((chk.rows[0] as unknown[])[1]).toBe(1000) // the re-inserted 100 matched byte-for-byte
    })
  }, TEST_TIMEOUT)

  test('forced text format stores the same values as binary', async () => {
    await withConn(async (c) => {
      await c.query(DDL(`${K}_txt`))
      const rows = Array.from({ length: 200 }, (_, i) => mkRow(i))
      await c.copyMany(`${K}_txt`, COLS, rows, { format: 'text' })
      await c.copyMany(`${K}_txt`, COLS, rows) // binary
      const chk = await c.query(`select count(*)::int4, count(distinct (id,name,qty,price,ok,at))::int4 from ${K}_txt`)
      expect((chk.rows[0] as unknown[])[0]).toBe(400)
      expect((chk.rows[0] as unknown[])[1]).toBe(200)
    })
  }, TEST_TIMEOUT)

  test('numeric column auto-selects text format; NULLs and records work', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_num(id int4, amount numeric, note text)`)
      const r = await c.copyMany(`${K}_num`, { id: 'int4', amount: 'numeric', note: 'text' }, [
        { id: 1, amount: '12345678901234567890.42', note: null },
        { id: 2, amount: null, note: 'n' },
      ])
      expect(r.rowCount).toBe(2)
      const got = await c.query(`select amount::text, note from ${K}_num order by id`)
      expect((got.rows[0] as unknown[])[0]).toBe('12345678901234567890.42')
      expect((got.rows[0] as unknown[])[1]).toBeNull()
      expect((got.rows[1] as unknown[])[0]).toBeNull()
    })
  }, TEST_TIMEOUT)

  test('binary mismatch rejects client-side with row/column named', async () => {
    await withConn(async (c) => {
      await c.query(DDL(`${K}_mis`))
      const rows = [mkRow(0), [2, 'ok', 'NaN-qty' as never, 1.5, true, new Date()]]
      const e = (await caught(() => c.copyMany(`${K}_mis`, COLS, rows))) as Error
      expect(String(e.message)).toMatch(/row 1, column "qty"/)
      const n = await c.query(`select count(*)::int4 from ${K}_mis`)
      expect((n.rows[0] as unknown[])[0]).toBe(0)
    })
  }, TEST_TIMEOUT)

  test('chunked COPY, atomic default: a bad chunk rolls back ALL chunks', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_ck(id int8 primary key, name text, qty int4, price float8, ok bool, at timestamptz)`)
      const rows = Array.from({ length: 250 }, (_, i) => mkRow(i))
      rows[205] = mkRow(3) // duplicate PK in the 3rd COPY chunk
      const e = (await caught(() => c.copyMany(`${K}_ck`, COLS, rows, { chunk: 100 }))) as PgError
      expect(e.code).toBe('23505')
      const n = await c.query(`select count(*)::int4 from ${K}_ck`)
      expect((n.rows[0] as unknown[])[0]).toBe(0) // wrapping transaction rolled chunks 1+2 back
      const ok = await c.copyMany(`${K}_ck`, COLS, rows.slice(0, 150), { chunk: 100 }) // connection + path still healthy
      expect(ok.rowCount).toBe(150)
    })
  }, TEST_TIMEOUT)

  test('chunked COPY, atomic:false (WAL-friendly): prior chunks stay, error carries insertedRows', async () => {
    await withConn(async (c) => {
      await c.query(`create temp table ${K}_wf(id int8 primary key, name text, qty int4, price float8, ok bool, at timestamptz)`)
      const rows = Array.from({ length: 250 }, (_, i) => mkRow(i))
      rows[205] = mkRow(3)
      const e = (await caught(() => c.copyMany(`${K}_wf`, COLS, rows, { chunk: 100, atomic: false }))) as PgError & { insertedRows?: number }
      expect(e.code).toBe('23505')
      expect(e.insertedRows).toBe(200)
      const n = await c.query(`select count(*)::int4 from ${K}_wf`)
      expect((n.rows[0] as unknown[])[0]).toBe(200)
    })
  }, TEST_TIMEOUT)

  test('empty rows is a COPY 0 no-op; pool.copyMany works', async () => {
    const pool = testPool({ max: 2 })
    try {
      await pool.execute(`create table ${K}_p(id int8, name text, qty int4, price float8, ok bool, at timestamptz)`)
      const empty = await pool.copyMany(`${K}_p`, COLS, [])
      expect(empty.rowCount).toBe(0)
      const r = await pool.copyMany(`${K}_p`, COLS, Array.from({ length: 50 }, (_, i) => mkRow(i)))
      expect(r.rowCount).toBe(50)
    } finally {
      await pool.execute(`drop table if exists ${K}_p`)
      await pool.end()
    }
  }, TEST_TIMEOUT)
})
