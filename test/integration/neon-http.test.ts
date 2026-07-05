// minipg/neon-http — real Neon SQL-over-HTTP test. Verifies the fetch transport + the raw-response
// byte-scan → synthetic DataRow → shared compiled-mapper decode path (full type fidelity with no
// JSON.parse of the hot data), plus atomic batch transactions and PgError mapping.
//
// Needs a live Neon endpoint. Set NEON_HTTP_URL to a `postgresql://…neon.tech/db?sslmode=require` string:
//   NEON_HTTP_URL='postgresql://user:pass@ep-xxx.region.aws.neon.tech/db?sslmode=require' bun run test:neon-http
// Skips entirely when unset. Uses uniquely-named real tables (HTTP has no cross-request session, so temp
// tables don't persist) and drops them in afterAll.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, PgError } from '../../src/neon-http.ts'
import type { NeonHttpClient } from '../../src/neon-http.ts'

const URL = process.env.NEON_HTTP_URL
const d = describe.skipIf(!URL)
const TBL = 'minipg_http_test'

d('minipg/neon-http over the Neon SQL-over-HTTP endpoint', () => {
  let db: NeonHttpClient
  beforeAll(async () => { db = await connect(URL!) })
  afterAll(async () => { await db?.query(`drop table if exists ${TBL}`).catch(() => {}); await db?.end() })

  test('scalars, params, and core type decoders', async () => {
    const r = await db.query('select 1 as n, $1::text as t, (2*1.5)::float8 as f, true as b', ['hi'], { mode: 'object' })
    expect(r.rows[0]).toEqual({ n: 1, t: 'hi', f: 3, b: true })
    expect(r.columns).toEqual(['n', 't', 'f', 'b'])
    expect(r.command).toBe('SELECT')
    expect(r.rowCount).toBe(1)
  })

  test('precision-safe decode via the shared mapper: int8->BigInt, numeric->string, json->object, ts->Date', async () => {
    const r = await db.query('select $1::int8 as big, $2::numeric as num, $3::json as j, now()::timestamptz as ts, null::text as nul',
      ['9223372036854775807', '10.50', JSON.stringify({ a: 1 })], { mode: 'object' })
    const row = r.rows[0] as Record<string, unknown>
    expect(row.big).toBe(9223372036854775807n)
    expect(row.num).toBe('10.50')
    expect(row.j).toEqual({ a: 1 })
    expect(row.ts).toBeInstanceOf(Date)
    expect(row.nul).toBeNull()
  })

  test('array mode (default), multi-row, and escaped text/jsonb (byte-scan slow path)', async () => {
    const r = await db.query(`select g, 'a,b"c\\d' as tricky, json_build_object('k', g) as j from generate_series(1,3) g`)
    expect(r.rows.length).toBe(3)
    expect((r.rows[0] as unknown[])[0]).toBe(1)
    expect((r.rows[0] as unknown[])[1]).toBe('a,b"c\\d')
    expect((r.rows[2] as unknown[])[2]).toEqual({ k: 3 })
  })

  test('larger result over the raw-scan path keeps bigints exact', async () => {
    const r = await db.query('select g, (g::int8 * 1000000007) as big from generate_series(1,1000) g', [], { mode: 'object' })
    expect(r.rows.length).toBe(1000)
    expect((r.rows[999] as { big: bigint }).big).toBe(1000000007000n)
  })

  test('errors surface as PgError with SQLSTATE', async () => {
    let err: unknown
    try { await db.query('select * from a_table_that_does_not_exist') } catch (e) { err = e }
    expect(err).toBeInstanceOf(PgError)
    expect((err as PgError).code).toBe('42P01')
  })

  test('atomic transaction batch returns results in order', async () => {
    const res = await db.transaction([
      { sql: 'select $1::int as a', params: [1], mode: 'object' },
      { sql: `select 'x' as s, $1::int8 as big`, params: ['42'], mode: 'object' },
    ])
    expect(res.length).toBe(2)
    expect(res[0]!.rows[0] as unknown as Record<string, unknown>).toEqual({ a: 1 })
    expect((res[1]!.rows[0] as unknown as Record<string, unknown>).big).toBe(42n)
  })

  test('transaction is atomic: an error rolls the whole batch back', async () => {
    await db.query(`drop table if exists ${TBL}`)
    await db.query(`create table ${TBL}(id int)`)
    await expect(db.transaction([
      { sql: `insert into ${TBL} values (1)` },
      { sql: `insert into ${TBL} values (not_a_column)` }, // errors -> rollback
    ])).rejects.toBeInstanceOf(PgError)
    const n = await db.query(`select count(*)::int as n from ${TBL}`, [], { mode: 'object' })
    expect((n.rows[0] as { n: number }).n).toBe(0)
  })

  test('interactive transaction(fn) is rejected with a clear message', async () => {
    // @ts-expect-error — fn form is intentionally unsupported over stateless HTTP
    await expect(db.transaction(async () => {})).rejects.toThrow(/not available over stateless HTTP/)
  })

  test('buffer mode returns raw text cells as Buffers', async () => {
    const r = await db.query('select 1::int4 as x', [], { mode: 'buffer' })
    const cell = (r.rows[0] as unknown[])[0]
    expect(Buffer.isBuffer(cell)).toBe(true)
    expect(String(cell)).toBe('1')
  })
})
