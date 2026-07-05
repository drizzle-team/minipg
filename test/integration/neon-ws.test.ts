// minipg/neon-ws — real Neon WebSocket-proxy transport test. Verifies the pieces our other transports
// can't: connecting a `wss://<host>/v2` WebSocket, the WebSocket->Duplex bridge, SCRAM-SHA-256 auth over
// the tunnel (no channel binding), and full protocol parity (prepared statements, pipelining, transactions).
//
// Needs a live Neon endpoint. Set NEON_WS_URL to a `postgresql://…neon.tech/db?sslmode=require` string:
//   NEON_WS_URL='postgresql://user:pass@ep-xxx.region.aws.neon.tech/db?sslmode=require' bun run test:neon-ws
// Skips entirely when unset, so it never breaks the default `bun test`.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, createPool } from '../../src/neon-ws.ts'
import type { Connection, Pool } from '../../src/neon-ws.ts'

const URL = process.env.NEON_WS_URL
const d = describe.skipIf(!URL)

d('minipg/neon-ws over the Neon WebSocket proxy', () => {
  let c: Connection
  beforeAll(async () => { c = await connect(URL!) })
  afterAll(async () => { await c?.end() })

  test('connects + authenticates (SCRAM over the tunnel) and reports server params', () => {
    expect(c.state).toBe('ready')
    expect(c.serverParams.server_version).toBeTruthy()
  })

  test('scalars, params, and core type decoders', async () => {
    const r = await c.query('select 1 as n, $1::text as t, (2 * 1.5)::float8 as f, true as b', ['hi'], { mode: 'object' })
    expect(r.rows[0]).toEqual({ n: 1, t: 'hi', f: 3, b: true })
    expect(r.columns).toEqual(['n', 't', 'f', 'b'])
    expect(r.command).toBe('SELECT')
    expect(r.rowCount).toBe(1)
  })

  test('precision-safe defaults: int8->BigInt, numeric->string, json->object, timestamptz->Date', async () => {
    const r = await c.query('select $1::int8 as big, $2::numeric as num, $3::json as j, now()::timestamptz as ts',
      ['9223372036854775807', '10.50', JSON.stringify({ a: 1 })], { mode: 'object' })
    const row = r.rows[0] as Record<string, unknown>
    expect(row.big).toBe(9223372036854775807n)
    expect(row.num).toBe('10.50')
    expect(row.j).toEqual({ a: 1 })
    expect(row.ts).toBeInstanceOf(Date)
  })

  test('large result streams through the WS bridge (chunk reassembly)', async () => {
    const r = await c.query('select g from generate_series(1, 1000) g')
    expect(r.rows.length).toBe(1000)
    expect(r.rows[0]).toEqual([1])
    expect(r.rows[999]).toEqual([1000])
  })

  test('server-side prepared statement reuse', async () => {
    const a = await c.query('select $1::int as x', [1], { name: 'nws_ps', mode: 'object', debug: true })
    const b = await c.query('select $1::int as x', [2], { name: 'nws_ps', mode: 'object', debug: true })
    expect(a.debug?.reusedPreparedStatement).toBe(false)
    expect(b.debug?.reusedPreparedStatement).toBe(true)
    expect((b.rows[0] as { x: number }).x).toBe(2)
  })

  test('pipelining engages on one connection (Promise.all -> maxInflight > 1)', async () => {
    const rs = await Promise.all([
      c.query('select 1 as a'), c.query('select 2 as a'), c.query('select 3 as a'),
      c.query('select pg_sleep(0)'), c.query('select 4 as a'),
    ])
    expect(rs.length).toBe(5)
    expect(c.stats.maxInflight).toBeGreaterThan(1)
  })

  test('async iteration (stream)', async () => {
    let seen = 0
    for await (const _row of c.stream('select g from generate_series(1, 250) g')) seen++
    expect(seen).toBe(250)
  })

  test('errors surface as PgError with SQLSTATE', async () => {
    let code: string | undefined
    try { await c.query('select * from a_table_that_does_not_exist') } catch (e) { code = (e as { code?: string }).code }
    expect(code).toBe('42P01')
  })

  test('transaction commit + rollback (connection-scoped temp table)', async () => {
    await c.begin(async (tx) => {
      await tx.query('create temp table nws_tx (id serial primary key, name text not null) on commit drop')
      await tx.query('insert into nws_tx (name) values ($1), ($2)', ['alice', 'bob'])
      const sel = await tx.query('select count(*)::int as n from nws_tx', [], { mode: 'object' })
      expect((sel.rows[0] as { n: number }).n).toBe(2)
    })
    // separate transaction so the ON COMMIT DROP temp table is gone; use a fresh temp table for rollback
    await expect(c.begin(async (tx) => {
      await tx.query('create temp table nws_rb (id int) on commit drop')
      await tx.query('insert into nws_rb values (1)')
      throw new Error('boom')
    })).rejects.toThrow('boom')
    // connection is usable after a rolled-back transaction
    const ok = await c.query('select 1 as ok', [], { mode: 'object' })
    expect((ok.rows[0] as { ok: number }).ok).toBe(1)
  })

  test('pool: query, parallel fan-out, and transaction', async () => {
    const pool: Pool = createPool(URL!)
    try {
      const r = await pool.query('select 42 as answer', [], { mode: 'object' })
      expect((r.rows[0] as unknown as { answer: number }).answer).toBe(42)

      const many = await Promise.all([1, 2, 3].map((i) => pool.query('select $1::int as i', [i], { mode: 'object' })))
      expect(many.map((m) => (m.rows[0] as unknown as { i: number }).i)).toEqual([1, 2, 3])

      const tx = await pool.begin(async (client) => (await client.query('select 7 as x', [], { mode: 'object' })).rows[0])
      expect((tx as { x: number }).x).toBe(7)
    } finally {
      await pool.end()
      expect(pool.size).toBe(0)
    }
  })
})
