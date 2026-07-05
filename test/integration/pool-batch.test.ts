// Domain: "pool lazy query API" — pool.query() is a lazy thenable, pool.execute() is eager, and
// pool.batch() runs a set pipelined (one connection) or concurrently (fan-out). Read-only SELECTs.
import { test, expect, describe } from 'bun:test'
import { testPool } from '../helpers/db.ts'

const cell = (r: { rows: unknown[] }, k: string) => (r.rows[0] as Record<string, unknown>)[k]

describe('pool.query is lazy; pool.execute is eager', () => {
  test('pool.query() does not run until awaited — no connection acquired before then', async () => {
    const pool = testPool()
    try {
      const q = pool.query('select 1 as n', [], { mode: 'object' })
      expect(pool.size).toBe(0)            // lazy: nothing checked out yet
      const r = await q                     // awaiting forces execution
      expect(cell(r, 'n')).toBe(1)
      expect(pool.size).toBe(1)            // one connection opened by the await
      expect(await q).toBe(r)              // memoized: awaiting again runs it once (same result object)
    } finally { await pool.end() }
  })

  test('pool.execute() runs now and returns a Promise', async () => {
    const pool = testPool()
    try {
      const p = pool.execute('select 42 as n', [], { mode: 'object' })
      expect(p).toBeInstanceOf(Promise)
      expect(cell(await p, 'n')).toBe(42)
    } finally { await pool.end() }
  })
})

describe('pool.batch', () => {
  test("default ('pipelined') runs the set on ONE connection, results in input order", async () => {
    const pool = testPool()
    try {
      const rs = await pool.batch([
        pool.query('select pg_backend_pid() as pid, $1::int as n', [0], { mode: 'object' }),
        pool.query('select pg_backend_pid() as pid, $1::int as n', [1], { mode: 'object' }),
        pool.query('select pg_backend_pid() as pid, $1::int as n', [2], { mode: 'object' }),
      ])
      rs.forEach((r, i) => expect(cell(r, 'n')).toBe(i))                 // order preserved
      expect(new Set(rs.map((r) => cell(r, 'pid'))).size).toBe(1)       // all on one backend -> pipelined
      expect(pool.size).toBe(1)                                          // reserved a single connection
    } finally { await pool.end() }
  })

  test("'concurrently' fans the set out across connections", async () => {
    const pool = testPool({ max: 5 })
    try {
      const rs = await pool.batch('concurrently', [
        pool.query('select $1::int as n', [0], { mode: 'object' }),
        pool.query('select $1::int as n', [1], { mode: 'object' }),
        pool.query('select $1::int as n', [2], { mode: 'object' }),
      ])
      rs.forEach((r, i) => expect(cell(r, 'n')).toBe(i))                 // order preserved
      expect(pool.size).toBeGreaterThan(1)                               // opened multiple connections (fan-out)
    } finally { await pool.end() }
  })
})
