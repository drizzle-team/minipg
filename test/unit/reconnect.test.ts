// Graceful-reconnect behavior, driven against the in-process MockPgServer — no real PG.
// Covers: single-flight reconnect (no herd), dead-idle eviction, fatal vs recoverable,
// acquire timeout, in-flight reject (no replay), and end() during an outage.
import { test, expect, describe } from 'bun:test'
import { connect, createPool, PgError } from '../../src/index.ts'
import { MockPgServer } from '../mock/server.ts'

const tick = (ms = 20) => new Promise((r) => setTimeout(r, ms))
const OK = () => ({ fields: [{ name: 'n', oid: 23 }], rows: [['1']] as string[][], command: 'SELECT' })
async function withMock<T>(fn: (m: MockPgServer) => Promise<T>): Promise<T> {
  const m = await MockPgServer.start({ onQuery: OK })
  try { return await fn(m) } finally { await m.close() }
}

describe('pool: single-flight reconnect', () => {
  test('after a restart, ONE probe reconnects and the whole herd is released', async () => {
    await withMock(async (m) => {
      const pool = createPool(m.connectConfig({ reconnect: { baseMs: 60, maxMs: 120, acquireTimeoutMs: 5000 } }))
      expect((await pool.query('select 1')).rows.length).toBe(1) // warm up one connection

      m.killActive(); m.setAvailable(false) // database restarts
      await tick(30)

      const first = pool.execute('select 1') // eager: this acquire trips the breaker (lazy query() wouldn't run yet)
      await tick(50)
      expect(pool.isDown).toBe(true)

      const before = m.connectAttempts
      const burst = Promise.all(Array.from({ length: 8 }, () => pool.query('select 1'))) // all wait on the probe
      await tick(250) // several backoff rounds elapse while down
      const attemptsWhileDown = m.connectAttempts - before
      expect(attemptsWhileDown).toBeLessThan(6) // single-flight: only the probe tried, NOT 9 waiters x rounds

      m.setAvailable(true) // database is back
      const results = await Promise.all([first, ...(await burst.then((r) => r))])
      expect(results.every((r) => r.rows.length === 1)).toBe(true) // everyone recovered
      expect(pool.isDown).toBe(false)
      await pool.end()
    })
  }, 15000)

  test('a connection that died while idle is evicted on the next acquire', async () => {
    await withMock(async (m) => {
      const pool = createPool(m.connectConfig())
      await pool.query('select 1')
      expect(pool.idleCount).toBe(1)
      m.killActive() // kill the idle connection (server still up)
      await tick(40)
      const r = await pool.query('select 1') // must drop the dead one and open a fresh one
      expect(r.rows.length).toBe(1)
      await pool.end()
    })
  })
})

describe('pool: recoverable vs fatal', () => {
  test('a prolonged outage makes acquire time out (no hang)', async () => {
    await withMock(async (m) => {
      const pool = createPool(m.connectConfig({ reconnect: { baseMs: 40, acquireTimeoutMs: 150 } }))
      m.setAvailable(false)
      const err = await pool.query('select 1').catch((e) => e)
      expect(err).toBeInstanceOf(Error)
      expect((err as Error).message).toMatch(/timed out/)
      await pool.end()
    })
  }, 10000)

  test('an unrecoverable startup error (auth) breaks the pool — no endless probing', async () => {
    await withMock(async (m) => {
      m.failStartupWith('28P01')
      const pool = createPool(m.connectConfig({ reconnect: { baseMs: 30 } }))
      const err = await pool.query('select 1').catch((e) => e)
      expect(err).toBeInstanceOf(PgError)
      expect((err as PgError).code).toBe('28P01')
      const before = m.connectAttempts
      await tick(150)
      expect(m.connectAttempts - before).toBe(0) // does not probe a fatal error
      const err2 = await pool.query('select 1').catch((e) => e)
      expect((err2 as PgError).code).toBe('28P01') // fast-fails as broken
      await pool.end()
    })
  })
})

describe('pool: safety', () => {
  test('a mid-query connection loss rejects and is never silently replayed', async () => {
    await withMock(async (m) => {
      const pool = createPool(m.connectConfig())
      m.setMode('fatal-then-close')
      const err = await pool.query('select 1').catch((e) => e)
      expect(err).toBeInstanceOf(PgError)
      expect((err as PgError).code).toBe('57P01') // surfaced to the caller, not hidden+retried
      m.setMode('normal')
      const ok = await pool.query('select 1') // a NEW query works once the server is healthy
      expect(ok.rows.length).toBe(1)
      await pool.end()
    })
  })

  test('end() during an outage rejects waiting acquirers (no hang)', async () => {
    await withMock(async (m) => {
      const pool = createPool(m.connectConfig({ reconnect: { baseMs: 50, acquireTimeoutMs: 5000 } }))
      m.setAvailable(false)
      const waiting = pool.query('select 1').catch((e) => e) // trips down, then waits
      await tick(70)
      await pool.end()
      expect(await waiting).toBeInstanceOf(Error)
    })
  }, 10000)
})

describe('single connection: durable reconnect (opt-in)', () => {
  test('reconnects after a drop and runs queries enqueued while down', async () => {
    await withMock(async (m) => {
      const c = await connect(m.connectConfig({ reconnect: { baseMs: 40, maxMs: 80 } }))
      expect((await c.query('select 1')).rows.length).toBe(1)
      m.killActive() // drop; server still up -> auto-reconnect
      await tick(20)
      expect(c.state).toBe('reconnecting')
      const q = c.query('select 1') // enqueued during reconnect -> runs after recovery
      expect((await q).rows.length).toBe(1)
      expect(c.state).toBe('ready')
      await c.end()
    })
  }, 10000)

  test('in-flight query rejects on drop (never replayed); connection then recovers', async () => {
    await withMock(async (m) => {
      const c = await connect(m.connectConfig({ reconnect: { baseMs: 40 } }))
      m.setMode('fatal-then-close')
      const err = await c.query('select 1').catch((e) => e)
      expect(err).toBeInstanceOf(PgError)
      expect((err as PgError).code).toBe('57P01')
      m.setMode('normal')
      await tick(90) // auto-reconnect
      expect((await c.query('select 1')).rows.length).toBe(1)
      expect(c.state).toBe('ready')
      await c.end()
    })
  }, 10000)

  test('prepared-statement cache is cleared on reconnect (no 26000 on reuse)', async () => {
    await withMock(async (m) => {
      const c = await connect(m.connectConfig({ reconnect: { baseMs: 40 } }))
      await c.query('select $1::int', [1], { name: 'p1' }) // Parse + cache server-side
      m.killActive() // restart loses the server-side statement
      await tick(90) // reconnect clears the client cache
      const r = await c.query('select $1::int', [2], { name: 'p1' }) // re-Parses -> must NOT be 26000
      expect(r.rows.length).toBe(1)
      expect(c.state).toBe('ready')
      await c.end()
    })
  }, 10000)

  test('end() during a failing reconnect settles pending and stops retrying', async () => {
    await withMock(async (m) => {
      const c = await connect(m.connectConfig({ reconnect: { baseMs: 40 } }))
      m.killActive(); m.setAvailable(false) // drop, server stays down
      await tick(20)
      const pending = c.query('select 1').catch((e) => e)
      await tick(30)
      await c.end()
      expect(await pending).toBeInstanceOf(Error)
      expect(c.state).toBe('closed')
    })
  }, 10000)

  test('an auth failure during reconnect gives up (no endless retry)', async () => {
    await withMock(async (m) => {
      const c = await connect(m.connectConfig({ reconnect: { baseMs: 30 } }))
      m.setMode('hang')
      const pending = c.query('select 1').catch((e) => e) // hangs -> reliably in-flight
      await tick(20)
      m.failStartupWith('28P01'); m.killActive() // drop; every reconnect now hits 28P01
      expect(await pending).toBeInstanceOf(Error) // in-flight rejected (no replay)
      await tick(90)
      expect(c.state).toBe('closed') // gave up on the fatal auth error
      const a = m.connectAttempts
      await tick(90)
      expect(m.connectAttempts).toBe(a) // and stopped retrying
      await c.end()
    })
  }, 10000)
})
