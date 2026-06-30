// Cancellation against REAL PostgreSQL — proves the out-of-band CancelRequest path
// actually stops a running query and that the connection recovers afterward.
import { test, expect, describe } from 'bun:test'
import { testConnect, caught } from '../helpers/db.ts'

describe('cancellation (real PostgreSQL)', () => {
  test('a per-query timeout cancels a long-running query; the connection then recovers', async () => {
    const c = await testConnect()
    const t0 = Date.now()
    const err = await caught(() => c.query('select pg_sleep(5)', [], { timeout: 250 }))
    expect((err as { code?: string }).code).toBe('QUERY_TIMEOUT')
    expect(Date.now() - t0).toBeLessThan(3000) // did NOT wait the full 5s
    const ok = await c.query('select 7') // recovered after the real server-side cancel
    expect((ok.rows[0] as unknown[])[0]).toBe(7)
    await c.end()
  }, 10000)

  test('AbortSignal cancels a long-running query', async () => {
    const c = await testConnect()
    const ac = new AbortController()
    const p = caught(() => c.query('select pg_sleep(5)', [], { signal: ac.signal }))
    setTimeout(() => ac.abort(), 150)
    expect(await p).toBeInstanceOf(Error)
    const ok = await c.query('select 1') // connection still usable
    expect((ok.rows[0] as unknown[])[0]).toBe(1)
    await c.end()
  }, 10000)
})
