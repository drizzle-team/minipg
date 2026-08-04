// minipg/http — the httpostgres gateway client. No gateway needed here: mode:'wire' against the
// real cluster produces EXACTLY the bytes the gateway would forward (that is the whole protocol),
// so a stub fetch serving wire captures gives end-to-end tests over real backend frames.
import { test, expect, describe } from 'bun:test'
import { httpPool, PgError } from '../../src/http.ts'
import { Parser } from '../../src/protocol.ts'
import { testConnect, caught, TEST_TIMEOUT } from '../helpers/db.ts'

// capture: each sql as its own wire result, concatenated — the gateway's N-statement body
async function wireBody(...sqls: string[]): Promise<Buffer> {
  const c = await testConnect()
  try {
    const parts: Buffer[] = []
    for (const sql of sqls) for (const f of await c.query(sql, [], { mode: 'wire' })) parts.push(Buffer.from(f))
    return Buffer.concat(parts)
  } finally { c.end() }
}
const frame = (tag: string, body: Buffer): Buffer => {
  const h = Buffer.allocUnsafe(5); h.write(tag, 0, 'latin1'); h.writeInt32BE(body.length + 4, 1)
  return Buffer.concat([h, body])
}
// a served response + captured requests
function serve(bodies: (Buffer | { status?: number; ct?: string; body: Buffer })[]) {
  const calls: { url: string; headers: Record<string, string>; body: unknown }[] = []
  let i = 0
  const fetchImpl = (async (url: Parameters<typeof fetch>[0], init?: RequestInit) => {
    calls.push({ url: String(url), headers: Object.fromEntries(Object.entries((init?.headers ?? {}) as Record<string, string>)), body: JSON.parse(init?.body as string) })
    const b = bodies[Math.min(i++, bodies.length - 1)]!
    const r = Buffer.isBuffer(b) ? { body: b } : b
    return new Response(new Uint8Array(r.body), { status: r.status ?? 200, headers: { 'Content-Type': r.ct ?? 'application/vnd.minipg.pgwire' } })
  }) as typeof fetch
  return { fetchImpl, calls }
}

describe('minipg/http', () => {
  test('decode parity over real backend frames; int8 defaults to STRING on this entry', async () => {
    const body = await wireBody(`select 9007199254740993::int8 as big, 'café' as s, 10.50::numeric as n, null::text as z`)
    const { fetchImpl, calls } = serve([body])
    const db = httpPool({ url: 'https://gw.example/query', token: 'k', fetch: fetchImpl })
    const r = await db.query('select …', [], { mode: 'object' })
    expect(r.rows[0]).toEqual({ big: '9007199254740993', s: 'café', n: '10.50', z: null }) // int8 -> string (JSON-serialisable, lossless)
    expect(r.command).toBe('SELECT'); expect(r.rowCount).toBe(1)
    expect(calls[0]!.headers['Accept']).toBe('application/vnd.minipg.pgwire') // never trips casual mode
    expect(calls[0]!.headers['Authorization']).toBe('Bearer k')
    const parity = httpPool({ url: 'https://gw.example/query', fetch: serve([body]).fetchImpl, int8: 'bigint' })
    expect((await parity.query('x', [], { mode: 'object' })).rows[0]).toEqual({ big: 9007199254740993n, s: 'café', n: '10.50', z: null })
  }, TEST_TIMEOUT)

  test('params encode like the wire driver (BigInt/bytea/Date as text); an E under 200 rejects with the backend PgError', async () => {
    const err = await wireBody('select 1/0')
    const { fetchImpl, calls } = serve([err])
    const db = httpPool({ url: 'https://gw.example/query', fetch: fetchImpl })
    const e = await caught(() => db.query('select $1, $2, $3', [9007199254740993n, Buffer.from('deadbeef', 'hex'), new Date('2024-01-02T03:04:05Z')]))
    expect(e).toBeInstanceOf(PgError)
    expect((e as PgError).code).toBe('22012')
    expect((calls[0]!.body as { params: unknown[] }).params).toEqual(['9007199254740993', '\\xdeadbeef', '2024-01-02T03:04:05.000Z'])
  }, TEST_TIMEOUT)

  test('batch: mid-statement failure rejects with the FIRST error; a TRAILING E (commit-time failure) also rejects the whole call', async () => {
    const mid = await wireBody(`select 1 as a`, 'select 1/0', `select 2 as b`)
    const db1 = httpPool({ url: 'https://gw.example/query', fetch: serve([mid]).fetchImpl })
    const e1 = await caught(() => db1.batch([{ sql: 'a' }, { sql: 'b' }, { sql: 'c' }]))
    expect((e1 as PgError).code).toBe('22012')

    // commit-time failure: N clean results, then an E AFTER the Nth — deferred constraint / 40001
    const okBody = await wireBody(`select 1 as a`, `select 2 as b`)
    const commitE = frame('E', Buffer.concat([Buffer.from('SERROR\0C23505\0Mduplicate key (deferred)\0'), Buffer.from([0])]))
    const db2 = httpPool({ url: 'https://gw.example/query', fetch: serve([Buffer.concat([okBody, commitE])]).fetchImpl })
    const e2 = await caught(() => db2.batch([{ sql: 'a' }, { sql: 'b' }]))
    expect((e2 as PgError).code).toBe('23505') // rows for undone work are never surfaced
  }, TEST_TIMEOUT)

  test('pipeline: statement i failed, the rest committed — full per-statement outcome array', async () => {
    const body = await wireBody(`select 1 as a`, 'select 1/0', `select 3 as c`)
    const db = httpPool({ url: 'https://gw.example/query', fetch: serve([body]).fetchImpl })
    const rs = await db.pipeline([{ sql: 'a', mode: 'object' }, { sql: 'b' }, { sql: 'c', mode: 'object' }])
    expect(rs[0]).toEqual({ status: 'fulfilled', value: expect.objectContaining({ rows: [{ a: 1 }] }) })
    expect(rs[1]!.status).toBe('rejected')
    expect(((rs[1] as { reason: PgError }).reason).code).toBe('22012')
    expect((rs[2] as { value: { rows: unknown[] } }).value.rows).toEqual([{ c: 3 }])
  }, TEST_TIMEOUT)

  test('desc elision: 2nd request carries the T-frame hash; a T-less response decodes via the cached descriptor', async () => {
    const sql = `select 7 as id, 'ada' as name`
    const full = await wireBody(sql)
    // simulate a desc hit: the gateway omits T — strip it from the capture
    const msgs = new Parser().push(full)
    const noT = Buffer.concat(msgs.filter((m) => m.type !== 'T').map((m) => frame(m.type, m.body)))
    const { fetchImpl, calls } = serve([full, noT])
    const db = httpPool({ url: 'https://gw.example/query', fetch: fetchImpl })
    expect((await db.query(sql, [], { mode: 'object' })).rows[0]).toEqual({ id: 7, name: 'ada' })
    expect((calls[0]!.body as { desc?: string }).desc).toBeUndefined() // cold: nothing to claim
    const r2 = await db.query(sql, [], { mode: 'object' })
    expect(r2.rows[0]).toEqual({ id: 7, name: 'ada' }) // decoded with the cached fields
    expect((calls[1]!.body as { desc?: string }).desc).toMatch(/^[0-9a-f]{16}$/) // FNV-1a 64 of the raw T frame
  }, TEST_TIMEOUT)

  test('guards + transport errors: tx-control refused locally; wrong content-type refused; non-200 synthetic E -> PgError', async () => {
    const db = httpPool({ url: 'https://gw.example/query', fetch: serve([Buffer.alloc(0)]).fetchImpl })
    expect(() => db['guard']('commit')).toThrow(/GATEWAY owns the transaction boundary/)
    await expect(db.query('begin')).rejects.toThrow(/transaction control/)

    const wrongCt = httpPool({ url: 'https://gw.example/query', fetch: serve([{ body: Buffer.from('{}'), ct: 'application/json' }]).fetchImpl })
    await expect(wrongCt.query('select 1')).rejects.toThrow(/unexpected response content-type/)

    const authE = frame('E', Buffer.concat([Buffer.from('SFATAL\0C28000\0Mbad key\0'), Buffer.from([0])]))
    const denied = httpPool({ url: 'https://gw.example/query', fetch: serve([{ body: authE, status: 401 }]).fetchImpl })
    const e = await caught(() => denied.query('select 1'))
    expect(e).toBeInstanceOf(PgError)
    expect((e as PgError).code).toBe('28000') // one parse path: gateway errors ARE pgwire
  }, TEST_TIMEOUT)

  test('connection-level frames in the body are refused (the gateway must never forward Z/S/R/K)', async () => {
    const z = frame('Z', Buffer.from('I'))
    const db = httpPool({ url: 'https://gw.example/query', fetch: serve([Buffer.concat([await wireBody('select 1 as x'), z])]).fetchImpl })
    await expect(db.query('select 1')).rejects.toThrow(/connection-level frame 'Z'/)
  }, TEST_TIMEOUT)
})
