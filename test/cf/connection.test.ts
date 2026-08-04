// Real-workerd test for the minipg/cf adapter (nodejs_compat path). Verifies the pieces that our
// Node-based simulation can't: cloudflare:sockets connect(), SCRAM auth via nodejs_compat node:crypto,
// Buffer, and — the open question — Duplex.from({readable,writable}) under workerd's node:stream.
//
// Needs the local PG cluster reachable from workerd (`bun run test:setup`, listening on 127.0.0.1:54329).
// Run with: `bun run test:cf`.
import { it, expect } from 'vitest'
import { connect } from '../../src/cf.ts'

const CFG = { host: '127.0.0.1', port: 54329, user: 'postgres', password: 'postgres', database: 'testdb' }

it('connects over cloudflare:sockets, authenticates, and text-decodes', async () => {
  const db = await connect(CFG)
  try {
    const r = await db.query('select 1 as n, $1::text as t, (g * 1.5)::float8 as f from generate_series(1, 3) g', ['hi'], { mode: 'object' })
    expect(r.rows.length).toBe(3)
    expect(r.rows[0]).toEqual({ n: 1, t: 'hi', f: 1.5 })
  } finally {
    await db.end()
  }
})

it('binary flow (queryTyped) + a larger stream survive the workerd transport', async () => {
  const db = await connect(CFG)
  try {
    const b = await db.queryTyped(
      `select 3.141592653589793::float8, 9223372036854775807::int8, 42::int4`,
      [],
      [
        { name: 'f', oid: 701, format: 'binary' },
        { name: 'big', oid: 20, format: 'binary' },
        { name: 'i', oid: 23, format: 'binary' },
      ],
      { mode: 'object' },
    )
    expect(b.rows[0]).toEqual({ f: 3.141592653589793, big: 9223372036854775807n, i: 42 })

    // stress chunking/backpressure through Duplex.from under workerd
    const big = await db.query('select g from generate_series(1, 1000) g')
    expect(big.rows.length).toBe(1000)
  } finally {
    await db.end()
  }
})

it('a url-only config dials the URL host/port, not localhost:5432 (cf-transport regression)', async () => {
  // pre-fix, cfSocket read the RAW config (host undefined -> localhost:5432) and only the
  // startup message saw the parsed url — right user, wrong server. 54329 != the 5432 default,
  // so this connect only succeeds if the url actually reached the dialer.
  const db = await connect('postgres://postgres:postgres@127.0.0.1:54329/testdb')
  try {
    const r = await db.query('select current_database() as db', [], { mode: 'object' })
    expect(r.rows[0]).toEqual({ db: 'testdb' })
  } finally {
    await db.end()
  }
})
