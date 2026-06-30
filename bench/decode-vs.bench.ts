// Decode-isolated driver comparison: minipg vs node-postgres (pg) vs postgres.js, all over
// loopback to the in-memory MockPgServer. The mock replies INSTANTLY with pre-serialized
// responses (cacheResponses), so PostgreSQL's query-execution time is removed and the common
// loopback round-trip cancels out in the comparison — what's left is each driver's CPU:
// protocol framing + value decode + row building. This is where the interpreted-decode work
// (cached decoders, fused parse+decode, int-from-bytes, pre-sized result array) shows up,
// which the end-to-end bench/drivers.bench.ts hides under network RTT.
//
// mitata, do_not_optimize, inner_gc. All inputs are built at module scope — the timed fn only
// issues the query.  bun bench/decode-vs.bench.ts   (or: bun run bench:decode-vs)
import { run, bench, group, summary, do_not_optimize } from 'mitata'
import { Client } from 'pg'
import postgres from 'postgres'
// import { SQL } from 'bun'
import { connect } from '../src/index.ts'            // interpreted decode path (cached decoders + int-from-bytes + batch)
import { connect as connectJit } from '../src/inline/index.ts' // JIT decode path (new Function row-builder codegen)
import { MockPgServer } from '../test/mock/server.ts'

// rows: 3 int + 1 text + 1 bool  (exercises int-from-bytes, string, bool decode)
const FIELDS = [{ name: 'id', oid: 23 }, { name: 'a', oid: 23 }, { name: 'b', oid: 23 }, { name: 'name', oid: 25 }, { name: 'ok', oid: 16 }]
const mkRows = (n: number) => Array.from({ length: n }, (_, i) => [String(i + 1), String(i * 2), String(i * 3), 'name_' + i, i % 2 ? 't' : 'f'] as (string | null)[])
const rows100 = mkRows(100), rows1000 = mkRows(1000)
const mock = await MockPgServer.start({ onQuery: (sql) => ({ fields: FIELDS, rows: sql.includes('1000') ? rows1000 : rows100, command: 'SELECT' }) })
mock.cacheResponses(true) // serialize each query once, then replay the bytes -> measure driver decode, not mock serialization

const host = '127.0.0.1', port = mock.port
const mc = await connect({ host, port, user: 'mock', database: 'mock', password: '' })       // interpreted
const mj = await connectJit({ host, port, user: 'mock', database: 'mock', password: '' })     // JIT/codegen
const pgc = new Client({ host, port, user: 'mock', database: 'mock', password: '' }); await pgc.connect()
const sql = postgres({ host, port, user: 'mock', database: 'mock', password: '', max: 1, prepare: false })
// const bunsql = new SQL({ hostname: host, port, username: 'mock', database: 'mock' }) // Bun's built-in Postgres client

// Per-connection SQL text so the (sql-keyed) response cache never serves an extended-protocol
// blob (no RowDescription) to pg's simple-query path. Semantically identical; size is picked
// by the '1000' substring. All inputs pre-built — no allocation in the timed fns.
const M100 = 'select rows100 -- m', X100 = 'select rows100 -- mjit', P100 = 'select rows100 -- pg', J100 = 'select rows100 -- pgjs', B100 = 'select rows100 -- bun'
const M1000 = 'select rows1000 -- m', X1000 = 'select rows1000 -- mjit', P1000 = 'select rows1000 -- pg', J1000 = 'select rows1000 -- pgjs', B1000 = 'select rows1000 -- bun'
const EMPTY: never[] = []
const MODE_OBJ = { mode: 'object' as const }

group('100 rows · 3int + text + bool (decode-isolated)', () => {
  summary(() => {
    bench('minipg interp (object)', async () => { do_not_optimize(await mc.query(M100, EMPTY, MODE_OBJ)) }).gc('inner')
    bench('minipg JIT (object)', async () => { do_not_optimize(await mj.query(X100, EMPTY, MODE_OBJ)) }).gc('inner')
    bench('minipg interp (array)', async () => { do_not_optimize(await mc.query(M100)) }).gc('inner')
    bench('minipg JIT (array)', async () => { do_not_optimize(await mj.query(X100)) }).gc('inner')
    bench('pg (object)', async () => { do_not_optimize(await pgc.query(P100)) }).gc('inner')
    bench('postgres.js (object)', async () => { do_not_optimize(await sql.unsafe(J100)) }).gc('inner')
    // bench('Bun.sql (object)', async () => { do_not_optimize(await bunsql.unsafe(B100)) }).gc('inner')
  })
})

group('1000 rows · 3int + text + bool (decode-isolated)', () => {
  summary(() => {
    bench('minipg interp (object)', async () => { do_not_optimize(await mc.query(M1000, EMPTY, MODE_OBJ)) }).gc('inner')
    bench('minipg JIT (object)', async () => { do_not_optimize(await mj.query(X1000, EMPTY, MODE_OBJ)) }).gc('inner')
    bench('minipg interp (array)', async () => { do_not_optimize(await mc.query(M1000)) }).gc('inner')
    bench('minipg JIT (array)', async () => { do_not_optimize(await mj.query(X1000)) }).gc('inner')
    bench('pg (object)', async () => { do_not_optimize(await pgc.query(P1000)) }).gc('inner')
    bench('postgres.js (object)', async () => { do_not_optimize(await sql.unsafe(J1000)) }).gc('inner')
    // bench('Bun.sql (object)', async () => { do_not_optimize(await bunsql.unsafe(B1000)) }).gc('inner')
  })
})

await run()
await mc.end(); await mj.end(); await pgc.end(); await sql.end({ timeout: 5 });
//  await bunsql.end()
process.exit(0)
