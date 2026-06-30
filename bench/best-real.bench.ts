// Real-PostgreSQL comparison: minipg in three flavors — no-codegen (write-buffer +
// int-from-bytes, the CSP-safe path), codegen, and codegen-inline — vs node-postgres
// and postgres.js. Needs `bun run test:setup`.
//   bun bench/best-real.bench.ts
import { run, bench, group, summary, do_not_optimize } from 'mitata'
import { Client } from 'pg'
import postgres from 'postgres'
import { connect as nocodegen } from '../src/all/index.ts'   // write-buffer + int-from-bytes (no new Function)
import { connect as codegen } from '../src/best/index.ts'    // write-buffer + codegen (helper calls)
import { connect as inlineGen } from '../src/inline/index.ts' // write-buffer + codegen (inlined)

const cfg = { host: '127.0.0.1', port: 54329, user: 'postgres', password: 'postgres', database: 'testdb' }
const mNo = await nocodegen(cfg)
const mCg = await codegen(cfg)
const mIn = await inlineGen(cfg)
const pgc = new Client(cfg); await pgc.connect()
const sql = postgres({ ...cfg, max: 1 })
let n = 0
const ROWS = "select g as id, 'name_' || g as name, g * 2 as v from generate_series(1, 100) g"

group('SELECT 1', () => summary(() => {
  bench('minipg no-codegen', async () => { do_not_optimize(await mNo.query('select 1')) })
  bench('minipg codegen', async () => { do_not_optimize(await mCg.query('select 1')) })
  bench('minipg codegen-inline', async () => { do_not_optimize(await mIn.query('select 1')) })
  bench('pg', async () => { do_not_optimize(await pgc.query('select 1')) })
  bench('postgres.js', async () => { do_not_optimize(await sql`select 1`) })
}))
group('100-row result (object)', () => summary(() => {
  bench('minipg no-codegen', async () => { do_not_optimize(await mNo.query(ROWS, [], { mode: 'object' })) })
  bench('minipg codegen', async () => { do_not_optimize(await mCg.query(ROWS, [], { mode: 'object' })) })
  bench('minipg codegen-inline', async () => { do_not_optimize(await mIn.query(ROWS, [], { mode: 'object' })) })
  bench('pg', async () => { do_not_optimize(await pgc.query(ROWS)) })
  bench('postgres.js', async () => { do_not_optimize(await sql.unsafe(ROWS)) })
}))
group('prepared reuse', () => summary(() => {
  bench('minipg no-codegen', async () => { do_not_optimize(await mNo.query('select $1::int', [n++], { name: 'bp' })) })
  bench('minipg codegen', async () => { do_not_optimize(await mCg.query('select $1::int', [n++], { name: 'bp' })) })
  bench('minipg codegen-inline', async () => { do_not_optimize(await mIn.query('select $1::int', [n++], { name: 'bp' })) })
  bench('pg', async () => { do_not_optimize(await pgc.query({ name: 'bp', text: 'select $1::int', values: [n++] })) })
  bench('postgres.js', async () => { const i = n++; do_not_optimize(await sql`select ${i}::int`) })
}))

await run()
await mNo.end(); await mCg.end(); await mIn.end(); await pgc.end(); await sql.end({ timeout: 5 })
process.exit(0)
