// Is the prepared-statement naming (valuesMeta cache + 256 cap) worth its complexity for the
// bulkInsert({ defaults:true }) VALUES path? The clean question: how much does Parse-once (named,
// reused across chunks) actually beat Parse-per-chunk (unnamed) at scale?
//
// Lever: a `prepare:false` connection makes query() drop every statement name -> UNNAMED (re-Parse
// each chunk). Same bulkInsert code, same rows, same chunks — only naming differs. So
//   prepared·uniform  vs  unnamed·uniform   = the pure Parse-reuse win (identical data & SQL).
//   prepared·highcard vs  unnamed·highcard  = the win when per-row-varying DEFAULT placement
//                                             defeats reuse anyway (prepared can't cache -> ~0).
// UNLOGGED table to cut WAL noise so the Parse difference (if any) actually shows.
//
//   bun run test:setup
//   bun playground/inserts/defaults-prepare.bench.ts            # 1M rows, chunks 100 & 1000
//   bun playground/inserts/defaults-prepare.bench.ts 1000000 100,250,1000
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }
const N = Number(process.argv[2] ?? 1_000_000)
const CHUNKS = (process.argv[3] ?? '100,1000').split(',').map(Number)
const RUNS = 3

const admin = await connect({ path: SOCK, ...DB })
await admin.query('drop table if exists dfl_bench')
await admin.query(`create unlogged table dfl_bench(
  id int8 primary key, a int4 not null, b text not null,
  c timestamptz default now(), d int4 not null default 0, e text default 'x')`)

const COLS = { id: 'int8', a: 'int4', b: 'text', c: 'timestamptz', d: 'int4', e: 'text' } as const
const OLD = new Date('2020-01-01T00:00:00Z')

// rand without Math.random dependence on run order: cheap LCG so the pattern is fixed & reproducible
let seed = 12345
const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff

// uniform: every row omits c,d,e -> one DEFAULT pattern for the whole batch (best case for reuse)
const uniformRows = (n: number) => Array.from({ length: n }, (_, i) => ({ id: i + 1, a: i % 1000, b: `b_${i}` }))
// high-card: each row independently omits a random subset of c,d,e -> each chunk's SQL is unique
const highCardRows = (n: number) => Array.from({ length: n }, (_, i) => {
  const r: Record<string, unknown> = { id: i + 1, a: i % 1000, b: `b_${i}` }
  if (rnd() < 0.5) r.c = OLD
  if (rnd() < 0.5) r.d = i % 7
  if (rnd() < 0.5) r.e = 't' + i
  return r
})

// fully-provided rows (all 6 columns) — the unnest baseline can't leave a NOT NULL column undefined
const fullRows = (n: number) => Array.from({ length: n }, (_, i) => ({ id: i + 1, a: i % 1000, b: `b_${i}`, c: OLD, d: i % 7, e: 't' + i }))

const uni = uniformRows(N)
seed = 12345
const hic = highCardRows(N)
const full = fullRows(N)

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!
const sync = async () => { await admin.query('checkpoint'); await Bun.sleep(500) }

interface Lane { name: string; prepare: boolean; rows: readonly Record<string, unknown>[]; defaults: boolean }
const lanes: Lane[] = [
  { name: 'unnest baseline (no defaults, prepared)', prepare: true, rows: full, defaults: false },
  { name: 'defaults · prepared · uniform', prepare: true, rows: uni, defaults: true },
  { name: 'defaults · UNNAMED  · uniform', prepare: false, rows: uni, defaults: true },
  { name: 'defaults · prepared · high-card', prepare: true, rows: hic, defaults: true },
  { name: 'defaults · UNNAMED  · high-card', prepare: false, rows: hic, defaults: true },
]

const results: { name: string; chunk: number; ms: number; stmts: number }[] = []

for (const chunk of CHUNKS) {
  console.log(`\n== ${N.toLocaleString()} rows, chunk ${chunk} (${Math.ceil(N / chunk).toLocaleString()} chunks) — median of ${RUNS} ==`)
  for (const l of lanes) {
    // fresh connection per lane: clean valuesMeta + per-session pg_prepared_statements count
    const c = await connect({ path: SOCK, ...DB, prepare: l.prepare })
    const opts = l.defaults ? { chunk, defaults: true } : { chunk }
    await admin.query('truncate dfl_bench'); await sync()
    await c.bulkInsert('dfl_bench', COLS, l.rows as never, opts) // warmup (primes plancache + prepared names)
    const times: number[] = []
    for (let r = 0; r < RUNS; r++) {
      await admin.query('truncate dfl_bench'); await sync()
      const t0 = performance.now()
      await c.bulkInsert('dfl_bench', COLS, l.rows as never, opts)
      times.push(performance.now() - t0)
    }
    const ms = median(times)
    const pp = await c.query("select count(*)::int4 from pg_prepared_statements where name like '\\_iv%'")
    const stmts = (pp.rows[0] as unknown[])[0] as number
    results.push({ name: l.name, chunk, ms, stmts })
    console.log(`  ${l.name.padEnd(40)} ${ms.toFixed(0).padStart(6)} ms  ${Math.round(N / (ms / 1000)).toLocaleString().padStart(11)} rows/s   (${stmts} prepared stmts)`)
    await c.end()
  }
}

// verdict: prepared vs unnamed delta per pattern
console.log('\n── prepared-vs-unnamed delta (positive = naming helps) ──')
for (const chunk of CHUNKS) {
  for (const pat of ['uniform', 'high-card']) {
    const p = results.find((r) => r.chunk === chunk && r.name.includes('prepared') && r.name.includes(pat))!
    const u = results.find((r) => r.chunk === chunk && r.name.includes('UNNAMED') && r.name.includes(pat))!
    const delta = ((u.ms - p.ms) / u.ms) * 100
    console.log(`  chunk ${String(chunk).padStart(4)} · ${pat.padEnd(9)}  prepared ${p.ms.toFixed(0)}ms vs unnamed ${u.ms.toFixed(0)}ms  →  ${delta >= 0 ? '+' : ''}${delta.toFixed(1)}% ${Math.abs(delta) < 3 ? '(noise)' : ''}`)
  }
}

await admin.query('drop table if exists dfl_bench')
await admin.end()
console.log('\ndone.')
