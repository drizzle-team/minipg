// Before/after the JIT param encoder: 1M-row inserts, encode:'interpreted' (the pre-JIT generic
// write-through plan) vs encode:'jit' (the new per-statement codegen'd Bind encoder). SAME binary,
// just the gate flipped — so this isolates exactly the JIT's effect. Local cluster: bun run test:setup.
//   bun playground/inserts/jit-1m.bench.ts            # N = 1,000,000
//   bun playground/inserts/jit-1m.bench.ts 500000 2   # N, RUNS
// Timed = rows-in-memory -> COMMIT confirmed (client encode included). TRUNCATE between runs (untimed).
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb', password: 'postgres' }
const N = Number(process.argv[2] ?? 1_000_000)
const RUNS = Number(process.argv[3] ?? 3)
const WIN = 50_000 // cap in-flight pipelined queries (1M promises is memory noise, not signal)

const COLS = 'id,name,qty,price,flag,created_at'
const INS = `insert into ins_jit(${COLS}) values ($1,$2,$3,$4,$5,$6)`
const mvSql = (c: number) => `insert into ins_jit(${COLS}) values ` + Array.from({ length: c }, (_, i) => `($${i * 6 + 1},$${i * 6 + 2},$${i * 6 + 3},$${i * 6 + 4},$${i * 6 + 5},$${i * 6 + 6})`).join(',')

type Row = [number, string, number, number, boolean, Date]
const BASE = Date.UTC(2026, 0, 1)
const rows: Row[] = new Array(N)
for (let i = 0; i < N; i++) rows[i] = [i + 1, `name_${i}_${(i * 2654435761 % 100000).toString(36)}`, i % 1000, (i % 90000) + 0.25, i % 2 === 0, new Date(BASE + (i % 86400) * 1000)]

const interp = await connect({ path: SOCK, ...DB, encode: 'interpreted' })
const jit = await connect({ path: SOCK, ...DB, encode: 'jit' })
type MC = typeof jit

await interp.query('drop table if exists ins_jit')
await interp.query(`create table ins_jit(id int8, name text, qty int4, price float8, flag bool, created_at timestamptz)`)

// ---- strategies (each returns after COMMIT) ----
async function perRowPipe(mc: MC): Promise<void> {
  await mc.begin(async (tx) => {
    for (let s = 0; s < N; s += WIN) {
      const e = Math.min(s + WIN, N)
      const ps: Promise<unknown>[] = new Array(e - s)
      for (let i = s; i < e; i++) ps[i - s] = tx.query(INS, rows[i]!, { name: 'i_pipe' })
      await Promise.all(ps)
    }
  })
}
function mvChunked(c: number, name: string): (mc: MC) => Promise<void> {
  const sql = mvSql(c)
  return (mc) => mc.begin(async (tx) => {
    let ps: Promise<unknown>[] = []
    for (let i = 0; i + c <= N; i += c) {
      const flat: unknown[] = new Array(c * 6)
      for (let j = 0; j < c; j++) { const r = rows[i + j]!, b = j * 6; flat[b] = r[0]; flat[b + 1] = r[1]; flat[b + 2] = r[2]; flat[b + 3] = r[3]; flat[b + 4] = r[4]; flat[b + 5] = r[5] }
      ps.push(tx.query(sql, flat, { name }))
      if (ps.length * c >= WIN) { await Promise.all(ps); ps = [] }
    }
    if (ps.length) await Promise.all(ps)
  })
}
const bulk = (mc: MC) => mc.bulkInsert('ins_jit', { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' }, rows)

const STRATS: Array<{ name: string; note: string; run: (mc: MC) => Promise<void> }> = [
  { name: 'per-row pipelined (6 params)', note: 'JIT: 6-col body ×1', run: perRowPipe },
  { name: 'VALUES ×10 (60 params)', note: 'JIT: 6-col body ×10 (period 6)', run: mvChunked(10, 'mv10') },
  { name: 'VALUES ×100 (600 params)', note: 'JIT: 6-col body ×100 (period 6)', run: mvChunked(100, 'mv100') },
  { name: 'VALUES ×1000 (6000 params)', note: 'JIT: 6-col body ×1000 (period 6)', run: mvChunked(1000, 'mv1000') },
  { name: 'bulkInsert unnest (arrays)', note: 'JIT engages but arrays -> closure (framing only)', run: bulk },
]

const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]!
async function time(mc: MC, run: (mc: MC) => Promise<void>): Promise<number> {
  await interp.query('truncate ins_jit')
  const t = performance.now()
  await run(mc)
  return performance.now() - t
}

console.log(`\n1M-row insert: encode:'interpreted' (pre-JIT) vs encode:'jit' — N=${N.toLocaleString()}, median of ${RUNS} (1 warmup)\n`)
console.log('strategy'.padEnd(34) + 'interp'.padStart(12) + 'jit'.padStart(12) + 'speedup'.padStart(10) + '   note')
for (const s of STRATS) {
  await time(interp, s.run); await time(jit, s.run) // warmup both
  const iMs: number[] = [], jMs: number[] = []
  for (let r = 0; r < RUNS; r++) { iMs.push(await time(interp, s.run)); jMs.push(await time(jit, s.run)) }
  const iRps = N / (median(iMs) / 1000), jRps = N / (median(jMs) / 1000)
  const fmt = (r: number) => (r / 1e6).toFixed(2) + 'M/s'
  console.log(s.name.padEnd(34) + fmt(iRps).padStart(12) + fmt(jRps).padStart(12) + (jRps / iRps).toFixed(2).padStart(9) + 'x   ' + s.note)
}

await interp.query('drop table if exists ins_jit')
await interp.end(); await jit.end()
