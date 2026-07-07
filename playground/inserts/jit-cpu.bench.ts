// Where the JIT param encoder actually pays off: CLIENT CPU, not wall-clock. Inserts return no rows
// (encode-heavy, ~zero decode), so process.cpuUsage() isolates the encode cost, and an event-loop lag
// monitor shows the user-facing consequence — how long the main thread is blocked while encoding.
// Wall-clock is server-bound (unchanged); the point is the client-side cost of producing the same bytes.
//   bun playground/inserts/jit-cpu.bench.ts            # N=1,000,000, RUNS=3
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb', password: 'postgres' }
const N = Number(process.argv[2] ?? 1_000_000)
const RUNS = Number(process.argv[3] ?? 3)
const WIN = 50_000

const COLS = 'id,name,qty,price,flag,created_at'
const INS = `insert into ins_cpu(${COLS}) values ($1,$2,$3,$4,$5,$6)`
const mvSql = (c: number) => `insert into ins_cpu(${COLS}) values ` + Array.from({ length: c }, (_, i) => `($${i * 6 + 1},$${i * 6 + 2},$${i * 6 + 3},$${i * 6 + 4},$${i * 6 + 5},$${i * 6 + 6})`).join(',')

type Row = [number, string, number, number, boolean, Date]
const BASE = Date.UTC(2026, 0, 1)
const rows: Row[] = new Array(N)
for (let i = 0; i < N; i++) rows[i] = [i + 1, `name_${i}_${(i * 2654435761 % 100000).toString(36)}`, i % 1000, (i % 90000) + 0.25, i % 2 === 0, new Date(BASE + (i % 86400) * 1000)]

const interp = await connect({ path: SOCK, ...DB, encode: 'interpreted' })
const jit = await connect({ path: SOCK, ...DB, encode: 'jit' })
type MC = typeof jit

await interp.query('drop table if exists ins_cpu')
await interp.query(`create unlogged table ins_cpu(id int8, name text, qty int4, price float8, flag bool, created_at timestamptz)`) // UNLOGGED: cheaper server side -> client cost more visible

function perRowPipe(mc: MC): Promise<void> {
  return mc.begin(async (tx) => {
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

interface M { wall: number; cpuMs: number; maxLag: number; p99: number }
async function measure(mc: MC, run: (mc: MC) => Promise<void>): Promise<M> {
  await interp.query('truncate ins_cpu')
  const lags: number[] = []
  let last = performance.now(), maxLag = 0
  const timer = setInterval(() => { const now = performance.now(); const lag = now - last - 4; if (lag > maxLag) maxLag = lag; lags.push(lag > 0 ? lag : 0); last = now }, 4)
  const cpu0 = process.cpuUsage(); const t0 = performance.now()
  await run(mc)
  const wall = performance.now() - t0; const cpu = process.cpuUsage(cpu0)
  clearInterval(timer)
  lags.sort((a, b) => a - b)
  return { wall, cpuMs: (cpu.user + cpu.system) / 1000, maxLag, p99: lags[Math.floor(lags.length * 0.99)] ?? 0 }
}
const med = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]!

const WORK: Array<{ name: string; run: (mc: MC) => Promise<void> }> = [
  { name: 'per-row pipelined (6 params ×1M)', run: perRowPipe },
  { name: 'VALUES ×10 (60 params ×100k)', run: mvChunked(10, 'mv10') },
  { name: 'VALUES ×100 (600 params ×10k)', run: mvChunked(100, 'mv100') },
  { name: 'VALUES ×1000 (6000 params ×1k)', run: mvChunked(1000, 'mv1000') },
]

console.log(`\nClient-CPU cost of the SAME 1M inserts: encode:'interpreted' vs encode:'jit' — N=${N.toLocaleString()}, median of ${RUNS}\n`)
for (const w of WORK) {
  await measure(interp, w.run); await measure(jit, w.run) // warmup (compiles the JIT once)
  const I: M[] = [], J: M[] = []
  for (let r = 0; r < RUNS; r++) { I.push(await measure(interp, w.run)); J.push(await measure(jit, w.run)) }
  const iCpu = med(I.map((m) => m.cpuMs)), jCpu = med(J.map((m) => m.cpuMs))
  const iLag = med(I.map((m) => m.maxLag)), jLag = med(J.map((m) => m.maxLag))
  const iP99 = med(I.map((m) => m.p99)), jP99 = med(J.map((m) => m.p99))
  console.log(w.name)
  console.log(`  client CPU:      interp ${iCpu.toFixed(0).padStart(6)} ms   jit ${jCpu.toFixed(0).padStart(6)} ms   -> ${((1 - jCpu / iCpu) * 100).toFixed(0)}% less CPU  (${(iCpu / jCpu).toFixed(2)}x)`)
  console.log(`  event-loop max:  interp ${iLag.toFixed(1).padStart(6)} ms   jit ${jLag.toFixed(1).padStart(6)} ms`)
  console.log(`  event-loop p99:  interp ${iP99.toFixed(2).padStart(6)} ms   jit ${jP99.toFixed(2).padStart(6)} ms`)
  console.log(`  wall (server-bound): interp ${med(I.map((m) => m.wall)).toFixed(0)} ms   jit ${med(J.map((m) => m.wall)).toFixed(0)} ms\n`)
}

await interp.query('drop table if exists ins_cpu')
await interp.end(); await jit.end()
