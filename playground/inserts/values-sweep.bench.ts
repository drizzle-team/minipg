// Multi-row VALUES chunk-size sweep — the VALUES-shape sibling of chunk-sweep.bench.ts.
// One prepared statement per chunk size (binary params via reuse), chunks pipelined in one tx.
// Hygiene: checkpoint + settle before every timed run; median of 3 with min..max spread.
//   bun playground/inserts/values-sweep.bench.ts
import { connect } from '../../src/index.ts'

const mc = await connect({ path: '/tmp/minipg_sock/.s.PGSQL.54329', user: 'postgres', database: 'testdb' })
await mc.query('drop table if exists vsw')
await mc.query('create table vsw(id int8 primary key, name text, qty int4, price float8, flag bool, created_at timestamptz)')
const BASE = Date.UTC(2026, 0, 1)
interface R { id: number; name: string; qty: number; price: number; flag: boolean; created_at: Date }
const mk = (n: number): R[] => {
  const rows: R[] = new Array(n)
  for (let i = 0; i < n; i++) rows[i] = { id: i + 1, name: `n_${i}`, qty: i % 1000, price: i + 0.25, flag: i % 2 === 0, created_at: new Date(BASE + (i % 86400) * 1000) }
  return rows
}
function mvSql(c: number): string {
  const vals: string[] = new Array(c)
  for (let i = 0; i < c; i++) { const b = i * 6; vals[i] = `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})` }
  return 'insert into vsw(id,name,qty,price,flag,created_at) values ' + vals.join(',')
}
const sync = async () => { await mc.query('checkpoint'); await Bun.sleep(700) }

async function runValues(rows: R[], c: number, sql: string, name: string): Promise<void> {
  await mc.begin(async (tx) => {
    const ps: Promise<unknown>[] = []
    for (let i = 0; i < rows.length; i += c) {
      const flat: unknown[] = new Array(c * 6)
      for (let j = 0; j < c; j++) { const r = rows[i + j]!, b = j * 6; flat[b] = r.id; flat[b + 1] = r.name; flat[b + 2] = r.qty; flat[b + 3] = r.price; flat[b + 4] = r.flag; flat[b + 5] = r.created_at }
      ps.push(tx.query(sql, flat, { name }))
    }
    await Promise.all(ps)
  })
}

const CHUNKS = [25, 50, 100, 250, 500, 1000, 2000]
const SIZES = [10_000, 100_000, 1_000_000]
const SQLS = new Map(CHUNKS.map((c) => [c, mvSql(c)]))
for (const n of SIZES) {
  const rows = mk(n)
  console.log(`\n== VALUES ${n.toLocaleString()} rows (median of 3, spread = min..max) ==`)
  for (const c of CHUNKS) {
    const sql = SQLS.get(c)!, name = `vs${c}`
    await mc.query('truncate vsw'); await sync(); await runValues(rows, c, sql, name) // warmup (primes binary plan)
    const times: number[] = []
    for (let r = 0; r < 3; r++) {
      await mc.query('truncate vsw'); await sync()
      const t0 = performance.now(); await runValues(rows, c, sql, name); times.push(performance.now() - t0)
    }
    const chk = await mc.query('select count(*)::int4 from vsw')
    if ((chk.rows[0] as unknown as number[])[0] !== n) throw new Error(`chunk ${c}: bad count`)
    const s = [...times].sort((a, b) => a - b)
    const rps = (ms: number) => Math.round(n / (ms / 1000))
    console.log(`  rows/stmt=${String(c).padEnd(5)} ${rps(s[1]!).toLocaleString().padStart(10)}/s   (${rps(s[2]!).toLocaleString()}..${rps(s[0]!).toLocaleString()})`)
  }
}
await mc.query('drop table vsw')
await mc.end()
process.exit(0)
