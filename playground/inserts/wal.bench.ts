// WAL-lever matrix: the three main insert paths × WAL/durability regimes, 1M rows each,
// run hygiene (checkpoint + settle) between timed runs. BENCH CLUSTER ONLY — fsync=off and
// full_page_writes=off are toggled via ALTER SYSTEM and always restored in a finally.
//   bun playground/inserts/wal.bench.ts
import { connect } from '../../src/index.ts'

const mc = await connect({ path: '/tmp/minipg_sock/.s.PGSQL.54329', user: 'postgres', database: 'testdb' })
const N = 1_000_000
const BASE = Date.UTC(2026, 0, 1)
const rows: Record<string, unknown>[] = new Array(N)
for (let i = 0; i < N; i++) rows[i] = { id: i + 1, name: `n_${i}`, qty: i % 1000, price: i + 0.25, flag: i % 2 === 0, created_at: new Date(BASE + (i % 86400) * 1000) }
const CT = { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' } as const

const COLS = 'id,name,qty,price,flag,created_at'
function mvSql(c: number): string {
  const vals: string[] = new Array(c)
  for (let i = 0; i < c; i++) { const b = i * 6; vals[i] = `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})` }
  return `insert into walb(${COLS}) values ` + vals.join(',')
}
const MV100 = mvSql(100)
const flat = (part: Record<string, unknown>[]): unknown[] => {
  const out: unknown[] = new Array(part.length * 6)
  for (let j = 0; j < part.length; j++) { const r = part[j]!, b = j * 6; out[b] = r.id; out[b + 1] = r.name; out[b + 2] = r.qty; out[b + 3] = r.price; out[b + 4] = r.flag; out[b + 5] = r.created_at }
  return out
}

const sync = async () => { await mc.query('checkpoint'); await Bun.sleep(700) }
type Strat = { name: string; run: () => Promise<void> }
const STRATS: Strat[] = [
  { name: 'VALUES ×100, one tx', run: () => mc.begin(async (tx) => { const ps: Promise<unknown>[] = []; for (let i = 0; i + 100 <= N; i += 100) ps.push(tx.query(MV100, flat(rows.slice(i, i + 100)), { name: 'wv' })); await Promise.all(ps) }) },
  { name: 'bulkInsert (unnest ×1k)', run: async () => { await mc.bulkInsert('walb', CT, rows) } },
  { name: 'copyMany (binary)', run: async () => { await mc.copyMany('walb', CT, rows) } },
]

interface Regime { name: string; setup: () => Promise<void>; teardown: () => Promise<void>; unlogged?: boolean }
const reload = () => mc.query('select pg_reload_conf()')
const REGIMES: Regime[] = [
  { name: 'baseline (full WAL + fsync)', setup: async () => {}, teardown: async () => {} },
  { name: 'full_page_writes=off', setup: async () => { await mc.query("alter system set full_page_writes = off"); await reload() }, teardown: async () => { await mc.query('alter system reset full_page_writes'); await reload() } },
  { name: 'synchronous_commit=off', setup: async () => { await mc.query('set synchronous_commit = off') }, teardown: async () => { await mc.query('set synchronous_commit = on') } },
  { name: 'UNLOGGED table', setup: async () => {}, teardown: async () => {}, unlogged: true },
  { name: 'fsync=off (dev-only ceiling)', setup: async () => { await mc.query('alter system set fsync = off'); await reload() }, teardown: async () => { await mc.query('alter system reset fsync'); await reload() } },
]

const mkTable = async (unlogged: boolean) => {
  await mc.query('drop table if exists walb')
  await mc.query(`create ${unlogged ? 'unlogged ' : ''}table walb(id int8 primary key, name text, qty int4, price float8, flag bool, created_at timestamptz)`)
}

try {
  for (const reg of REGIMES) {
    console.log(`\n== ${reg.name} · ${N.toLocaleString()} rows (median of 3) ==`)
    await reg.setup()
    try {
      for (const s of STRATS) {
        await mkTable(!!reg.unlogged)
        await sync(); await s.run() // warmup
        const times: number[] = []
        for (let r = 0; r < 3; r++) {
          await mc.query('truncate walb'); await sync()
          const t0 = performance.now(); await s.run(); times.push(performance.now() - t0)
        }
        const n = await mc.query('select count(*)::int4 from walb')
        if ((n.rows[0] as unknown as number[])[0] !== N) throw new Error(`${s.name}: bad count`)
        const ms = [...times].sort((a, b) => a - b)[1]!
        console.log(`  ${s.name.padEnd(28)} ${ms.toFixed(0).padStart(7)} ms   ${Math.round(N / (ms / 1000)).toLocaleString().padStart(10)} rows/s`)
      }
    } finally { await reg.teardown() }
  }
} finally {
  // belt and suspenders: leave the cluster durable no matter what happened above
  await mc.query('alter system reset fsync').catch(() => {})
  await mc.query('alter system reset full_page_writes').catch(() => {})
  await mc.query('select pg_reload_conf()').catch(() => {})
  await mc.query('drop table if exists walb').catch(() => {})
}
await mc.end()
process.exit(0)
