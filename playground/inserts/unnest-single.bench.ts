// Single-statement unnest inserts (NO chunking) across a size sweep — where exactly does the
// one-giant-statement curve bend? Binary arrays via bulkInsert with an oversized chunk;
// chunked ×1k and copyMany as references. Hygiene: checkpoint + settle before every timed run.
//   bun playground/inserts/unnest-single.bench.ts
import { connect } from '../../src/index.ts'

const mc = await connect({ path: '/tmp/minipg_sock/.s.PGSQL.54329', user: 'postgres', database: 'testdb' })
await mc.query('drop table if exists usb')
await mc.query('create table usb(id int8 primary key, name text, qty int4, price float8, flag bool, created_at timestamptz)')
const CT = { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' } as const
const BASE = Date.UTC(2026, 0, 1)
const mk = (n: number): Record<string, unknown>[] => {
  const rows: Record<string, unknown>[] = new Array(n)
  for (let i = 0; i < n; i++) rows[i] = { id: i + 1, name: `n_${i}`, qty: i % 1000, price: i + 0.25, flag: i % 2 === 0, created_at: new Date(BASE + (i % 86400) * 1000) }
  return rows
}
const sync = async () => { await mc.query('checkpoint'); await Bun.sleep(700) }

const LANES = [
  { name: 'unnest SINGLE stmt (binary)', run: (r: Record<string, unknown>[]) => mc.bulkInsert('usb', CT, r, { chunk: Number.MAX_SAFE_INTEGER }) },
  { name: 'unnest ×1k chunks (default)', run: (r: Record<string, unknown>[]) => mc.bulkInsert('usb', CT, r) },
  { name: 'copyMany binary (reference)', run: (r: Record<string, unknown>[]) => mc.copyMany('usb', CT, r) },
]

const SIZES = [1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000]
console.log('size'.padEnd(11) + LANES.map((l) => l.name.padStart(30)).join(''))
for (const n of SIZES) {
  const rows = mk(n)
  const cells: string[] = []
  for (const lane of LANES) {
    await mc.query('truncate usb'); await sync(); await lane.run(rows) // warmup
    const reps = n > 100_000 ? 2 : 3
    const times: number[] = []
    for (let r = 0; r < reps; r++) {
      await mc.query('truncate usb'); await sync()
      const t0 = performance.now(); await lane.run(rows); times.push(performance.now() - t0)
    }
    const chk = await mc.query('select count(*)::int4 from usb')
    if ((chk.rows[0] as unknown as number[])[0] !== n) throw new Error(`${lane.name}@${n}: bad count`)
    const ms = Math.min(...times)
    cells.push(`${Math.round(n / (ms / 1000)).toLocaleString()}/s`.padStart(30))
  }
  console.log(String(n.toLocaleString()).padEnd(11) + cells.join(''))
}
await mc.query('drop table usb')
await mc.end()
process.exit(0)
