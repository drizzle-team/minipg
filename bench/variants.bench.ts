// Attribute each optimization: baseline vs lean (features stripped) vs each variant vs
// all-combined, every driver over the SAME in-process cached mock (pure driver CPU).
//   bun bench/variants.bench.ts
import { run, bench, group, summary, do_not_optimize } from 'mitata'
import { MockPgServer } from '../test/mock/server.ts'
import { connect as baseline } from '../src/index.ts'
import { connect as variant4 } from '../src/variant4/index.ts'
import { connect as codegen } from '../src/codegen/index.ts'
import { connect as best } from '../src/best/index.ts'
import { connect as inline } from '../src/inline/index.ts'

type Drv = { query: (sql: string, params?: unknown[], opts?: { name?: string; mode?: 'array' | 'object' | 'buffer' | 'raw' }) => Promise<unknown>; end: () => Promise<void> }

const rows100 = Array.from({ length: 100 }, (_, i) => [String(i + 1), `name_${i + 1}`, String((i + 1) * 2)] as (string | null)[])
const mock = await MockPgServer.start({
  onQuery: (sql) => (sql.includes('series')
    ? { fields: [{ name: 'id', oid: 23 }, { name: 'name', oid: 25 }, { name: 'v', oid: 23 }], rows: rows100, command: 'SELECT' }
    : { fields: [{ name: 'n', oid: 23 }], rows: [['1']], command: 'SELECT' }),
})
mock.cacheResponses(true)

const factories = { baseline, variant4, codegen, best, inline }
const NAMES = Object.keys(factories) as (keyof typeof factories)[]
const drivers: Record<string, Drv> = {}
for (const name of NAMES) {
  drivers[name] = (await factories[name]({ host: 'x', port: 0, user: 'u', database: 'd', socket: () => mock.inProcessConnect() })) as unknown as Drv
}

const ROWS = 'select * from generate_series(1,100)'
let n = 0
function benchOp(label: string, op: (d: Drv) => Promise<unknown>): void {
  group(label, () => {
    summary(() => {
      for (const name of NAMES) bench(name, async () => { do_not_optimize(await op(drivers[name]!)) })
    })
  })
}

benchOp('SELECT 1 (write path)', (d) => d.query('select 1'))
benchOp('param $1 (write path)', (d) => d.query('select $1::int', [n++]))
benchOp('prepared $1 (write path, cached stmt)', (d) => d.query('select $1::int', [n++], { name: 'p' }))
benchOp('100-row · array (decode path)', (d) => d.query(ROWS))
benchOp('100-row · object (decode path)', (d) => d.query(ROWS, [], { mode: 'object' }))

await run()
for (const name of NAMES) await drivers[name]!.end()
await mock.close()
process.exit(0)
