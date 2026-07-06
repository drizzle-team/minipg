// The insert efficiency ladder, measured end-to-end against the local test cluster
// (bun run test:setup first). Everything minipg runs through the PUBLIC API with zero src/
// changes; COPY goes through the raw playground client (playground/inserts/copy-client.ts).
//
//   bun playground/inserts/inserts.bench.ts            # N = 1k, 10k, 100k
//   bun playground/inserts/inserts.bench.ts 100,1000   # custom row counts
//
// Timed section = rows-in-memory -> COMMIT confirmed (client-side encoding included).
// TRUNCATE between runs is untimed. Median of RUNS runs after one warmup.
import { connect } from '../../src/index.ts'
import { Client } from 'pg'
import postgres from 'postgres'
import { RawPg } from './copy-client.ts'
import { arrLitPlain, arrLitQuoted, copyTextChunks, copyBinaryChunks, type Row } from './encode.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const SOCKDIR = '/tmp/minipg_sock'
const PORT = 54329
const DB = { user: 'postgres', database: 'testdb' }

const NS = (process.argv[2] ?? '1000,10000,100000').split(',').map(Number)
const SEQ_MAX = 10_000 // sequential (round-trip-bound) strategies get too slow past this

const COLS = 'id,name,qty,price,flag,created_at'
const INS = `insert into ins_bench(${COLS}) values ($1,$2,$3,$4,$5,$6)`
const UNNEST = `insert into ins_bench(${COLS}) select * from unnest($1::int8[],$2::text[],$3::int4[],$4::float8[],$5::bool[],$6::timestamptz[])`
function mvSql(c: number): string {
  const vals: string[] = new Array(c)
  for (let i = 0; i < c; i++) {
    const b = i * 6
    vals[i] = `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`
  }
  return `insert into ins_bench(${COLS}) values ` + vals.join(',')
}
const MV100 = mvSql(100)
const MV1000 = mvSql(1000)

const BASE = Date.UTC(2026, 0, 1)
function makeRows(n: number): Row[] {
  const rows: Row[] = new Array(n)
  for (let i = 0; i < n; i++) {
    rows[i] = {
      id: i + 1,
      name: `name_${i}_${(i * 2654435761 % 100000).toString(36)}`,
      qty: i % 1000,
      price: (i % 90000) + 0.25,
      flag: i % 2 === 0,
      created_at: new Date(BASE + (i % 86400) * 1000),
    }
  }
  return rows
}
const params6 = (r: Row): unknown[] => [r.id, r.name, r.qty, r.price, r.flag, r.created_at]

// ---------- connections ----------
const mc = await connect({ path: SOCK, ...DB })
const pgc = new Client({ host: SOCKDIR, port: PORT, ...DB })
await pgc.connect()
const pgjs = postgres({ host: SOCKDIR, port: PORT, username: DB.user, database: DB.database, max: 1 })
const raw = await RawPg.connect({ unix: SOCK, ...DB })

await mc.query('drop table if exists ins_bench')
await mc.query(`create table ins_bench(
  id int8 primary key, name text not null, qty int4 not null,
  price float8 not null, flag bool not null, created_at timestamptz not null)`)

// NOTE: this bench originally needed a "prime each name once" block here — pipelining N first
// uses of one named statement crashed with 42P05 (duplicate Parse). That driver bug is fixed
// (parseInflight dedup in src/connection.ts); running un-primed doubles as the regression check.

// ---------- strategies ----------
interface Strat { name: string; maxN?: number; run: (rows: Row[]) => Promise<void> }

async function mvChunked(rows: Row[], c: number, sql: string, name: string): Promise<void> {
  await mc.begin(async (tx) => {
    const ps: Promise<unknown>[] = []
    let i = 0
    for (; i + c <= rows.length; i += c) {
      const flat: unknown[] = new Array(c * 6)
      for (let j = 0; j < c; j++) {
        const r = rows[i + j]!, b = j * 6
        flat[b] = r.id; flat[b + 1] = r.name; flat[b + 2] = r.qty
        flat[b + 3] = r.price; flat[b + 4] = r.flag; flat[b + 5] = r.created_at
      }
      ps.push(tx.query(sql, flat, { name }))
    }
    if (i < rows.length) { // remainder: distinct SQL text -> unnamed one-shot
      const rest = rows.slice(i)
      ps.push(tx.query(mvSql(rest.length), rest.flatMap(params6)))
    }
    await Promise.all(ps)
  })
}

const STRATS: Strat[] = [
  {
    name: 'minipg · seq prepared, autocommit', maxN: SEQ_MAX,
    run: async (rows) => { for (const r of rows) await mc.query(INS, params6(r), { name: 'i_seq' }) },
  },
  {
    name: 'minipg · seq prepared, one tx', maxN: SEQ_MAX,
    run: (rows) => mc.begin(async (tx) => { for (const r of rows) await tx.query(INS, params6(r), { name: 'i_seq' }) }),
  },
  {
    name: 'minipg · pipelined 1-row, one tx', maxN: 200_000, // 1M in-flight promises is memory noise, not signal
    run: (rows) => mc.begin(async (tx) => {
      const ps: Promise<unknown>[] = new Array(rows.length)
      for (let i = 0; i < rows.length; i++) ps[i] = tx.query(INS, params6(rows[i]!), { name: 'i_pipe' })
      await Promise.all(ps)
    }),
  },
  { name: 'minipg · VALUES ×100, one tx', run: (rows) => mvChunked(rows, 100, MV100, 'mv100') },
  { name: 'minipg · VALUES ×1000, one tx', run: (rows) => mvChunked(rows, 1000, MV1000, 'mv1000') },
  {
    name: 'minipg · unnest arrays, 1 stmt', maxN: 200_000, // a 1M-row text-literal statement is ~200MB of strings
    run: async (rows) => {
      await mc.query(UNNEST, [
        arrLitPlain(rows, (r) => r.id),
        arrLitQuoted(rows, (r) => r.name),
        arrLitPlain(rows, (r) => r.qty),
        arrLitPlain(rows, (r) => r.price),
        arrLitPlain(rows, (r) => r.flag),
        arrLitQuoted(rows, (r) => r.created_at.toISOString()),
      ], { name: 'un' })
    },
  },
  {
    // giant array literals degrade past ~10k rows (multi-MB strings, server array parsing);
    // chunking keeps the per-statement size at the sweet spot while STILL using one prepared stmt
    name: 'minipg · unnest ×10k chunks, one tx',
    run: (rows) => mc.begin(async (tx) => {
      const ps: Promise<unknown>[] = []
      for (let i = 0; i < rows.length; i += 10_000) {
        const part = rows.slice(i, i + 10_000)
        ps.push(tx.query(UNNEST, [
          arrLitPlain(part, (r) => r.id),
          arrLitQuoted(part, (r) => r.name),
          arrLitPlain(part, (r) => r.qty),
          arrLitPlain(part, (r) => r.price),
          arrLitPlain(part, (r) => r.flag),
          arrLitQuoted(part, (r) => r.created_at.toISOString()),
        ], { name: 'un' }))
      }
      await Promise.all(ps)
    }),
  },
  {
    // the shipped helper: unnest + declared types -> binary array params from execution #1.
    // Rows pass through as RECORDS (no per-row conversion in the timed section); auto-chunks
    // at 10k rows/statement inside one transaction.
    name: 'minipg · bulkInsert (unnest binary)',
    run: async (rows) => {
      await mc.bulkInsert('ins_bench', { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' }, rows)
    },
  },
  {
    // the shipped COPY path: binary row encoding + drain-backpressured CopyData pump
    name: 'minipg · copyMany (binary)',
    run: async (rows) => {
      await mc.copyMany('ins_bench', { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' }, rows)
    },
  },
  {
    name: 'minipg · copyMany (text)',
    run: async (rows) => {
      await mc.copyMany('ins_bench', { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' }, rows, { format: 'text' })
    },
  },
  {
    // WAL-friendly mode: one COPY per 100k rows, each COMMITTING on its own
    name: 'minipg · copyMany (binary ×100k, atomic:false)',
    run: async (rows) => {
      await mc.copyMany('ins_bench', { id: 'int8', name: 'text', qty: 'int4', price: 'float8', flag: 'bool', created_at: 'timestamptz' }, rows, { chunk: 100_000, atomic: false })
    },
  },
  {
    name: 'COPY text (raw prototype)',
    run: async (rows) => { await raw.copyIn('copy ins_bench from stdin', copyTextChunks(rows)) },
  },
  {
    name: 'COPY binary (raw prototype)',
    run: async (rows) => { await raw.copyIn('copy ins_bench from stdin (format binary)', copyBinaryChunks(rows)) },
  },
  {
    name: 'pg · seq prepared, one tx', maxN: SEQ_MAX,
    run: async (rows) => {
      await pgc.query('begin')
      for (const r of rows) await pgc.query({ name: 'pg_i', text: INS, values: params6(r) })
      await pgc.query('commit')
    },
  },
  {
    name: 'postgres.js · helper ×1000, one tx',
    run: (rows) => pgjs.begin(async (sql) => {
      for (let i = 0; i < rows.length; i += 1000) {
        const chunk = rows.slice(i, i + 1000).map((r) => ({
          id: r.id, name: r.name, qty: r.qty, price: r.price, flag: r.flag, created_at: r.created_at,
        }))
        await sql`insert into ins_bench ${sql(chunk)}`
      }
    }) as unknown as Promise<void>,
  },
]

// ---------- harness ----------
// hygiene between timed runs: force the WAL/dirty-page debt of the PREVIOUS load to flush now,
// not during the next timed section (1M-row lanes otherwise contaminate whoever runs next)
const sync = async (): Promise<void> => { await mc.query('checkpoint'); await Bun.sleep(500) }
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!
const results: { strat: string; n: number; ms: number }[] = []

for (const n of NS) {
  const rows = makeRows(n)
  const runs = n >= 100_000 ? 3 : 5
  console.log(`\n== ${n.toLocaleString()} rows (median of ${runs}) ==`)
  for (const s of STRATS) {
    if (s.maxN && n > s.maxN) { console.log(`  ${s.name.padEnd(38)} skipped (> ${s.maxN.toLocaleString()} rows)`); continue }
    await mc.query('truncate ins_bench')
    await sync() // checkpoint + settle: flush prior lanes' WAL/dirty-page debt so runs don't influence each other
    await s.run(rows) // warmup (also primes prepared statements + plancache)
    const got = await mc.query('select count(*)::int4 from ins_bench')
    const count = (got.rows[0] as unknown as number[])[0]
    if (count !== n) throw new Error(`${s.name}: expected ${n} rows, got ${count}`)
    const times: number[] = []
    for (let r = 0; r < runs; r++) {
      await mc.query('truncate ins_bench')
      await sync()
      const t0 = performance.now()
      await s.run(rows)
      times.push(performance.now() - t0)
    }
    const ms = median(times)
    results.push({ strat: s.name, n, ms })
    console.log(`  ${s.name.padEnd(38)} ${ms.toFixed(1).padStart(9)} ms   ${Math.round(n / (ms / 1000)).toLocaleString().padStart(11)} rows/s`)
  }
}

// markdown summary (paste into README)
console.log('\n| strategy | ' + NS.map((n) => `${n.toLocaleString()} rows`).join(' | ') + ' |')
console.log('|---|' + NS.map(() => '---:').join('|') + '|')
for (const s of STRATS) {
  const cells = NS.map((n) => {
    const r = results.find((x) => x.strat === s.name && x.n === n)
    return r ? `${Math.round(n / (r.ms / 1000)).toLocaleString()}/s` : '—'
  })
  console.log(`| ${s.name} | ${cells.join(' | ')} |`)
}

await mc.query('drop table ins_bench')
await mc.end(); await pgc.end(); await pgjs.end({ timeout: 5 }); raw.end()
process.exit(0)
