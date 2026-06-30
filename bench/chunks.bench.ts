// Query-builder chunks fast path: minipg's query(chunks[], values) vs hand-joining the chunks
// into a string. Real DB over the unix socket (bun run test:setup first).
//
// NOTE: this uses a manual interleaved timing loop, NOT mitata. For async DB round-trips
// mitata's first-bench-in-a-gc('inner')-group is inflated (measurement artifact); a plain
// warmed + interleaved loop is stable and matches the protocol trace (chunks PARSE once then
// REUSE, identical to an explicit {name}).
//   bun bench/chunks.bench.ts   (or: bun run bench:chunks)
import { connect } from '../src/index.ts'

const c = await connect({ path: '/tmp/minipg_sock/.s.PGSQL.54329', user: 'postgres', database: 'testdb' })
const CHUNKS = ['select g, g * ', ' as v from generate_series(1, ', ') g'] // 2 params -> $1,$2
const P = [2, 20]                                  // -> 20-row result
const NAME = { name: 'cbp' }
const join = (ch: readonly string[]) => { let s = ch[0]!; for (let i = 1; i < ch.length; i++) s += '$' + i + ch[i]!; return s }
const PREJOINED = join(CHUNKS)

const cases: [string, () => Promise<unknown>][] = [
  ['chunks reused (auto-prepared)', () => c.query(CHUNKS, P)],
  ['pre-joined string + {name}', () => c.query(PREJOINED, P, NAME)],
  ['join() each call + {name}', () => c.query(join(CHUNKS), P, NAME)],
  ['join() each call + unprepared', () => c.query(join(CHUNKS), P)],
]

const time = async (fn: () => Promise<unknown>, N: number) => { const t0 = Bun.nanoseconds(); for (let i = 0; i < N; i++) await fn(); return (Bun.nanoseconds() - t0) / N / 1000 }
for (const [, fn] of cases) for (let i = 0; i < 2000; i++) await fn() // warm connection + JIT + prepared statements

console.log('builder query reuse (real DB, unix socket, 20-row result) — µs/query, 3 interleaved rounds:\n')
const acc = new Map(cases.map(([n]) => [n, [] as number[]]))
for (let r = 0; r < 3; r++) for (const [name, fn] of cases) acc.get(name)!.push(await time(fn, 8000))
for (const [name, xs] of acc) console.log('  ' + name.padEnd(32), (xs.reduce((a, b) => a + b, 0) / xs.length).toFixed(2), 'µs', '  (' + xs.map((x) => x.toFixed(1)).join(' / ') + ')')

await c.end()
process.exit(0)
