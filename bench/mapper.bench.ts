// Microbenchmark for THE MAPPER in isolation: DataRow body buffers -> JS rows. No socket, no server,
// no protocol framing — just decode. DataRow bodies are pre-built with the wire helper at module
// scope, and the JIT builders are compiled once outside the timed fns, so each bench measures ONLY
// value decode + row construction. Compares the interpreted mapper (src/decode.ts) against the
// codegen mapper (src/inline/codegen.ts), array vs object, per-row vs whole-set.
//   bun bench/mapper.bench.ts   (or: bun run bench:mapper)
import { run, bench, group, summary, do_not_optimize } from 'mitata'
import { decodeRows } from '../src/decode.ts'
import { buildDecoders, decoderFor } from '../src/codec.ts'
import { compileRow, compileResultSet } from '../src/inline/codegen.ts'
import { compileResultSet as compileResultSet2 } from '../src/inline/decode2.ts' // v2 (latin1 opt)
import { buildDecoders as buildDecodersJit } from '../src/inline/codec.ts'
import type { Field } from '../src/types.ts'
import * as wire from '../test/helpers/wire.ts'

const fld = (name: string, oid: number): Field => ({ name, tableOid: 0, columnId: 0, dataTypeOid: oid, dataTypeSize: -1, typeModifier: -1, format: 0 })
const mapI = buildDecoders(), mapJ = buildDecodersJit()

// ONE shape, built ONLY from latin1-optimizable column types (int8, numeric, float8, date,
// timestamp, timestamptz, interval, uuid) so the v1→v2 latin1 gain shows undiluted — every sliced
// column benefits, nothing is UTF-8-bound. A separate all-types bench comes later.
const SHAPES = {
  latin1: {
    cols: [
      { name: 'big', oid: 20 },     // int8
      { name: 'amount', oid: 1700 }, // numeric
      { name: 'score', oid: 701 },  // float8
      { name: 'd', oid: 1082 },     // date
      { name: 'ts', oid: 1114 },    // timestamp
      { name: 'tstz', oid: 1184 },  // timestamptz
      { name: 'iv', oid: 1186 },    // interval
      { name: 'u', oid: 2950 },     // uuid
    ],
    cells: (i: number) => [
      String(9007199254740000 + i),
      (i % 1000) + '.2500',
      '3.14159' + (i % 10),
      '2021-06-01',
      '2021-06-01 12:34:56.789',
      '2021-06-01 12:34:56.789+00',
      '1 year 2 mons 3 days 04:05:06',
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    ] as wire.Cell[],
  },
  // less-common ASCII types (network/pg_lsn/timetz/bit/geometric/ranges): v1 routes these to the
  // UTF-8 helper closure, so v2 gains from BOTH inlining (no subarray/closure) AND latin1.
  uncommon: {
    cols: [
      { name: 'mac', oid: 774 },   // macaddr8
      { name: 'ip', oid: 869 },    // inet
      { name: 'net', oid: 650 },   // cidr
      { name: 'lsn', oid: 3220 },  // pg_lsn
      { name: 'ttz', oid: 1266 },  // timetz
      { name: 'r4', oid: 3904 },   // int4range
      { name: 'rtz', oid: 3910 },  // tstzrange
      { name: 'mr', oid: 4451 },   // int4multirange
      { name: 'pt', oid: 600 },    // point
      { name: 'poly', oid: 604 },  // polygon
      { name: 'bits', oid: 1560 }, // bit
    ],
    cells: (i: number) => [
      '08:00:2b:01:02:03:04:05',
      '192.168.0.' + (i % 256),
      '10.0.0.0/8',
      '16/B374D8' + (i % 100),
      '12:34:56+02',
      '[' + (i % 100) + ',' + ((i % 100) + 10) + ')',
      '[2021-01-01 00:00:00+00,2021-06-01 00:00:00+00)',
      '{[1,5),[10,20)}',
      '(' + (i % 10) + ',2)',
      '((0,0),(1,1),(2,0))',
      '101',
    ] as wire.Cell[],
  },
}

function bodies(shape: typeof SHAPES.latin1, n: number): Buffer[] {
  return wire.dataRows(Array.from({ length: n }, (_, i) => shape.cells(i)))
}

for (const [shapeName, shape] of Object.entries(SHAPES)) {
  const fields = shape.cols.map((c) => fld(c.name, c.oid))
  const decs = shape.cols.map((c) => decoderFor(c.oid, mapI))
  const jitSetObj = compileResultSet(shape.cols, 'object', mapJ)          // v1 (utf8)
  const jitSetObj2 = compileResultSet2(shape.cols, 'object', mapJ)        // v2 (latin1 for ASCII types)
  const jitSetArr = compileResultSet(shape.cols, 'array', mapJ)
  const jitRowObj = compileRow(shape.cols, 'object', mapJ)

  for (const n of [100, 1000]) {
    const b = bodies(shape, n)
    // parity guard: never benchmark a v2 that decodes differently from v1
    if (JSON.stringify(jitSetObj(b)) !== JSON.stringify(jitSetObj2(b))) throw new Error(`decode1 != decode2 for shape ${shapeName}`)

    // the focused comparison the request is about: JIT object v1 vs v2
    group(`${shapeName} · ${n} rows × ${shape.cols.length} cols · JIT OBJECT: decode1 vs decode2 (latin1)`, () => {
      summary(() => {
        bench('decode1 JIT object (utf8)', () => do_not_optimize(jitSetObj(b))).gc('inner')
        bench('decode2 JIT object (latin1)', () => do_not_optimize(jitSetObj2(b))).gc('inner')
      })
    })

    group(`${shapeName} · ${n} rows × ${shape.cols.length} cols · mapper only (context)`, () => {
      summary(() => {
        bench('interp decodeRows (array)', () => do_not_optimize(decodeRows(b, 'array', fields, decs))).gc('inner')
        bench('interp decodeRows (object)', () => do_not_optimize(decodeRows(b, 'object', fields, decs))).gc('inner')
        bench('JIT compileResultSet (array)', () => do_not_optimize(jitSetArr(b))).gc('inner')
        bench('JIT compileResultSet (object)', () => do_not_optimize(jitSetObj(b))).gc('inner')
        bench('JIT per-row compileRow (object)', () => { const r = new Array(n); for (let i = 0; i < n; i++) r[i] = jitRowObj(b[i]!); do_not_optimize(r) }).gc('inner')
      })
    })
  }
}

// --- float/double + numeric:number decode: decode2's exact Clinger fast path (f64FromBytes) vs
//     decode1's Number(utf8Slice). Both are correctly rounded, so decode2 is bit-identical to decode1
//     (STRICT parity guard) — we're only measuring the speed of exact-fast bytes->f64 vs Number(). ---
{
  const numCols: Array<{ name: string; oid: number; js?: 'number' }> = [
    { name: 'big', oid: 20, js: 'number' },      // bigint:number (intFromBytes in both)
    { name: 'amount', oid: 1700, js: 'number' }, // numeric:number
    { name: 'dbl', oid: 701 },                   // float8
    { name: 'flt', oid: 700 },                   // float4
  ]
  const nv1 = compileResultSet(numCols, 'object', mapJ)   // decode1: Number(utf8) / intFromBytes
  const nv2 = compileResultSet2(numCols, 'object', mapJ)  // decode2: exact Clinger f64FromBytes / intFromBytes
  const mk = (n: number) => wire.dataRows(Array.from({ length: n }, (_, i) => [String(9007199254740000 + i), (i % 1000) + '.2500', '3.1415926535', '2.5' + (i % 10)] as wire.Cell[]))
  for (const n of [100, 1000]) {
    const b = mk(n)
    if (JSON.stringify(nv1(b)) !== JSON.stringify(nv2(b))) throw new Error('number decode: decode2 not bit-identical to decode1')
    group(`float/numeric:number · ${n} rows × ${numCols.length} cols · decode1 vs decode2 (both exact)`, () => {
      summary(() => {
        bench('decode1 (Number utf8)', () => do_not_optimize(nv1(b))).gc('inner')
        bench('decode2 (exact bytes->f64)', () => do_not_optimize(nv2(b))).gc('inner')
      })
    })
  }
}

// --- temporal: default string (exact, keeps micros) vs opt-in :date (JS Date) / :epoch (ms number),
//     both built by decode2's direct Date.UTC parse. :date/:epoch truncate micros -> ms. ---
{
  const tsStr = compileResultSet2([{ name: 't', oid: 1184 }], 'object', mapJ)
  const tsDate = compileResultSet2([{ name: 't', oid: 1184, js: 'date' }], 'object', mapJ)
  const tsEpoch = compileResultSet2([{ name: 't', oid: 1184, js: 'epoch' }], 'object', mapJ)
  const mk = (n: number) => wire.dataRows(Array.from({ length: n }, (_, i) => [`2021-06-${String(1 + (i % 28)).padStart(2, '0')} 12:34:${String(i % 60).padStart(2, '0')}.789+00`] as wire.Cell[]))
  for (const n of [100, 1000]) {
    const b = mk(n)
    group(`temporal timestamptz · ${n} rows · string vs :date vs :epoch`, () => {
      summary(() => {
        bench('string (default: exact + micros)', () => do_not_optimize(tsStr(b))).gc('inner')
        bench(':epoch (ms number)', () => do_not_optimize(tsEpoch(b))).gc('inner')
        bench(':date (JS Date)', () => do_not_optimize(tsDate(b))).gc('inner')
      })
    })
  }
}

await run()
