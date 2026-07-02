// Unit tests for THE MAPPER in isolation: DataRow body buffer -> JS row. No socket, no protocol.
// Every case is decoded by BOTH implementations — the interpreted mapper (src/decode.ts decodeRow)
// and the JIT/codegen mapper (src/inline/codegen.ts compileRow) — and both must equal the expected
// JS value. So this doubles as a cross-variant parity check at the decode layer. Buffers are built
// with the compact wire helper (test/helpers/wire.ts) using PG's exact text-protocol representation.
import { test, expect, describe } from 'bun:test'
import { decodeRow, decodeRows } from '../../src/decode.ts'
import { buildDecoders, decoderFor } from '../../src/codec.ts'
import { compileRow, compileResultSet } from '../../src/inline/codegen.ts'
import { buildDecoders as buildDecodersJit } from '../../src/inline/codec.ts'
import type { Field } from '../../src/types.ts'
import * as wire from '../helpers/wire.ts'

const mapI = buildDecoders()       // interpreted: Map<oid, CellDecoder>
const mapJ = buildDecodersJit()    // JIT: Map<oid, Decoder>
const fld = (name: string, oid: number): Field => ({ name, tableOid: 0, columnId: 0, dataTypeOid: oid, dataTypeSize: -1, typeModifier: -1, format: 0 })

// decode a single-column, single-row value with each mapper (object mode)
const interpObj = (oid: number, cell: wire.Cell) => (decodeRow(wire.dataRow([cell]), 'object', [fld('c', oid)], [decoderFor(oid, mapI)]) as Record<string, unknown>).c
const jitObj = (oid: number, cell: wire.Cell) => (compileRow([{ name: 'c', oid }], 'object', mapJ)(wire.dataRow([cell])) as Record<string, unknown>).c

// PG OIDs
const OID = {
  bool: 16, bytea: 17, name: 19, int8: 20, int2: 21, int4: 23, text: 25, oid: 26, json: 114, xml: 142,
  float4: 700, float8: 701, unknown: 705, money: 790, point: 600, inet: 869, cidr: 650, macaddr: 829,
  bpchar: 1042, varchar: 1043, date: 1082, time: 1083, timestamp: 1114, timestamptz: 1184, interval: 1186,
  timetz: 1266, bit: 1560, varbit: 1562, numeric: 1700, uuid: 2950, jsonb: 3802, tsvector: 3614,
  int4arr: 1007, textarr: 1009, int8arr: 1016, boolarr: 1000, numericarr: 1231,
} as const

// [label, oid, wire text (PG text format), expected JS]
const CASES: Array<[string, number, wire.Cell, unknown]> = [
  // booleans
  ['bool true', OID.bool, 't', true],
  ['bool false', OID.bool, 'f', false],
  // small/regular integers -> JS number (digit-parsed from bytes)
  ['int2 max', OID.int2, '32767', 32767],
  ['int2 min', OID.int2, '-32768', -32768],
  ['int4 max', OID.int4, '2147483647', 2147483647],
  ['int4 min', OID.int4, '-2147483648', -2147483648],
  ['int4 zero', OID.int4, '0', 0],
  ['oid (unsigned)', OID.oid, '4294967295', 4294967295],
  // 64-bit / exact numerics -> STRING (precision-safe; f64 would corrupt)
  ['int8 max', OID.int8, '9223372036854775807', '9223372036854775807'],
  ['int8 min', OID.int8, '-9223372036854775808', '-9223372036854775808'],
  ['numeric', OID.numeric, '12345.678900', '12345.678900'],
  ['numeric huge', OID.numeric, '100000000000000000000.0000000001', '100000000000000000000.0000000001'],
  ['money', OID.money, '$1,234.56', '$1,234.56'],
  // floats -> JS number
  ['float4', OID.float4, '3.5', 3.5],
  ['float8', OID.float8, '3.141592653589793', 3.141592653589793],
  ['float8 negative', OID.float8, '-0.5', -0.5],
  ['float8 exponent', OID.float8, '1.5e300', 1.5e300],
  // strings (text/varchar/bpchar/name) -> as-is
  ['text', OID.text, 'hello world', 'hello world'],
  ['text empty', OID.text, '', ''],
  ['text unicode', OID.text, 'café 😀 € 日本', 'café 😀 € 日本'],
  ['varchar', OID.varchar, 'vc', 'vc'],
  ['bpchar', OID.bpchar, 'ch  ', 'ch  '],
  ['name', OID.name, 'pg_class', 'pg_class'],
  // temporal (all -> exact string; Date conversion is lossy)
  ['date', OID.date, '2021-06-01', '2021-06-01'],
  ['time', OID.time, '12:34:56.789', '12:34:56.789'],
  ['timetz', OID.timetz, '12:34:56+02', '12:34:56+02'],
  ['timestamp', OID.timestamp, '2021-06-01 12:34:56.123456', '2021-06-01 12:34:56.123456'],
  ['timestamptz', OID.timestamptz, '2021-06-01 10:34:56.123456+00', '2021-06-01 10:34:56.123456+00'],
  ['interval', OID.interval, '1 year 2 mons 3 days 04:05:06', '1 year 2 mons 3 days 04:05:06'],
  // uuid / network / bit / xml / geometric / tsvector -> string
  ['uuid', OID.uuid, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'],
  ['inet', OID.inet, '192.168.0.1/24', '192.168.0.1/24'],
  ['cidr', OID.cidr, '10.0.0.0/8', '10.0.0.0/8'],
  ['macaddr', OID.macaddr, '08:00:2b:01:02:03', '08:00:2b:01:02:03'],
  ['bit', OID.bit, '101', '101'],
  ['varbit', OID.varbit, '10110', '10110'],
  ['xml', OID.xml, '<a>1</a>', '<a>1</a>'],
  ['point', OID.point, '(1,2)', '(1,2)'],
  ['tsvector', OID.tsvector, "'cat':1 'sat':2", "'cat':1 'sat':2"],
  // arrays -> raw array literal string (no array parsing in scope)
  ['int4[]', OID.int4arr, '{1,2,3}', '{1,2,3}'],
  ['int8[]', OID.int8arr, '{1,2,3}', '{1,2,3}'],
  ['text[]', OID.textarr, '{a,b,"c d"}', '{a,b,"c d"}'],
  ['bool[]', OID.boolarr, '{t,f,t}', '{t,f,t}'],
  ['numeric[]', OID.numericarr, '{1.5,2.5}', '{1.5,2.5}'],
  ['empty array', OID.int4arr, '{}', '{}'],
  ['array with NULL', OID.int4arr, '{1,NULL,3}', '{1,NULL,3}'],
  // json / jsonb -> parsed JS
  ['json object', OID.json, '{"a":1,"b":"x"}', { a: 1, b: 'x' }],
  ['jsonb object', OID.jsonb, '{"a": 1, "b": [2, 3]}', { a: 1, b: [2, 3] }],
  ['json array', OID.json, '[1,2,3]', [1, 2, 3]],
  ['json scalar', OID.json, '42', 42],
  ['json null', OID.json, 'null', null],
  ['json unicode', OID.json, '{"k":"café 😀"}', { k: 'café 😀' }],
  // geometric / range / multirange / text-search / lsn / reg* — all lossless raw text (string)
  ['point', OID.point, '(1,2)', '(1,2)'],
  ['circle', 718, '<(1,2),3>', '<(1,2),3>'],
  ['box (server reorders)', 603, '(1,1),(0,0)', '(1,1),(0,0)'],
  ['polygon', 604, '((0,0),(1,1),(2,0))', '((0,0),(1,1),(2,0))'],
  ['int4range', 3904, '[1,10)', '[1,10)'],
  ['int4range empty', 3904, 'empty', 'empty'],
  ['numrange unbounded', 3906, '[1.5,)', '[1.5,)'],
  ['int4multirange', 4451, '{[1,5),[10,20)}', '{[1,5),[10,20)}'],
  ['tsquery', 3615, "'fat' <-> 'cat':AB & !'mouse'", "'fat' <-> 'cat':AB & !'mouse'"],
  ['jsonpath', 4072, '$."a"[*]?(@ >= 10.5)', '$."a"[*]?(@ >= 10.5)'],
  ['pg_lsn', 3220, '16/B374D848', '16/B374D848'],
  ['macaddr8', 774, '08:00:2b:01:02:03:04:05', '08:00:2b:01:02:03:04:05'],
  ['regtype', 2206, 'integer', 'integer'],
  ['multidim int2[]', 1005, '{{1,2,3},{4,5,6}}', '{{1,2,3},{4,5,6}}'],
  ['void (empty, not null)', 2278, '', ''],
  // unknown OID (705) falls back to string
  ['unknown -> string', OID.unknown, 'whatever', 'whatever'],
]

describe('per-type decode — interpreted and JIT agree with expected (object mode)', () => {
  for (const [label, oid, cell, want] of CASES) {
    test(`${label} (oid ${oid})`, () => {
      expect(interpObj(oid, cell)).toEqual(want)
      expect(jitObj(oid, cell)).toEqual(want)
    })
  }
})

describe('bytea -> Buffer', () => {
  test('hex format decodes to the exact bytes (both)', () => {
    const want = Buffer.from('deadbeef', 'hex')
    expect(interpObj(OID.bytea, '\\xdeadbeef')).toEqual(want)
    expect(jitObj(OID.bytea, '\\xdeadbeef')).toEqual(want)
  })
  test('empty bytea (both)', () => {
    expect(interpObj(OID.bytea, '\\x')).toEqual(Buffer.alloc(0))
    expect(jitObj(OID.bytea, '\\x')).toEqual(Buffer.alloc(0))
  })
})

describe('float non-finite specials', () => {
  const nonfinite: Array<[string, string, number]> = [['NaN', 'NaN', NaN], ['Infinity', 'Infinity', Infinity], ['-Infinity', '-Infinity', -Infinity]]
  for (const [label, cell, want] of nonfinite) {
    test(`float8 ${label} (both)`, () => {
      const i = interpObj(OID.float8, cell) as number, j = jitObj(OID.float8, cell) as number
      if (Number.isNaN(want)) { expect(Number.isNaN(i)).toBe(true); expect(Number.isNaN(j)).toBe(true) }
      else { expect(i).toBe(want); expect(j).toBe(want) }
    })
  }
})

describe('NULL handling', () => {
  test('every column NULL -> null (both)', () => {
    const cols = [{ name: 'a', oid: OID.int4 }, { name: 'b', oid: OID.text }, { name: 'c', oid: OID.bool }, { name: 'd', oid: OID.jsonb }]
    const body = wire.dataRow([null, null, null, null])
    const want = { a: null, b: null, c: null, d: null }
    expect(decodeRow(body, 'object', cols.map((c) => fld(c.name, c.oid)), cols.map((c) => decoderFor(c.oid, mapI)))).toEqual(want)
    expect(compileRow(cols, 'object', mapJ)(body)).toEqual(want)
  })
  test('NULL vs empty string are distinct (both)', () => {
    expect(interpObj(OID.text, null)).toBe(null)
    expect(interpObj(OID.text, '')).toBe('')
    expect(jitObj(OID.text, null)).toBe(null)
    expect(jitObj(OID.text, '')).toBe('')
  })
})

describe('result modes (interpreted supports all four; JIT covers array/object)', () => {
  const cols = [{ name: 'id', oid: OID.int4 }, { name: 'name', oid: OID.text }]
  const fields = cols.map((c) => fld(c.name, c.oid))
  const decs = cols.map((c) => decoderFor(c.oid, mapI))
  const body = wire.dataRow(['7', 'hi'])
  test('object', () => {
    expect(decodeRow(body, 'object', fields, decs)).toEqual({ id: 7, name: 'hi' })
    expect(compileRow(cols, 'object', mapJ)(body)).toEqual({ id: 7, name: 'hi' })
  })
  test('array', () => {
    expect(decodeRow(body, 'array', fields, decs)).toEqual([7, 'hi'])
    expect(compileRow(cols, 'array', mapJ)(body)).toEqual([7, 'hi'])
  })
  test('buffer -> per-cell Buffers (interp)', () => {
    expect(decodeRow(body, 'buffer', fields, decs)).toEqual([Buffer.from('7'), Buffer.from('hi')])
  })
  test('buffer NULL cell -> null (interp)', () => {
    expect(decodeRow(wire.dataRow(['7', null]), 'buffer', fields, decs)).toEqual([Buffer.from('7'), null])
  })
  test('raw -> a copy of the body bytes (interp)', () => {
    const raw = decodeRow(body, 'raw', fields, decs) as Buffer
    expect(raw).toEqual(body)
    expect(raw).not.toBe(body) // detached copy
  })
})

describe('__proto__ column is an own property, never prototype pollution', () => {
  const cols = [{ name: '__proto__', oid: OID.int4 }]
  const body = wire.dataRow(['1'])
  test('interpreted', () => {
    const row = decodeRow(body, 'object', [fld('__proto__', OID.int4)], [decoderFor(OID.int4, mapI)]) as object
    expect(Object.getOwnPropertyDescriptor(row, '__proto__')?.value).toBe(1)
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype)
  })
  test('JIT', () => {
    const row = compileRow(cols, 'object', mapJ)(body) as object
    expect(Object.getOwnPropertyDescriptor(row, '__proto__')?.value).toBe(1)
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype)
  })
})

describe('multi-column mixed-type row (both)', () => {
  const cols = [
    { name: 'id', oid: OID.int4 }, { name: 'big', oid: OID.int8 }, { name: 'name', oid: OID.text },
    { name: 'ok', oid: OID.bool }, { name: 'score', oid: OID.float8 }, { name: 'meta', oid: OID.jsonb }, { name: 'nada', oid: OID.text },
  ]
  const body = wire.dataRow(['42', '9007199254740993', 'alice', 't', '2.5', '{"x":1}', null])
  const want = { id: 42, big: '9007199254740993', name: 'alice', ok: true, score: 2.5, meta: { x: 1 }, nada: null }
  test('interpreted', () => {
    expect(decodeRow(body, 'object', cols.map((c) => fld(c.name, c.oid)), cols.map((c) => decoderFor(c.oid, mapI)))).toEqual(want)
  })
  test('JIT', () => {
    expect(compileRow(cols, 'object', mapJ)(body)).toEqual(want)
  })
})

describe('batch / whole-result-set mappers', () => {
  const cols = [{ name: 'id', oid: OID.int4 }, { name: 'ok', oid: OID.bool }, { name: 'name', oid: OID.text }]
  const fields = cols.map((c) => fld(c.name, c.oid))
  const decs = cols.map((c) => decoderFor(c.oid, mapI))
  const bodies = wire.dataRows([['1', 't', 'a'], ['2', 'f', 'b'], ['3', 't', 'c']])
  const want = [{ id: 1, ok: true, name: 'a' }, { id: 2, ok: false, name: 'b' }, { id: 3, ok: true, name: 'c' }]
  test('decodeRows (interpreted)', () => { expect(decodeRows(bodies, 'object', fields, decs)).toEqual(want) })
  test('compileResultSet (JIT)', () => { expect(compileResultSet(cols, 'object', mapJ)(bodies)).toEqual(want) })
  test('empty result set -> [] (both)', () => {
    expect(decodeRows([], 'object', fields, decs)).toEqual([])
    expect(compileResultSet(cols, 'object', mapJ)([])).toEqual([])
  })
})
