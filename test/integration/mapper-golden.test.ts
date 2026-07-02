// GOLDEN cross-check against a live server: proves (1) the wire helper builds byte-identical DataRow
// buffers to what PostgreSQL actually sends, and (2) both mappers decode those REAL bytes to the
// expected JS. We capture PG's exact DataRow body via minipg's `raw` result mode (which returns the
// untouched body), then feed it through the interpreted + JIT mappers and rebuild it with the wire
// helper. Requires the local cluster (`bun run test:setup`).
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { decodeRow } from '../../src/decode.ts'
import { buildDecoders, decoderFor } from '../../src/codec.ts'
import { compileRow } from '../../src/inline/codegen.ts'
import { buildDecoders as buildDecodersJit } from '../../src/inline/codec.ts'
import type { Field } from '../../src/types.ts'
import { testConnect } from '../helpers/db.ts'
import * as wire from '../helpers/wire.ts'

const mapI = buildDecoders(), mapJ = buildDecodersJit()
const fld = (name: string, oid: number): Field => ({ name, tableOid: 0, columnId: 0, dataTypeOid: oid, dataTypeSize: -1, typeModifier: -1, format: 0 })

// pull the raw cell texts back out of a DataRow body (to rebuild it with the wire helper)
function extractCells(body: Buffer): (string | null)[] {
  const n = body.readInt16BE(0); let o = 2; const out: (string | null)[] = []
  for (let i = 0; i < n; i++) { const l = body.readInt32BE(o); o += 4; if (l === -1) { out.push(null); continue } out.push(body.toString('utf8', o, o + l)); o += l }
  return out
}

// [label, SQL (single column, single row), the column's type OID, expected JS]
const GOLD: Array<[string, string, number, unknown]> = [
  ['bool true', 'select true', 16, true],
  ['bool false', 'select false', 16, false],
  ['int2', 'select 32767::int2', 21, 32767],
  ['int4 min', 'select (-2147483648)::int4', 23, -2147483648],
  ['int8 max', 'select 9223372036854775807::int8', 20, '9223372036854775807'],
  ['oid max', 'select 4294967295::oid', 26, 4294967295],
  ['float4', 'select 3.5::float4', 700, 3.5],
  ['float8', 'select 3.141592653589793::float8', 701, 3.141592653589793],
  ['numeric (scale kept)', 'select 12345.678900::numeric', 1700, '12345.678900'],
  ['text', "select 'hello'::text", 25, 'hello'],
  ['text unicode', "select 'café 😀 €'::text", 25, 'café 😀 €'],
  ['text empty', "select ''::text", 25, ''],
  ['varchar', "select 'vc'::varchar(8)", 1043, 'vc'],
  ['uuid', "select 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", 2950, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'],
  ['date', "select '2021-06-01'::date", 1082, '2021-06-01'],
  ['time', "select '12:34:56'::time", 1083, '12:34:56'],
  ['timestamp', "select '2021-06-01 12:34:56'::timestamp", 1114, '2021-06-01 12:34:56'],
  ['interval', "select '1 day'::interval", 1186, '1 day'],
  ['inet', "select '192.168.0.1'::inet", 869, '192.168.0.1'],
  ['int4[]', "select '{1,2,3}'::int4[]", 1007, '{1,2,3}'],
  ['text[]', "select array['a','b']::text[]", 1009, '{a,b}'],
  ['null', 'select null::int4', 23, null],
]

const GOLD_BUF: Array<[string, number, unknown]> = [
  ['bytea', 17, Buffer.from('deadbeef', 'hex')],
]

let c: Awaited<ReturnType<typeof testConnect>>
beforeAll(async () => { c = await testConnect() })
afterAll(async () => { await c?.end() })

// fetch the exact DataRow body PG sends for a one-column, one-row query
const rawBody = async (sql: string): Promise<Buffer> => (await c.query(sql, [], { mode: 'raw' })).rows[0] as Buffer

describe('golden: real PG bytes -> both mappers -> expected JS', () => {
  for (const [label, sql, oid, want] of GOLD) {
    test(`${label} (oid ${oid})`, async () => {
      const raw = await rawBody(sql)
      // (1) both mappers decode the REAL bytes to the expected value
      expect((decodeRow(raw, 'object', [fld('c', oid)], [decoderFor(oid, mapI)]) as Record<string, unknown>).c).toEqual(want)
      expect((compileRow([{ name: 'c', oid }], 'object', mapJ)(raw) as Record<string, unknown>).c).toEqual(want)
      // (2) the wire helper reproduces PG's exact DataRow bytes from the same cell text
      expect(wire.dataRow(extractCells(raw))).toEqual(raw)
    })
  }
})

describe('golden: bytea decodes to a Buffer of the exact bytes', () => {
  for (const [label, oid, want] of GOLD_BUF) {
    test(`${label} (oid ${oid})`, async () => {
      const raw = await rawBody("select '\\xdeadbeef'::bytea")
      expect((decodeRow(raw, 'object', [fld('c', oid)], [decoderFor(oid, mapI)]) as Record<string, unknown>).c).toEqual(want)
      expect((compileRow([{ name: 'c', oid }], 'object', mapJ)(raw) as Record<string, unknown>).c).toEqual(want)
      expect(wire.dataRow(extractCells(raw))).toEqual(raw)
    })
  }
})

describe('golden: json/jsonb decode to parsed JS from real bytes', () => {
  test('json preserves declared key order text; parses equal', async () => {
    const raw = await rawBody(`select '{"a":1,"b":"x"}'::json`)
    const want = { a: 1, b: 'x' }
    expect((decodeRow(raw, 'object', [fld('c', 114)], [decoderFor(114, mapI)]) as Record<string, unknown>).c).toEqual(want)
    expect((compileRow([{ name: 'c', oid: 114 }], 'object', mapJ)(raw) as Record<string, unknown>).c).toEqual(want)
    expect(wire.dataRow(extractCells(raw))).toEqual(raw)
  })
  test('jsonb (keys normalized by server) parses equal', async () => {
    const raw = await rawBody(`select '{"b":[2,3],"a":1}'::jsonb`)
    const want = { a: 1, b: [2, 3] }
    expect((decodeRow(raw, 'object', [fld('c', 3802)], [decoderFor(3802, mapI)]) as Record<string, unknown>).c).toEqual(want)
    expect((compileRow([{ name: 'c', oid: 3802 }], 'object', mapJ)(raw) as Record<string, unknown>).c).toEqual(want)
    expect(wire.dataRow(extractCells(raw))).toEqual(raw)
  })
})

// For lossless-text types (geometric, ranges/multiranges, network, text-search, arrays, reg*, void,
// user-defined) the mapper must return EXACTLY the cell bytes PG sent, as a UTF-8 string. We derive
// that text from the real body (no hand-transcribed literals -> immune to escaping + session TZ),
// then assert both mappers reproduce it and the wire helper rebuilds the exact bytes.
async function checkTextType(sql: string, oid: number) {
  const raw = await rawBody(sql)
  const text = extractCells(raw)[0]
  expect((decodeRow(raw, 'object', [fld('c', oid)], [decoderFor(oid, mapI)]) as Record<string, unknown>).c).toBe(text)
  expect((compileRow([{ name: 'c', oid }], 'object', mapJ)(raw) as Record<string, unknown>).c).toBe(text)
  expect(wire.dataRow(extractCells(raw))).toEqual(raw)
}

// [label, single-column SELECT, result type OID] — harvested from the live server, all decode to
// their raw text. Backslashes are doubled for JS; \\x -> SQL \x, \\ -> SQL \ .
const TEXT_TYPES: Array<[string, string, number]> = [
  ['line', `select '{1,-1,0}'::line`, 628],
  ['lseg', `select '[(0,0),(1,1)]'::lseg`, 601],
  ['box (server reorders)', `select '((0,0),(1,1))'::box`, 603],
  ['path closed', `select '((0,0),(1,1),(2,0))'::path`, 602],
  ['path open', `select '[(0,0),(1,1),(2,0)]'::path`, 602],
  ['polygon', `select '((0,0),(1,1),(2,0))'::polygon`, 604],
  ['circle', `select '<(1,2),3>'::circle`, 718],
  ['macaddr8', `select '08:00:2b:01:02:03:04:05'::macaddr8`, 774],
  ['inet v6', `select '2001:db8::1/64'::inet`, 869],
  ['cidr v6', `select '2001:db8::/32'::cidr`, 650],
  ['int4range empty', `select 'empty'::int4range`, 3904],
  ['int4range unbounded', `select '(,)'::int4range`, 3904],
  ['int8range', `select '[10,9000000000000)'::int8range`, 3926],
  ['numrange', `select '[1.5,2.75]'::numrange`, 3906],
  ['tsrange', `select '[2021-01-01 12:00:00,2021-12-31 23:59:59.5)'::tsrange`, 3908],
  ['tstzrange', `select '[2021-01-01 12:00:00+00,2021-06-01 00:00:00+00)'::tstzrange`, 3910],
  ['daterange', `select '[2021-01-01,2021-12-31)'::daterange`, 3912],
  ['int4multirange', `select '{[1,5),[10,20)}'::int4multirange`, 4451],
  ['int4multirange empty', `select '{}'::int4multirange`, 4451],
  ['nummultirange', `select '{[1.5,3.5),[10,20.25]}'::nummultirange`, 4532],
  ['tsquery', `select 'fat <-> cat:AB & !mouse'::tsquery`, 3615],
  ['tsquery unicode', `select '!café & rät:*A'::tsquery`, 3615],
  ['jsonpath', `select 'strict $."café".値[*] ? (@.x >= 10.5)'::jsonpath`, 4072],
  ['jsonpath like_regex', `select '$.a ? (@ like_regex "^ab.*" flag "i")'::jsonpath`, 4072],
  ['pg_lsn', `select 'FFFFFFFF/FFFFFFFF'::pg_lsn`, 3220],
  ['"char" letter', `select 'A'::"char"`, 18],
  ['"char" backslash byte', `select E'\\\\'::"char"`, 18],
  ['char[]', `select '{a,z,~,1}'::"char"[]`, 1002],
  ['uuid[] with NULL', `select array['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, null]::uuid[]`, 2951],
  ['tsquery[]', `select array['fat & rat'::tsquery, '!cat:*'::tsquery]`, 3645],
  ['jsonpath[]', `select array['$.a'::jsonpath, '$[*]'::jsonpath]`, 4073],
  ['pg_lsn[]', `select array['16/B374D848'::pg_lsn, '0/0'::pg_lsn]`, 3221],
  ['int2[] multi-dim', `select '{{1,2,3},{4,5,6}}'::int2[]`, 1005],
  ['varchar[] escaped/NULL', `select array['a b', 'quote"me', 'comma,here', null, 'back\\slash', 'brace}{']::varchar[]`, 1015],
  ['varchar[] empty', `select '{}'::varchar[]`, 1015],
  ['timestamp[]', `select array['2021-01-01 12:34:56.789'::timestamp, '1999-12-31 23:59:59'::timestamp]::timestamp[]`, 1115],
  ['timestamptz[]', `select array['2021-01-01 12:34:56.789+00'::timestamptz]::timestamptz[]`, 1185],
  ['bytea[]', `select array['\\x00ff'::bytea, '\\xdeadbeef'::bytea]::bytea[]`, 1001],
  ['float8[] specials', `select array[1.5::float8, 'Infinity'::float8, '-Infinity'::float8, 'NaN'::float8]::float8[]`, 1022],
  ['date[] with BC', `select array['2021-01-01'::date, '0001-12-31 BC'::date]::date[]`, 1182],
  ['void', `select pg_sleep(0)`, 2278],
  ['regtype', `select 'int4'::regtype`, 2206],
  ['regclass', `select 'pg_class'::regclass`, 2205],
]

describe('golden: lossless-text types decode to exactly the bytes PG sent', () => {
  for (const [label, sql, oid] of TEXT_TYPES) {
    test(`${label} (oid ${oid})`, async () => { await checkTextType(sql, oid) })
  }
})

describe('golden: user-defined types (enum / composite / domain) decode as their text', () => {
  beforeAll(async () => {
    await c.query(`drop type if exists mtest_mood cascade`); await c.query(`create type mtest_mood as enum ('sad','ok','happy')`)
    await c.query(`drop type if exists mtest_cx cascade`); await c.query(`create type mtest_cx as (r float8, i float8)`)
    await c.query(`drop type if exists mtest_rec cascade`); await c.query(`create type mtest_rec as (a text, b int, c text)`)
    await c.query(`drop domain if exists mtest_mac8 cascade`); await c.query(`create domain mtest_mac8 as macaddr8`)
  })
  afterAll(async () => {
    for (const s of ['drop type if exists mtest_mood cascade', 'drop type if exists mtest_cx cascade', 'drop type if exists mtest_rec cascade', 'drop domain if exists mtest_mac8 cascade']) await c.query(s)
  })
  // oid 0 -> asString (any non-builtin OID decodes as raw text; user-type OIDs are DB-assigned)
  const cases: Array<[string, string]> = [
    ['enum', `select 'happy'::mtest_mood`],
    ['composite float pair', `select row(1.5, -2.25)::mtest_cx`],
    ['composite quoted-comma + NULL field', `select row('he,llo', 5, null)::mtest_rec`],
    ['domain over macaddr8', `select 'aa:bb:cc:dd:ee:ff:00:11'::mtest_mac8`],
  ]
  for (const [label, sql] of cases) {
    test(label, async () => { await checkTextType(sql, 0) })
  }
})

describe('golden: a wide multi-column row round-trips through the wire helper', () => {
  test('mixed types: real bytes == wire.dataRow(extracted)', async () => {
    const sql = `select 1::int4 as id, 9223372036854775807::int8 as big, 'x'::text as name, true as ok, 2.5::float8 as score, '{"k":1}'::jsonb as meta, null::text as nada`
    const raw = await rawBody(sql)
    expect(wire.dataRow(extractCells(raw))).toEqual(raw)
    const cols = [{ name: 'id', oid: 23 }, { name: 'big', oid: 20 }, { name: 'name', oid: 25 }, { name: 'ok', oid: 16 }, { name: 'score', oid: 701 }, { name: 'meta', oid: 3802 }, { name: 'nada', oid: 25 }]
    const want = { id: 1, big: '9223372036854775807', name: 'x', ok: true, score: 2.5, meta: { k: 1 }, nada: null }
    expect(decodeRow(raw, 'object', cols.map((x) => fld(x.name, x.oid)), cols.map((x) => decoderFor(x.oid, mapI)))).toEqual(want)
    expect(compileRow(cols, 'object', mapJ)(raw)).toEqual(want)
  })
})
