// decode2 — a v2 of the JIT/codegen row mapper (a copy of codegen.ts) used as a playground to
// gradually try performance optimizations against the v1 mapper. Compare with bench/mapper.bench.ts.
//
// OPT #1 — latin1 for ASCII-guaranteed types: PostgreSQL's TEXT representation of int8, numeric,
// float, all temporal types, uuid and bytea(hex) is always ASCII (a subset of latin1). Decoding
// those via latin1Slice / toString('latin1') skips the UTF-8 multibyte scan that utf8Slice does, so
// it's faster while producing an identical string (ASCII bytes map 1:1 under both encodings).
// Types that can carry non-ASCII (text/varchar/bpchar/name/money/xml/json/jsonb, and anything routed
// to the helper closure) stay UTF-8 so unicode is preserved.
import type { Decoder } from './types.ts'
import { decoderFor, defaultDecoders } from './codec.ts'
import { genJsonParsers, type JsonMarker, type JsonPlan, type JsTarget } from './json.ts'

// decode2 extends the JS-target set with temporal INSTANT targets: 'date' -> JS Date, 'epoch' -> ms number.
export type Target = JsTarget | 'date' | 'epoch'
/** A column to decode: name + wire OID, optional JS-target override, shaped-JSON marker, and the WIRE
 *  format the value arrives in ('text' default, or 'binary' when the query requested binary for it). */
export interface CodegenCol { name: string; oid: number; js?: Target; json?: JsonMarker; format?: 'text' | 'binary' }

type AtDecoder = (b: Buffer, o: number, l: number) => unknown
const byteaAt: AtDecoder = (b, o, l) => { const s = b.toString('utf8', o, o + l); return s.startsWith('\\x') ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }

const DEBUG = !!process.env.MINIPG_CODEGEN_DEBUG

// utf8: the general slice (unicode-safe). latin1: faster, ONLY valid for ASCII-guaranteed bytes.
const HAS_UTF8_SLICE = typeof (Buffer.prototype as { utf8Slice?: unknown }).utf8Slice === 'function'
const HAS_LATIN1_SLICE = typeof (Buffer.prototype as { latin1Slice?: unknown }).latin1Slice === 'function'
const str = (from: string, to: string) => (HAS_UTF8_SLICE ? `b.utf8Slice(${from}, ${to})` : `b.toString('utf8', ${from}, ${to})`)
const lat = (from: string, to: string) => (HAS_LATIN1_SLICE ? `b.latin1Slice(${from}, ${to})` : `b.toString('latin1', ${from}, ${to})`)

// Wire OIDs whose text is guaranteed ASCII -> decode via latin1 (see OPT #1). Float is handled in the
// 'number' case (always ASCII). NOT money (locale currency), text/varchar/char/name (unicode), json.
// Common ASCII types — already inlined as 'string' by defaultJs: int8, numeric, all temporal, uuid.
const ASCII_COMMON = [20, 1700, 1082, 1083, 1114, 1184, 1186, 2950]
// Less-common ASCII types that v1 routes to the UTF-8 helper closure — v2 INLINES them as latin1
// (removes the per-cell subarray + closure call too): network, pg_lsn, timetz, bit, geometric, ranges.
const ASCII_EXTRA = [
  774, 829, 650, 869,                 // macaddr8, macaddr, cidr, inet
  1266, 1560, 1562, 3220,             // timetz, bit, varbit, pg_lsn
  600, 601, 602, 603, 604, 628, 718,  // point, lseg, path, box, polygon, line, circle
  3904, 3906, 3908, 3910, 3912, 3926, // ranges: int4, num, ts, tstz, date, int8
  4451, 4532, 4533, 4534, 4535, 4536, // multiranges: int4, num, ts, tstz, date, int8
]
export const ASCII_SAFE = new Set([...ASCII_COMMON, ...ASCII_EXTRA])

// Read the int32 length prefix as a signed big-endian int straight from the four bytes.
const readLen = 'l = (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3]; o += 4;'

// digit-parse straight from ASCII bytes -> JS number (no toString), for integer OIDs. int8 beyond
// 2^53 loses precision (accumulated in f64) — that's the accepted cost of `bigint:number` (ORM mode).
const intFromBytes = (v: string) => `{ let p = o, s = false, x = 0; const e = o + l; if (b[p] === 45) { s = true; p++ } for (; p < e; p++) x = x * 10 + (b[p] - 48); ${v} = s ? -x : x }`

// Exact powers of 10 (1e0..1e22) — all exactly representable in f64. Closed over by the compiled
// builder as `P` for the correctly-rounded fast path below.
const POW10 = [1, 10, 100, 1000, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22]

// OPT #2 — fast AND COMPLETELY PRECISE ASCII -> f64 (Clinger fast path). Parse the digits into an
// INTEGER significand (exact while ≤ 2^53, i.e. ≤ 15 significant digits) and apply a SINGLE multiply/
// divide by an EXACT power of 10 (|exp| ≤ 22): IEEE-754 guarantees that one operation is correctly
// rounded, so the result is bit-identical to Number(). Values outside the fast domain (>15 significant
// digits or |exp| > 22) fall back to Number(latin1) — also exact. No string allocation on the fast
// path. Used for float4/float8 and the numeric:number target (numeric's only loss is the inherent
// f64 rounding, which this reproduces exactly). NaN/±Infinity handled up front.
const f64FromBytes = (v: string) => `{ let p = o; const e = o + l; const c0 = b[p]; let neg = false;
    if (c0 === 45) { neg = true; p++ } else if (c0 === 43) { p++ }
    if (b[p] === 78 || b[p] === 110) { ${v} = NaN }
    else if (b[p] === 73 || b[p] === 105) { ${v} = neg ? -Infinity : Infinity }
    else {
      let sig = 0, nd = 0, fd = 0, dot = false, hard = false;
      for (; p < e; p++) { const c = b[p];
        if (c === 46) { dot = true; continue }
        if (c < 48 || c > 57) break;
        if (sig === 0 && c === 48) { if (dot) fd++; continue }
        if (nd < 15) { sig = sig * 10 + (c - 48); nd++; if (dot) fd++ } else { hard = true }
      }
      let exp = 0, es = 1;
      if (b[p] === 101 || b[p] === 69) { p++; if (b[p] === 45) { es = -1; p++ } else if (b[p] === 43) { p++ } for (; p < e; p++) { const c = b[p]; if (c < 48 || c > 57) break; exp = exp * 10 + (c - 48) } }
      const eff = es * exp - fd;
      if (hard || eff > 22 || eff < -22) { ${v} = Number(${lat('o', 'o + l')}) }
      else { const r = eff >= 0 ? sig * P[eff] : sig / P[-eff]; ${v} = neg ? -r : r }
    } }`

// OPT #3 — temporal INSTANT targets (opt-in via js 'date'/'epoch'). Default stays the exact string
// (fastest + lossless). date/timestamp/timestamptz can map to a JS Date or epoch-ms number; naive
// (no-offset) values are treated as UTC, timestamptz applies its offset. Direct field parse from
// bytes -> Date.UTC (2-4x faster than new Date(text) AND correct — new Date parses no-tz as LOCAL).
// ms = first 3 fractional digits (micros truncated; Date is ms-only). Years < 100 / BC: use string.
export const INSTANT_OIDS = new Set([1082, 1114, 1184]) // date, timestamp, timestamptz
const tsFromBytes = (v: string, kind: 'date' | 'epoch') => `{ let p = o; const e = o + l;
      let Y = 0; for (; p < e; p++) { const c = b[p]; if (c < 48 || c > 57) break; Y = Y * 10 + (c - 48) } p++;
      const Mo = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 3;
      const D = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 2;
      let H = 0, Mi = 0, S = 0, ms = 0, off = 0;
      if (p < e && b[p] === 32) { p++;
        H = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 3;
        Mi = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 3;
        S = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 2;
        if (p < e && b[p] === 46) { p++; let f = 0, k = 0; for (; p < e && k < 3; p++) { const c = b[p]; if (c < 48 || c > 57) break; f = f * 10 + (c - 48); k++ } while (k < 3) { f *= 10; k++ } ms = f; while (p < e) { const c = b[p]; if (c < 48 || c > 57) break; p++ } }
        if (p < e && (b[p] === 43 || b[p] === 45)) { const sg = b[p] === 45 ? -1 : 1; p++; const th = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 2; let tm = 0; if (p < e && b[p] === 58) { p++; tm = (b[p] - 48) * 10 + (b[p + 1] - 48); p += 2 } off = sg * (th * 60 + tm) * 60000 }
      }
      const ems = Date.UTC(Y, Mo - 1, D, H, Mi, S, ms) - off; ${kind === 'date' ? `${v} = new Date(ems)` : `${v} = ems`} }`

// The natural JS target for a wire OID (before any per-column override).
export function defaultJs(oid: number): 'number' | 'string' | 'bool' | 'json' | 'bytea' | 'helper' {
  switch (oid) {
    case 16: return 'bool'
    case 17: return 'bytea'
    case 114: case 3802: return 'json'
    case 21: case 23: case 26: case 700: case 701: return 'number'   // int2/int4/oid + float4/float8 fit a JS number
    case 20: case 1700: case 790: return 'string'                    // int8/numeric/money -> exact string (avoid f64 loss)
    case 18: case 19: case 25: case 1042: case 1043: case 1082: case 1083: case 1114: case 1184: case 1186: case 2950: return 'string'
    // ASCII_EXTRA types (network/pg_lsn/timetz/bit/geometric/ranges) inline as latin1 strings instead
    // of routing to the UTF-8 helper; anything else (real arrays / unknown / custom) stays a helper.
    default: return ASCII_SAFE.has(oid) ? 'string' : 'helper'
  }
}
export const INT_OIDS = new Set([20, 21, 23, 26]) // int8/int2/int4/oid — digit-parseable straight to a number

// Inline decode expression for a column. ASCII-safe types use latin1; unicode-capable ones utf8.
// float4/float8 (and the numeric:number target) decode via the exact Clinger fast path (f64FromBytes).
function inlineSnippet(oid: number, v: string, js?: Target): [string, string] | null {
  if (js === 'date' || js === 'epoch') {
    if (INSTANT_OIDS.has(oid)) return [tsFromBytes(v, js), `temporal :${js}`]
    js = 'string' // :date/:epoch on a non-instant type -> fall back to the exact string
  }
  const eff = js ?? defaultJs(oid)
  switch (eff) {
    case 'number':
      if (INT_OIDS.has(oid)) return [intFromBytes(v), js ? 'int<-bytes :number' : 'int<-bytes'] // int digit-parse
      return [f64FromBytes(v), js ? 'f64<-bytes :number' : 'float<-bytes']                       // exact fast f64 parse
    case 'string': {
      const ascii = ASCII_SAFE.has(oid)
      return [`${v} = ${(ascii ? lat : str)('o', 'o + l')}`, (js ? 'string :string' : 'string') + (ascii ? ' (latin1)' : '')]
    }
    case 'latin1': return [`${v} = ${lat('o', 'o + l')}`, 'string :latin1'] // caller asserts ASCII/latin1 (e.g. email)
    case 'bool': return [`${v} = b[o] === 116`, 'bool']
    case 'json': return [`${v} = JSON.parse(${str('o', 'o + l')})`, 'json'] // JSON text may contain unicode -> utf8
    case 'bytea': return [`{ const s = ${lat('o', 'o + l')}; ${v} = s.charCodeAt(0) === 92 && s.charCodeAt(1) === 120 ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }`, 'bytea (latin1)'] // hex text is ASCII
    default: return null // helper closure (the `d` array)
  }
}

// OPT #4 — BINARY wire format (opt-in per column, for ORM flows that know columns upfront and request
// format code 1). PostgreSQL sends the raw fixed-width representation, so decode is a direct read (no
// text parsing) AND the server skips string formatting. Big win for float8 (readDoubleBE = exact),
// int8, timestamp (int64 µs since 2000 -> the integer directly), bytea. numeric/arrays/json/jsonb are
// NOT supported in binary (complex format, or no benefit) — request text. uuid works but text is faster
// (PG ships it pre-formatted), so prefer text there too. `js` still picks date/epoch/number.
const PG_EPOCH_MS = 946684800000 // 2000-01-01 UTC in unix ms
// int64 read, NO BigInt: signed hi from a manual shift, unsigned lo (>>> 0), value = hi*2^32 + lo.
// Exact for |value| < 2^53 (int8 magnitudes below that, timestamp µs until ~year 2255).
const RD64 = 'const hi = (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3], lo = ((b[o + 4] << 24) | (b[o + 5] << 16) | (b[o + 6] << 8) | b[o + 7]) >>> 0'
function binarySnippet(oid: number, v: string, js?: Target): [string, string] | null {
  switch (oid) {
    case 16: return [`${v} = b[o] === 1`, 'bool bin']
    case 21: return [`${v} = b.readInt16BE(o)`, 'int2 bin']
    case 23: return [`${v} = b.readInt32BE(o)`, 'int4 bin']
    case 26: return [`${v} = b.readUInt32BE(o)`, 'oid bin']
    case 20: return js === 'number'
      ? [`{ ${RD64}; ${v} = hi * 4294967296 + lo }`, 'int8 bin :number']
      // exact decimal string with NO BigInt for the common case (|v| < 2^53); BigInt only for the rest
      : [`{ ${RD64}; const n = hi * 4294967296 + lo; ${v} = (n >= -9007199254740991 && n <= 9007199254740991) ? '' + n : b.readBigInt64BE(o).toString() }`, 'int8 bin string']
    case 700: return [`${v} = b.readFloatBE(o)`, 'float4 bin']
    case 701: return [`${v} = b.readDoubleBE(o)`, 'float8 bin (exact)']
    case 1114: case 1184: { // timestamp/timestamptz: int64 µs since 2000-01-01 (no BigInt)
      const ms = `Math.floor((hi * 4294967296 + lo) / 1000) + ${PG_EPOCH_MS}`
      return js === 'date' ? [`{ ${RD64}; ${v} = new Date(${ms}) }`, 'timestamp bin :date'] : [`{ ${RD64}; ${v} = ${ms} }`, 'timestamp bin :epoch']
    }
    case 1082: { // date: int32 days since 2000-01-01
      const ms = `b.readInt32BE(o) * 86400000 + ${PG_EPOCH_MS}`
      return js === 'date' ? [`${v} = new Date(${ms})`, 'date bin :date'] : [`${v} = ${ms}`, 'date bin :epoch']
    }
    // NOTE: binary uuid must BUILD the 36-char string from 16 bytes, whereas TEXT ships it pre-formatted
    // (one slice) — so text uuid is ~3x (JSC) / ~7x (V8) faster. Request text for uuid strings; use binary
    // only if you want the raw 16 bytes. (A hex-LUT build measured slower than toString('hex') here.)
    case 2950: return [`{ const h = b.toString('hex', o, o + 16); ${v} = h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20, 32) }`, 'uuid bin']
    case 17: return [`{ const c = Buffer.allocUnsafe(l); b.copy(c, 0, o, o + l); ${v} = c }`, 'bytea bin']  // raw bytes, no hex decode
    case 18: case 19: case 25: case 1042: case 1043: return [`${v} = ${str('o', 'o + l')}`, 'text bin'] // binary text == utf8 bytes
    // json/jsonb deliberately NOT supported in binary: `json` is stored as text and `jsonb` re-serializes
    // to text on send (binary just adds a version byte) — binary saves nothing on either side. Use text.
    default: return null // no inline binary decoder (numeric, arrays, json/jsonb) — request text format
  }
}

function helperFor(oid: number, map: Map<number, Decoder>): AtDecoder {
  if (oid === 17 && map.get(17) === defaultDecoders.get(17)) return byteaAt
  const dec = decoderFor(oid, map)
  return (b, o, l) => dec(b.subarray(o, o + l))
}

const NO_CUSTOM = new Set<number>()
function customOidsOf(map: Map<number, Decoder>): Set<number> {
  if (map === defaultDecoders) return NO_CUSTOM
  const s = new Set<number>()
  for (const [oid, fn] of map) if (fn !== defaultDecoders.get(oid)) s.add(oid)
  return s
}

function jsonPrep(cols: CodegenCol[]): { header: string; plan: Map<JsonMarker, JsonPlan> } {
  const markers = cols.filter((c) => c.json).map((c) => c.json!)
  if (!markers.length) return { header: '', plan: new Map() }
  const { plan } = genJsonParsers(markers)
  // the cursor (jb/jp/je) is declared block-locally at each column site (below), not once at function
  // scope — block locals are register-allocated better by V8, worth it in the hot per-field loop.
  return { header: '', plan }
}

function columnLines(out: string[], col: CodegenCol, i: number, ind: string, helperOids: number[], plan: Map<JsonMarker, JsonPlan>, custom: Set<number>): void {
  const v = `v${i}`
  if (col.format === 'binary') { // BINARY wire: direct fixed-width read (still length-prefixed; -1 = NULL)
    const bin = binarySnippet(col.oid, v, col.js)
    if (!bin) throw new Error(`decode2: no binary decoder for oid ${col.oid} (request text format for this column)`)
    if (DEBUG) out.push(`${ind}// ${JSON.stringify(col.name)} oid=${col.oid} binary (${bin[1]})`)
    out.push(`${ind}${readLen} let ${v} = null;`)
    out.push(`${ind}if (l !== -1) { ${bin[0]}; o += l }`)
    return
  }
  if (col.json) {
    const pl = plan.get(col.json)!
    if (DEBUG) out.push(`${ind}// ${JSON.stringify(col.name)} (${pl.fast ? 'json fast: JSON.parse' : `json shaped: positional ${col.json.type}`})`)
    out.push(`${ind}${readLen} let ${v} = null;`)
    out.push(pl.fast
      ? `${ind}if (l !== -1) { ${v} = JSON.parse(${str('o', 'o + l')}); o += l }`
      : `${ind}if (l !== -1) { let jb = b, jp = o, je = o + l; ${pl.inline(v)} o += l }`)
    return
  }
  const inl = custom.has(col.oid) ? null : inlineSnippet(col.oid, v, col.js)
  const kind = inl ? inl[1] : 'helper'
  if (DEBUG) out.push(`${ind}// ${JSON.stringify(col.name)} oid=${col.oid}${col.js ? ' :' + col.js : ''} (${kind})`)
  out.push(`${ind}${readLen} let ${v} = null;`)
  if (inl) out.push(`${ind}if (l !== -1) { ${inl[0]}; o += l }`)
  else { const hi = helperOids.length; helperOids.push(col.oid); out.push(`${ind}if (l !== -1) { ${v} = d[${hi}](b, o, l); o += l }`) }
}

const objKey = (name: string, i: number) => (name === '__proto__' ? `["__proto__"]: v${i}` : `${JSON.stringify(name)}: v${i}`)

export type RowBuilder = ((body: Buffer) => unknown) & { source: string }

export function rowBuilderSource(cols: CodegenCol[], mode: 'array' | 'object', custom: Set<number> = NO_CUSTOM): { source: string; helperOids: number[] } {
  const helperOids: number[] = []
  const { header, plan } = jsonPrep(cols)
  const lines = ['  let o = 2, l;']
  for (let i = 0; i < cols.length; i++) columnLines(lines, cols[i]!, i, '  ', helperOids, plan, custom)
  const ret = mode === 'object'
    ? '  return { ' + cols.map((c, i) => objKey(c.name, i)).join(', ') + ' };'
    : '  return [' + cols.map((_, i) => `v${i}`).join(', ') + '];'
  return { source: `function row(b) {\n  "use strict";${header}\n${lines.join('\n')}\n${ret}\n}`, helperOids }
}

export function resultSetSource(cols: CodegenCol[], mode: 'array' | 'object', custom: Set<number> = NO_CUSTOM): { source: string; helperOids: number[] } {
  const helperOids: number[] = []
  const { header, plan } = jsonPrep(cols)
  const decode: string[] = []
  for (let i = 0; i < cols.length; i++) columnLines(decode, cols[i]!, i, '    ', helperOids, plan, custom)
  const assign = mode === 'object'
    ? '    res[i] = { ' + cols.map((c, i) => objKey(c.name, i)).join(', ') + ' };'
    : '    res[i] = [' + cols.map((_, i) => `v${i}`).join(', ') + '];'
  const source = [
    'function rows(arr) {',
    '  "use strict";',
    '  const n = arr.length, res = new Array(n);',
    header,
    '  for (let i = 0; i < n; i++) {',
    '    const b = arr[i]; let o = 2, l;',
    ...decode,
    assign,
    '  }',
    '  return res;',
    '}',
  ].filter((x) => x !== '').join('\n')
  return { source, helperOids }
}

/** Compile a cached, monomorphic single-row builder (v2). */
export function compileRow(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): RowBuilder {
  const { source, helperOids } = rowBuilderSource(cols, mode, customOidsOf(map))
  const helpers = helperOids.map((oid) => helperFor(oid, map))
  if (DEBUG) console.error(`\n[minipg decode2] ${mode} builder for (${cols.map((c) => c.name).join(', ')}):\n${source}\n`)
  const fn = new Function('d', 'P', `return (${source})`)(helpers, POW10) as RowBuilder
  Object.defineProperty(fn, 'source', { value: source, enumerable: false })
  return fn
}

export type ResultSetMapper = ((rows: Buffer[]) => unknown[]) & { source: string }

/** Compile a cached, monomorphic WHOLE-RESULT-SET mapper (v2). */
export function compileResultSet(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): ResultSetMapper {
  const { source, helperOids } = resultSetSource(cols, mode, customOidsOf(map))
  const helpers = helperOids.map((oid) => helperFor(oid, map))
  if (DEBUG) console.error(`\n[minipg decode2] ${mode} result-set mapper for (${cols.map((c) => c.name).join(', ')}):\n${source}\n`)
  const fn = new Function('d', 'P', `return (${source})`)(helpers, POW10) as ResultSetMapper
  Object.defineProperty(fn, 'source', { value: source, enumerable: false })
  return fn
}
