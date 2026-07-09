// The INTERPRETED decode catalog: the function-form of every strategy decode2.ts emits as inlined code.
// Same optimizations (latin1 for ASCII, exact Clinger float, int-from-bytes, temporal targets, all the
// binary reads) — just as pre-selected per-column CellDecoders driven by a per-row loop instead of a
// monomorphic compiled function. pickDecoder() mirrors decode2's inlineSnippet/binarySnippet dispatch,
// so interpreted and JIT decode identically (validated by the dual-variant suite against real Postgres).
import type { Decoder } from './types.ts'
import { decoderFor, defaultDecoders, arrayDecoderFor } from './codec.ts'
import { ASCII_SAFE, INSTANT_OIDS, INT_OIDS, defaultJs, type CodegenCol, type Target } from './decode2.ts'
import { specNeedsWalk, buildJsonWalk } from './json.ts'
import { extAt } from './geo.ts'

/** Decode a cell in place from (buffer, offset, length) — no per-cell subarray. */
export type CellDecoder = (b: Buffer, o: number, l: number) => unknown

type Sliceable = Buffer & { utf8Slice(s: number, e: number): string; latin1Slice(s: number, e: number): string }
const HAS_UTF8 = typeof (Buffer.prototype as Partial<Sliceable>).utf8Slice === 'function'
const HAS_LAT1 = typeof (Buffer.prototype as Partial<Sliceable>).latin1Slice === 'function'
const utf8 = HAS_UTF8 ? (b: Buffer, o: number, l: number) => (b as Sliceable).utf8Slice(o, o + l) : (b: Buffer, o: number, l: number) => b.toString('utf8', o, o + l)
const lat1 = HAS_LAT1 ? (b: Buffer, o: number, l: number) => (b as Sliceable).latin1Slice(o, o + l) : (b: Buffer, o: number, l: number) => b.toString('latin1', o, o + l)

// ---- TEXT strategies (mirror decode2 inlineSnippet) ----
const txtUtf8: CellDecoder = (b, o, l) => utf8(b, o, l)
const txtLatin1: CellDecoder = (b, o, l) => lat1(b, o, l)
const txtBool: CellDecoder = (b, o) => b[o] === 116
const txtJson: CellDecoder = (b, o, l) => JSON.parse(utf8(b, o, l))
const txtBytea: CellDecoder = (b, o, l) => { const s = lat1(b, o, l); return s.charCodeAt(0) === 92 && s.charCodeAt(1) === 120 ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }
// digit-parse straight from ASCII bytes -> JS number (int2/int4/oid, and int8/bigint:number)
const txtInt: CellDecoder = (b, o, l) => { let p = o, s = false, x = 0; const e = o + l; if (b[o] === 45) { s = true; p++ } for (; p < e; p++) x = x * 10 + (b[p]! - 48); return s ? -x : x }
const txtBigInt: CellDecoder = (b, o, l) => BigInt(lat1(b, o, l)) // int8/bigint -> exact JS BigInt

// exact-fast Clinger ASCII -> f64 (bit-identical to Number for the fast domain; falls back to Number otherwise)
const POW10 = [1, 10, 100, 1000, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22]
const txtF64: CellDecoder = (b, o, l) => {
  let p = o; const e = o + l; const c0 = b[p]!; let neg = false
  if (c0 === 45) { neg = true; p++ } else if (c0 === 43) p++
  if (b[p] === 78 || b[p] === 110) return NaN
  if (b[p] === 73 || b[p] === 105) return neg ? -Infinity : Infinity
  let sig = 0, nd = 0, fd = 0, dot = false, hard = false
  for (; p < e; p++) { const c = b[p]!; if (c === 46) { dot = true; continue } if (c < 48 || c > 57) break; if (sig === 0 && c === 48) { if (dot) fd++; continue } if (nd < 15) { sig = sig * 10 + (c - 48); nd++; if (dot) fd++ } else hard = true }
  let exp = 0, es = 1
  if (b[p] === 101 || b[p] === 69) { p++; if (b[p] === 45) { es = -1; p++ } else if (b[p] === 43) p++; for (; p < e; p++) { const c = b[p]!; if (c < 48 || c > 57) break; exp = exp * 10 + (c - 48) } }
  const eff = es * exp - fd
  if (hard || eff > 22 || eff < -22) return Number(lat1(b, o, l))
  const r = eff >= 0 ? sig * POW10[eff]! : sig / POW10[-eff]!
  return neg ? -r : r
}
const txtF4Precise: CellDecoder = (b, o, l) => Math.fround(txtF64(b, o, l) as number) // float4:precise -> exact stored f32

// temporal :date/:ms (direct field parse -> Date.UTC; naive = UTC, tz applies offset; micros -> ms)
function tsParse(b: Buffer, o: number, l: number): number {
  let p = o; const e = o + l
  let Y = 0; for (; p < e; p++) { const c = b[p]!; if (c < 48 || c > 57) break; Y = Y * 10 + (c - 48) } p++
  const Mo = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 3
  const D = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 2
  let H = 0, Mi = 0, S = 0, ms = 0, off = 0
  if (p < e && b[p] === 32) {
    p++
    H = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 3
    Mi = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 3
    S = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 2
    if (p < e && b[p] === 46) { p++; let f = 0, k = 0; for (; p < e && k < 3; p++) { const c = b[p]!; if (c < 48 || c > 57) break; f = f * 10 + (c - 48); k++ } while (k < 3) { f *= 10; k++ } ms = f; while (p < e) { const c = b[p]!; if (c < 48 || c > 57) break; p++ } }
    if (p < e && (b[p] === 43 || b[p] === 45)) { const sg = b[p] === 45 ? -1 : 1; p++; const th = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 2; let tm = 0; if (p < e && b[p] === 58) { p++; tm = (b[p]! - 48) * 10 + (b[p + 1]! - 48); p += 2 } off = sg * (th * 60 + tm) * 60000 }
  }
  let ems = Date.UTC(Y, Mo - 1, D, H, Mi, S, ms)
  if (Y <= 99) { const d = new Date(ems); d.setUTCFullYear(Y); ems = d.getTime() } // Date.UTC remaps years 0-99 to 1900+Y; undo it BEFORE applying the tz offset
  return ems - off
}
const tsDate: CellDecoder = (b, o, l) => new Date(tsParse(b, o, l))
const tsEpoch: CellDecoder = (b, o, l) => tsParse(b, o, l)

// ---- BINARY strategies (mirror decode2 binarySnippet) ----
const PG_EPOCH_MS = 946684800000
const rd64 = (b: Buffer, o: number) => { const hi = (b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!, lo = ((b[o + 4]! << 24) | (b[o + 5]! << 16) | (b[o + 6]! << 8) | b[o + 7]!) >>> 0; return hi * 4294967296 + lo }
const binBool: CellDecoder = (b, o) => b[o] === 1
const binInt2: CellDecoder = (b, o) => b.readInt16BE(o)
const binInt4: CellDecoder = (b, o) => b.readInt32BE(o)
const binOid: CellDecoder = (b, o) => b.readUInt32BE(o)
const binFloat4: CellDecoder = (b, o) => b.readFloatBE(o)
const binFloat8: CellDecoder = (b, o) => b.readDoubleBE(o)
const binInt8Num: CellDecoder = (b, o) => rd64(b, o)
const binInt8Str: CellDecoder = (b, o) => { const n = rd64(b, o); return n >= -9007199254740991 && n <= 9007199254740991 ? '' + n : b.readBigInt64BE(o).toString() }
const binInt8BigInt: CellDecoder = (b, o) => b.readBigInt64BE(o)
const binTsEpoch: CellDecoder = (b, o) => Math.floor(rd64(b, o) / 1000) + PG_EPOCH_MS
const binTsDate: CellDecoder = (b, o) => new Date(Math.floor(rd64(b, o) / 1000) + PG_EPOCH_MS)
const binDateEpoch: CellDecoder = (b, o) => b.readInt32BE(o) * 86400000 + PG_EPOCH_MS
const binDateDate: CellDecoder = (b, o) => new Date(b.readInt32BE(o) * 86400000 + PG_EPOCH_MS)
const binBytea: CellDecoder = (b, o, l) => { const c = Buffer.allocUnsafe(l); b.copy(c, 0, o, o + l); return c }
const binUuid: CellDecoder = (b, o) => { const h = b.toString('hex', o, o + 16); return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20, 32) }
const binText: CellDecoder = (b, o, l) => utf8(b, o, l)

const wrap = (d: Decoder): CellDecoder => (b, o, l) => d(b.subarray(o, o + l)) // public (buf)=>value override -> offset form

function pickBinary(oid: number, js?: Target): CellDecoder {
  switch (oid) {
    case 16: return binBool
    case 21: return binInt2
    case 23: return binInt4
    case 26: return binOid
    case 20: return js === 'number' ? binInt8Num : js === 'string' ? binInt8Str : binInt8BigInt // default -> BigInt
    case 700: return binFloat4
    case 701: return binFloat8
    case 1114: case 1184: return js === 'ms' ? binTsEpoch : binTsDate // default -> Date
    case 1082: return js === 'ms' ? binDateEpoch : binDateDate
    case 2950: return binUuid
    case 17: return binBytea
    case 18: case 19: case 25: case 1042: case 1043: return binText
    default: throw new Error(`minipg: no binary decoder for oid ${oid} (request text format for this column)`)
  }
}

// text strategy for an oid+target; null = "use the helper/map decoder" (uncommon/unknown types)
function pickText(oid: number, js?: Target): CellDecoder | null {
  if (!js && INSTANT_OIDS.has(oid)) js = 'date' // date/timestamp/timestamptz default to a JS Date (:string/:ms override)
  if (js === 'date' || js === 'ms') { if (INSTANT_OIDS.has(oid)) return js === 'date' ? tsDate : tsEpoch; js = 'string' }
  if (js === 'bigint' || (!js && oid === 20)) return txtBigInt // int8/bigint default to a JS BigInt (:number/:string override)
  if (oid === 700 && js !== 'string') { if (js === 'precise') return txtF4Precise; js = undefined } // float4: :precise -> fround; bare/:pretty -> Number (canonical)
  const eff = js ?? defaultJs(oid)
  switch (eff) {
    case 'number': return INT_OIDS.has(oid) ? txtInt : txtF64
    case 'string': return ASCII_SAFE.has(oid) ? txtLatin1 : txtUtf8
    case 'latin1': return txtLatin1 // caller asserts ASCII/latin1 (e.g. email) -> skip the utf8 scan
    case 'bool': return txtBool
    case 'json': return txtJson
    case 'bytea': return txtBytea
    default: return null // helper closure
  }
}

/** Resolve the CellDecoder for a column, mirroring decode2's dispatch exactly. */
export function pickDecoder(col: CodegenCol, map: Map<number, Decoder>): CellDecoder {
  if (col.format === 'binary') return pickBinary(col.oid, col.js)
  if (col.json) {
    // shaped json in the no-eval path: JSON.parse (respects jsonBigints via the map's json decoder), then
    // a temporal post-parse walk to match the jit scanner's :ms/:date output. Exact bigint/numeric
    // precision beyond JSON.parse stays a jit-only / jsonBigints concern (documented).
    const base = wrap(decoderFor(col.oid, map))
    if (!specNeedsWalk(col.json.spec)) return base
    const walk = buildJsonWalk(col.json)
    return (b, o, l) => walk(base(b, o, l))
  }
  // a custom (config.types) override always wins — decode2 routes these to its helper too
  if (map !== defaultDecoders) { const d = map.get(col.oid); if (d && d !== defaultDecoders.get(col.oid)) return wrap(d) }
  const ext = extAt(col); if (ext) return ext // point / pgvector / PostGIS shape columns (custom override handled above)
  if (col.array) { const dec = arrayDecoderFor(col.oid, col.array.js); return (b, o, l) => dec(b.subarray(o, o + l)) } // '{…}' text -> JS array (same as the JIT helper)
  return pickText(col.oid, col.js) ?? wrap(decoderFor(col.oid, map))
}
