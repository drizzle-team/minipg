// Text-format value decoders for the parsed ('array'/'object') result modes, and
// the param encoder for Bind. Deliberately minimal and precision-safe: int8 and
// numeric default to STRING (the #1 silent-corruption footgun in pg & postgres.js),
// and timestamps stay strings (Date conversion is lossy). Everything is overridable.
import type { Decoder } from './types.ts'
import { Writer, type ParamsEncoder } from './protocol.ts' // protocol.ts only type-imports from here -> no runtime cycle
import { parseJsonBuffer } from './jsonparse.ts'

const asString: Decoder = (b) => b.toString('utf8')
const asNumber: Decoder = (b) => Number(b.toString('utf8'))
const asBool: Decoder = (b) => b[0] === 0x74 // 't'
const asJson: Decoder = (b) => JSON.parse(b.toString('utf8'))
const asBytea: Decoder = (b) => {
  const s = b.toString('utf8')
  return s.startsWith('\\x') ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8')
}

/** OID -> decoder. Anything not listed (int8=20, numeric=1700, timestamps, arrays,
 *  uuid, ...) falls back to a UTF-8 string. */
export const defaultDecoders = new Map<number, Decoder>([
  [16, asBool],    // bool
  [17, asBytea],   // bytea
  [21, asNumber],  // int2
  [23, asNumber],  // int4
  [26, asNumber],  // oid
  [700, asNumber], // float4
  [701, asNumber], // float8
  [114, asJson],   // json
  [3802, asJson],  // jsonb
])

export type JsonBigints = 'number' | 'string' | 'bigint'

// Native JSON source-text reviver (ctx.source): recent V8 (Node 21+) and Bun. Preserves
// oversized integers by value (order-independent, so it works for jsonb too).
export const jsonSourceSupported: boolean = (() => {
  try { let ok = false; JSON.parse('{"x":1}', function (_k: string, v: unknown, ctx?: { source?: string }) { if (ctx && typeof ctx.source === 'string') ok = true; return v } as never); return ok } catch { return false }
})()
function bigintReviver(as: 'string' | 'bigint') {
  return (_k: string, v: unknown, ctx?: { source?: string }) =>
    typeof v === 'number' && Number.isInteger(v) && !Number.isSafeInteger(v) && ctx && typeof ctx.source === 'string' && /^-?\d+$/.test(ctx.source)
      ? (as === 'bigint' ? BigInt(ctx.source) : ctx.source) : v
}

/** The json/jsonb decoder for a given jsonBigints mode. 'number' = plain native JSON.parse.
 *  'string'/'bigint' are HYBRID: a cheap byte pre-scan (mayHaveBigInt) detects whether a value
 *  could contain an integer > 2^53. The common case (none) takes the native JSON.parse fast
 *  path; only flagged values pay the slow exact path (source-text reviver, or the pure-JS
 *  buffer parser when ctx.source is unavailable). Order-independent (covers jsonb). Only whole
 *  integers are preserved; high-precision *decimals* still need a declared Json({x:'numeric'}). */
export function jsonDecoder(mode: JsonBigints): Decoder {
  if (mode === 'number') return asJson
  // Detect a possible bigint (>=16 consecutive digits) with a native regex on the string we
  // build for JSON.parse anyway — cheaper than a JS byte loop, and the slow path reuses the
  // string. Never a false negative (correctness holds); false positives only take the slow path.
  const BIG = /[0-9]{16}/ // no /g — stateless .test, safe to reuse
  if (jsonSourceSupported) {
    const rev = bigintReviver(mode)
    return (b) => { const s = b.toString('utf8'); return BIG.test(s) ? JSON.parse(s, rev as never) : JSON.parse(s) }
  }
  return (b) => { const s = b.toString('utf8'); return BIG.test(s) ? parseJsonBuffer(b, 0, b.length, mode) : JSON.parse(s) }
}

export function buildDecoders(overrides?: Record<number, Decoder>, jsonBigints: JsonBigints = 'number'): Map<number, Decoder> {
  if (!overrides && jsonBigints === 'number') return defaultDecoders
  const m = new Map(defaultDecoders)
  if (jsonBigints !== 'number') { const d = jsonDecoder(jsonBigints); m.set(114, d); m.set(3802, d) }
  if (overrides) for (const [oid, fn] of Object.entries(overrides)) m.set(Number(oid), fn)
  return m
}

export const decoderFor = (oid: number, map: Map<number, Decoder>): Decoder => map.get(oid) ?? asString

export interface EncodedParam {
  format: number // 0 = text, 1 = binary
  bytes: Buffer | null // null = SQL NULL
}

/** Encode a JS param value for a Bind message. Kept for compatibility (tests, W.bind); the hot
 *  path uses the zero-alloc write-through `encodeValueInto` below. */
export function encodeParam(v: unknown): EncodedParam {
  if (v == null) return { format: 0, bytes: null }
  if (Buffer.isBuffer(v)) return { format: 1, bytes: v }
  if (v instanceof Date) return { format: 0, bytes: Buffer.from(v.toISOString(), 'utf8') }
  if (typeof v === 'boolean') return { format: 0, bytes: Buffer.from(v ? 't' : 'f', 'utf8') }
  if (typeof v === 'object') return { format: 0, bytes: Buffer.from(JSON.stringify(v), 'utf8') }
  const s = String(v)
  if (s.indexOf('\0') !== -1) throw new Error(NUL_MSG)
  return { format: 0, bytes: Buffer.from(s, 'utf8') }
}

const NUL_MSG = 'parameter contains NUL byte (0x00), which PostgreSQL text values cannot represent'

/** Write-through param encoding: ONE value straight into the Bind message being built in `w`
 *  (int32 length + payload), TEXT format, byte-identical to encodeParam — but with zero
 *  intermediate allocations (no String() for safe integers, no Buffer.from, no wrapper object).
 *  Returns the format code used: 0, or 1 for the Buffer (bytea) passthrough. */
export function encodeValueInto(w: Writer, v: unknown): number {
  if (v == null) { w.int32(-1); return 0 }
  if (Buffer.isBuffer(v)) { w.lpBytes(v); return 1 }
  switch (typeof v) {
    case 'number':
      if (Number.isSafeInteger(v)) w.lpAsciiInt(v)
      else w.lpStr(String(v)) // fraction/exponent/NaN/Infinity forms can't contain NUL
      return 0
    case 'string':
      if (v.indexOf('\0') !== -1) throw new Error(NUL_MSG)
      w.lpStr(v)
      return 0
    case 'boolean':
      w.int32(1); w.byte(v ? 0x74 : 0x66) // 't' / 'f'
      return 0
    case 'object':
      if (v instanceof Date) w.lpStr(v.toISOString())
      else w.lpStr(JSON.stringify(v)) // stringify escapes control chars — no raw NUL possible
      return 0
    default: { // bigint (digits), symbol/function (String() throws / nonsense — same as encodeParam)
      const s = String(v)
      if (s.indexOf('\0') !== -1) throw new Error(NUL_MSG)
      w.lpStr(s)
      return 0
    }
  }
}

// ---- binary param plans (prepared-statement reuse) ----------------------------------------
// Once a named statement's ParameterDescription OIDs are cached (first round trip), params of
// the bench-fast types can go out BINARY: no text formatting, fewer bytes, no server-side text
// parse. Every encoder GUARDS on the JS value's type/range and falls back to text for that one
// value when it doesn't fit — so semantics (incl. server range errors) are unchanged. This is
// the encode-side mirror of reuseBinaryOids: first execution text, reuse upgrades.

const PG_EPOCH_MS = 946684800000 // 2000-01-01T00:00:00Z
const MS_SAFE = 9007199254740    // |ms since PG epoch| below this, ms*1000 stays a safe integer
const I64_MIN = -9223372036854775808n
const I64_MAX = 9223372036854775807n

type BinEnc = (w: Writer, v: unknown) => number

function binEncoderFor(oid: number): BinEnc | null {
  switch (oid) {
    case 16: return (w, v) => { if (typeof v !== 'boolean') return encodeValueInto(w, v); w.int32(1); w.byte(v ? 1 : 0); return 1 }
    case 21: return (w, v) => { if (typeof v !== 'number' || !Number.isInteger(v) || v < -32768 || v > 32767) return encodeValueInto(w, v); w.int32(2); w.int16(v & 0xffff); return 1 }
    case 23: return (w, v) => { if (typeof v !== 'number' || !Number.isInteger(v) || v < -2147483648 || v > 2147483647) return encodeValueInto(w, v); w.int32(4); w.int32(v); return 1 }
    case 20: return (w, v) => {
      if (typeof v === 'number' && Number.isSafeInteger(v)) { w.lpI64(v); return 1 }
      if (typeof v === 'bigint' && v >= I64_MIN && v <= I64_MAX) { w.lpI64Big(v); return 1 }
      return encodeValueInto(w, v) // out-of-range/strings fall back: the server raises the proper 22003
    }
    case 701: return (w, v) => { if (typeof v !== 'number') return encodeValueInto(w, v); w.lpF8(v); return 1 }
    // timestamp + timestamptz: micros since 2000-01-01 UTC. Matches the text path's toISOString
    // semantics for both (timestamp ignores the zone suffix; getTime IS the UTC reading).
    case 1114: case 1184: return (w, v) => {
      if (!(v instanceof Date)) return encodeValueInto(w, v)
      const ms = v.getTime() - PG_EPOCH_MS
      if (Number.isNaN(ms)) return encodeValueInto(w, v) // Invalid Date -> toISOString throws, same as text
      if (ms >= -MS_SAFE && ms <= MS_SAFE) w.lpI64(ms * 1000)
      else w.lpI64Big(BigInt(ms) * 1000n) // >±285yr from 2000: exact via BigInt
      return 1
    }
    // ---- array types (the unnest batch-insert path): JS array -> binary array wire format ----
    case 1000: return arrayEnc(16)   // bool[]
    case 1005: return arrayEnc(21)   // int2[]
    case 1007: return arrayEnc(23)   // int4[]
    case 1016: return arrayEnc(20)   // int8[]
    case 1022: return arrayEnc(701)  // float8[]
    case 1009: return arrayEnc(25)   // text[]
    case 1015: return arrayEnc(1043) // varchar[]
    case 1115: return arrayEnc(1114) // timestamp[]
    case 1185: return arrayEnc(1184) // timestamptz[]
    // NOT upgraded on purpose: float4 (binary would silently clamp out-of-range to ±Infinity where
    // text raises 22003), numeric/uuid (later), text/varchar scalars (binary == text bytes, no win).
    default: return null
  }
}

// ---- binary arrays ------------------------------------------------------------------------
// Wire format: int32 ndim (0 for empty), int32 hasnull, int32 elemOid, then per dim int32 length +
// int32 lower-bound(1), then per element int32 len + payload (-1 = NULL). Element payloads are the
// SAME encodings as scalar binary params. An element whose JS type doesn't fit bails the WHOLE value
// out to a text array literal (rewind + rewrite) — the server then parses/validates as usual.

class ElemMismatch extends TypeError { constructor() { super('array element does not fit the declared element type') } }
const MISMATCH = new ElemMismatch() // singleton: thrown/caught on the fallback path only, no stack use

type ElemEnc = (w: Writer, v: unknown) => void // writes int32 len + payload; throws MISMATCH if v doesn't fit
function elemEncoderFor(elemOid: number): ElemEnc {
  switch (elemOid) {
    case 16: return (w, v) => { if (typeof v !== 'boolean') throw MISMATCH; w.int32(1); w.byte(v ? 1 : 0) }
    case 21: return (w, v) => { if (typeof v !== 'number' || !Number.isInteger(v) || v < -32768 || v > 32767) throw MISMATCH; w.int32(2); w.int16(v & 0xffff) }
    case 23: return (w, v) => { if (typeof v !== 'number' || !Number.isInteger(v) || v < -2147483648 || v > 2147483647) throw MISMATCH; w.int32(4); w.int32(v) }
    case 20: return (w, v) => {
      if (typeof v === 'number' && Number.isSafeInteger(v)) return w.lpI64(v)
      if (typeof v === 'bigint' && v >= I64_MIN && v <= I64_MAX) return w.lpI64Big(v)
      throw MISMATCH
    }
    case 701: return (w, v) => { if (typeof v !== 'number') throw MISMATCH; w.lpF8(v) }
    case 25: case 1043: return (w, v) => { if (typeof v !== 'string' || v.indexOf('\0') !== -1) throw MISMATCH; w.lpStr(v) } // NUL -> literal path -> proper error
    case 1114: case 1184: return (w, v) => {
      if (!(v instanceof Date)) throw MISMATCH
      const ms = v.getTime() - PG_EPOCH_MS
      if (Number.isNaN(ms)) throw MISMATCH
      if (ms >= -MS_SAFE && ms <= MS_SAFE) w.lpI64(ms * 1000)
      else w.lpI64Big(BigInt(ms) * 1000n)
    }
    default: return () => { throw MISMATCH }
  }
}

function arrayEnc(elemOid: number): BinEnc {
  const elem = elemEncoderFor(elemOid)
  return (w, v) => {
    if (!Array.isArray(v)) return encodeValueInto(w, v) // e.g. a caller-built '{…}' literal string passes through as text
    const mark = w.mark()
    try {
      const lenPos = w.mark()
      w.int32(0) // total length, back-patched
      if (v.length === 0) { w.int32(0); w.int32(0); w.int32(elemOid) } // ndim 0 = empty array
      else {
        let hasNull = 0
        for (let i = 0; i < v.length; i++) if (v[i] == null) { hasNull = 1; break }
        w.int32(1); w.int32(hasNull); w.int32(elemOid)
        w.int32(v.length); w.int32(1) // one dimension, lower bound 1
        for (let i = 0; i < v.length; i++) { const e = v[i]; if (e == null) w.int32(-1); else elem(w, e) }
      }
      w.patch32(lenPos, w.mark() - lenPos - 4)
      return 1
    } catch (e) {
      if (e !== MISMATCH) throw e
      w.rewind(mark)
      return encodeValueInto(w, arrayLiteral(v)) // text literal: the server parses + raises the proper error if truly invalid
    }
  }
}

// ---- COPY FROM STDIN row encoding ----------------------------------------------------------
// A COPY BINARY field is the SAME wire bytes as a binary Bind param (int32 len + payload), so
// the element encoders above serve both. Rows stream out as ~chunkBytes Buffers.

/** True when every column of this OID list can be COPY-BINARY encoded (else use text format). */
export const copyBinarySupported = (oids: readonly number[]): boolean =>
  oids.every((o) => [16, 21, 23, 20, 701, 25, 1043, 1114, 1184].includes(o))

const COPY_SIG = Buffer.from([0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00]) // "PGCOPY\n\xff\r\n\0"
type CopyRow = readonly unknown[] | Readonly<Record<string, unknown>>
const cell = (row: CopyRow, names: readonly string[], c: number): unknown =>
  Array.isArray(row) ? row[c] : (row as Record<string, unknown>)[names[c]!]

/** Rows -> COPY BINARY payload chunks (header + tuples + trailer). Strict per-column typing:
 *  a value that doesn't fit its declared type rejects with the row/column named. */
export function* copyRowsBinary(oids: readonly number[], names: readonly string[], rows: readonly CopyRow[], chunkBytes = 1 << 18): Generator<Buffer> {
  const encs = oids.map(elemEncoderFor)
  const w = new Writer(chunkBytes + 4096)
  w.bytes(COPY_SIG); w.int32(0); w.int32(0) // flags, header-extension length
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r]!
    w.int16(oids.length)
    for (let c = 0; c < oids.length; c++) {
      const v = cell(row, names, c)
      if (v == null) w.int32(-1)
      else {
        try { encs[c]!(w, v) } catch (e) {
          if (e !== MISMATCH) throw e
          throw new TypeError(`copy: row ${r}, column "${names[c]}" — value is incompatible with its declared type`)
        }
      }
    }
    if (w.mark() >= chunkBytes) { yield Buffer.from(w.slice()); w.reset() }
  }
  w.int16(0xffff) // trailer: int16 -1
  yield Buffer.from(w.slice())
}

const escCopyText = (s: string) => s.replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r')
function copyTextCell(v: unknown): string {
  if (typeof v === 'string') return escCopyText(v)
  if (typeof v === 'number' || typeof v === 'bigint') return String(v)
  if (typeof v === 'boolean') return v ? 't' : 'f'
  if (v instanceof Date) return v.toISOString()
  if (Buffer.isBuffer(v)) return '\\\\x' + v.toString('hex') // literal backslash-x…: escaped \ + hex
  return escCopyText(typeof v === 'object' ? JSON.stringify(v) : String(v))
}

/** Rows -> COPY text-format payload chunks (tab-separated, \N for NULL). Universal: works for
 *  every column type — the server parses each field with the type's input function. */
export function* copyRowsText(names: readonly string[], rows: readonly CopyRow[], chunkBytes = 1 << 18): Generator<Buffer> {
  let parts: string[] = []
  let size = 0
  for (const row of rows) {
    let line = ''
    for (let c = 0; c < names.length; c++) {
      if (c) line += '\t'
      const v = cell(row, names, c)
      line += v == null ? '\\N' : copyTextCell(v)
    }
    line += '\n'
    parts.push(line)
    size += line.length
    if (size >= chunkBytes) { yield Buffer.from(parts.join(''), 'utf8'); parts = []; size = 0 }
  }
  if (parts.length) yield Buffer.from(parts.join(''), 'utf8')
}

/** JS array -> PG text array literal ('{…}'), the fallback when an element can't go binary.
 *  Handles nesting (multidimensional), NULLs, and quoted-element escaping. */
export function arrayLiteral(arr: readonly unknown[]): string {
  let s = '{'
  for (let i = 0; i < arr.length; i++) {
    if (i) s += ','
    const v = arr[i]
    if (v == null) { s += 'NULL'; continue }
    if (Array.isArray(v)) { s += arrayLiteral(v); continue }
    if (typeof v === 'number' || typeof v === 'bigint') { s += String(v); continue }
    if (typeof v === 'boolean') { s += v ? 't' : 'f'; continue }
    const str = v instanceof Date ? v.toISOString() : Buffer.isBuffer(v) ? '\\x' + v.toString('hex') : String(v)
    s += '"' + str.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"'
  }
  return s + '}'
}

/** Compile the per-statement param plan from cached ParameterDescription OIDs. Returns null when
 *  no param has a binary encoder (pure-text statement — callers keep the plain path). */
export function compileParamPlan(oids: readonly number[]): ParamsEncoder | null {
  let any = false
  const encs = oids.map((o) => { const e = binEncoderFor(o); if (e) any = true; return e })
  if (!any) return null
  return (w, v, i) => { const e = i < encs.length ? encs[i] : null; return e ? e(w, v) : encodeValueInto(w, v) }
}
