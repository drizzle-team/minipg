// Text-format value decoders for the parsed ('array'/'object') result modes, and
// the param encoder for Bind. Deliberately minimal and precision-safe: int8 and
// numeric default to STRING (the #1 silent-corruption footgun in pg & postgres.js),
// and timestamps stay strings (Date conversion is lossy). Everything is overridable.
import type { Decoder } from './types.ts'
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

/** Encode a JS param value for a Bind message. */
export function encodeParam(v: unknown): EncodedParam {
  if (v == null) return { format: 0, bytes: null }
  if (Buffer.isBuffer(v)) return { format: 1, bytes: v }
  if (v instanceof Date) return { format: 0, bytes: Buffer.from(v.toISOString(), 'utf8') }
  if (typeof v === 'boolean') return { format: 0, bytes: Buffer.from(v ? 't' : 'f', 'utf8') }
  if (typeof v === 'object') return { format: 0, bytes: Buffer.from(JSON.stringify(v), 'utf8') }
  const s = String(v)
  if (s.indexOf('\0') !== -1) throw new Error('parameter contains NUL byte (0x00), which PostgreSQL text values cannot represent')
  return { format: 0, bytes: Buffer.from(s, 'utf8') }
}
