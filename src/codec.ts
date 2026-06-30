// Text-format value decoders for the parsed ('array'/'object') result modes, and
// the param encoder for Bind. Deliberately minimal and precision-safe: int8 and
// numeric default to STRING (the #1 silent-corruption footgun in pg & postgres.js),
// and timestamps stay strings (Date conversion is lossy). Everything is overridable.
import type { Decoder } from './types.ts'

// Internal decoder form: reads the value out of b[off, off+len) in place, so the per-row decode
// never slices a Buffer per cell. off/len are OPTIONAL (default to the whole buffer) so a decoder
// can also be called as dec(buf). The PUBLIC Decoder (config.types) stays (buf)=>value — those
// overrides are wrapped below (a subarray is taken only for overridden columns).
export type CellDecoder = (b: Buffer, off?: number, len?: number) => unknown

// utf8Slice (the Node/Bun primitive behind toString('utf8',…)) skips toString's encoding-name
// lookup + argument coercion, so it's faster on text-heavy rows. Feature-detected so a Buffer
// shim lacking it still works. asString stays direct (the hot text path); the rest reuse it.
type Sliceable = Buffer & { utf8Slice(start: number, end: number): string }
const HAS_UTF8_SLICE = typeof (Buffer.prototype as Partial<Sliceable>).utf8Slice === 'function'
const asString: CellDecoder = HAS_UTF8_SLICE
  ? (b, o = 0, l = b.length) => (b as Sliceable).utf8Slice(o, o + l)
  : (b, o = 0, l = b.length) => b.toString('utf8', o, o + l)
const asNumber: CellDecoder = (b, o = 0, l = b.length) => Number(asString(b, o, l) as string) // float4/float8 — needs full parse (., e)
// Parse a base-10 integer straight from ASCII bytes — no intermediate string (int2/int4/oid).
// Safe for values that fit a JS number; int8/numeric deliberately stay strings (see below).
const asInt: CellDecoder = (b, o = 0, l = b.length) => {
  let p = o, x = 0; const e = o + l
  if (b[o] === 45) { for (p = o + 1; p < e; p++) x = x * 10 + (b[p]! - 48); return -x } // '-'
  for (; p < e; p++) x = x * 10 + (b[p]! - 48)
  return x
}
const asBool: CellDecoder = (b, o = 0) => b[o] === 0x74 // 't'
const asJson: CellDecoder = (b, o = 0, l = b.length) => JSON.parse(asString(b, o, l) as string)
const asBytea: CellDecoder = (b, o = 0, l = b.length) => {
  const s = asString(b, o, l) as string
  return s.startsWith('\\x') ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8')
}

/** OID -> decoder. Anything not listed (int8=20, numeric=1700, timestamps, arrays,
 *  uuid, ...) falls back to a UTF-8 string. */
export const defaultDecoders = new Map<number, CellDecoder>([
  [16, asBool],    // bool
  [17, asBytea],   // bytea
  [21, asInt],     // int2  — digit-parse from bytes
  [23, asInt],     // int4  — digit-parse from bytes
  [26, asInt],     // oid   — digit-parse from bytes
  [700, asNumber], // float4
  [701, asNumber], // float8
  [114, asJson],   // json
  [3802, asJson],  // jsonb
])

export function buildDecoders(overrides?: Record<number, Decoder>): Map<number, CellDecoder> {
  if (!overrides) return defaultDecoders
  const m = new Map(defaultDecoders)
  // wrap a public (buf)=>value override into the offset form (slices only this column's cell)
  for (const [oid, fn] of Object.entries(overrides)) m.set(Number(oid), (b, o = 0, l = b.length) => fn(b.subarray(o, o + l)))
  return m
}

export const decoderFor = (oid: number, map: Map<number, CellDecoder>): CellDecoder => map.get(oid) ?? asString

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
