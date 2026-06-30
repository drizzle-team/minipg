// Monomorphic row decoding via codegen: compile one row builder per result shape
// (cached) with `new Function`, returning objects from a literal (single hidden class)
// and decoding each field directly from (buffer, offset, length) — no per-cell subarray.
// NOTE: uses `new Function` → not usable under strict CSP / some edge runtimes; a driver
// would feature-detect and fall back to the interpreted path. This variant always uses it.
import type { Field, Decoder } from './types.ts'
import { decoderFor } from './codec.ts'

type AtDecoder = (b: Buffer, o: number, l: number) => unknown

// offset-based built-in decoders (no subarray allocation)
const intAt: AtDecoder = (b, o, l) => { let i = o, neg = false; const e = o + l; if (b[i] === 0x2d) { neg = true; i++ } let n = 0; for (; i < e; i++) n = n * 10 + (b[i]! - 48); return neg ? -n : n }
const numAt: AtDecoder = (b, o, l) => Number(b.toString('utf8', o, o + l))
const textAt: AtDecoder = (b, o, l) => b.toString('utf8', o, o + l)
const boolAt: AtDecoder = (b, o) => b[o] === 0x74
const jsonAt: AtDecoder = (b, o, l) => JSON.parse(b.toString('utf8', o, o + l))
const byteaAt: AtDecoder = (b, o, l) => { const s = b.toString('utf8', o, o + l); return s.startsWith('\\x') ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }

// OID -> offset decoder. Mirrors codec.ts defaults: int2/4/oid -> number (int parse),
// float -> number, bool, json/jsonb -> parsed, bytea -> Buffer; int8/numeric/text/
// timestamps -> string. Anything unlisted falls back to slicing + the merged decoder map.
const builtinAt: Record<number, AtDecoder> = {
  16: boolAt, 17: byteaAt, 18: textAt, 19: textAt, 20: textAt, 21: intAt, 23: intAt, 25: textAt, 26: intAt,
  114: jsonAt, 700: numAt, 701: numAt, 1043: textAt, 1082: textAt, 1083: textAt, 1114: textAt, 1184: textAt, 1700: textAt, 3802: jsonAt,
}

function atFor(oid: number, map: Map<number, Decoder>): AtDecoder {
  const builtin = builtinAt[oid]
  if (builtin) return builtin
  const dec = decoderFor(oid, map) // user override or string default
  return (b, o, l) => dec(b.subarray(o, o + l))
}

export type RowBuilder = (body: Buffer) => unknown

/** Compile a row builder for the given fields + mode ('array' | 'object'). */
export function compileRow(fields: Field[], mode: 'array' | 'object', map: Map<number, Decoder>): RowBuilder {
  const decs = fields.map((f) => atFor(f.dataTypeOid, map))
  const src: string[] = ['let o=2,l;']
  for (let i = 0; i < fields.length; i++) {
    // read int32 length; -1 = NULL (no bytes follow); else decode from (b,o,l) then advance
    src.push(`l=b.readInt32BE(o);o+=4;const v${i}=l===-1?null:d[${i}](b,o,l);o+=l===-1?0:l;`)
  }
  const ret = mode === 'object'
    ? 'return {' + fields.map((f, i) => `${JSON.stringify(f.name)}:v${i}`).join(',') + '};'
    : 'return [' + fields.map((_, i) => `v${i}`).join(',') + '];'
  // factory captures `d` (the decoder array) so each call is just builder(body)
  const factory = new Function('d', `return function(b){${src.join('')}${ret}}`) as (d: AtDecoder[]) => RowBuilder
  return factory(decs)
}
