// Monomorphic row decoding via codegen, with per-column decode INLINED into the
// generated function (no per-cell helper call) so V8 fuses the whole row builder into
// one monomorphic function. Int parsing / toString / bool / JSON.parse / bytea are emitted
// directly; only arrays / user overrides / unknown OIDs fall back to a helper closure (`d`).
// Shaped JSON columns (Json()/JsonArray()) emit a positional sub-parser instead. Cached per
// result shape.
//
// DEBUG: set MINIPG_CODEGEN_DEBUG=1 to print each generated builder; every builder also
// carries its readable `.source` (the exact text compiled into the JIT mapper).
// NOTE: `new Function` — not usable under strict CSP / some edge runtimes (feature-detect + fall back when merged into src/).
import type { Decoder } from './types.ts'
import { decoderFor, defaultDecoders } from './codec.ts'
import { genJsonParsers, JSON_RUNTIME, type JsonMarker, type JsonPlan, type JsTarget } from './json.ts'

/** A column to decode: name + wire OID, plus optional JS-target override and shaped-JSON marker. */
export interface CodegenCol { name: string; oid: number; js?: JsTarget; json?: JsonMarker }

type AtDecoder = (b: Buffer, o: number, l: number) => unknown
const byteaAt: AtDecoder = (b, o, l) => { const s = b.toString('utf8', o, o + l); return s.startsWith('\\x') ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }

const DEBUG = !!process.env.MINIPG_CODEGEN_DEBUG

// b.utf8Slice(o, e) is the primitive behind b.toString('utf8', o, e): it skips toString's
// encoding-name lookup + argument coercion, so it's faster (~1.19x JSC / 1.08x V8 in the row
// decoder). Present on Node and Bun; feature-detected so a Buffer shim lacking it still works.
const HAS_UTF8_SLICE = typeof (Buffer.prototype as { utf8Slice?: unknown }).utf8Slice === 'function'
const str = (from: string, to: string) => (HAS_UTF8_SLICE ? `b.utf8Slice(${from}, ${to})` : `b.toString('utf8', ${from}, ${to})`)

// Read the int32 length prefix as a signed big-endian int straight from the four bytes instead
// of b.readInt32BE(o) (a method call + a bounds check). PG field lengths are 0..2^31-1 or -1
// (NULL), so the signed shift is exact: 0xFFFFFFFF -> -1. ~1.15x V8 / 1.08x JSC. Emitted per column.
const readLen = 'l = (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3]; o += 4;'

// digit-parse straight from ASCII bytes -> JS number (no toString), for integer OIDs.
const intFromBytes = (v: string) => `{ let p = o, s = false, x = 0; const e = o + l; if (b[p] === 45) { s = true; p++ } for (; p < e; p++) x = x * 10 + (b[p] - 48); ${v} = s ? -x : x }`

// The natural JS target for a wire OID (before any per-column override).
function defaultJs(oid: number): 'number' | 'string' | 'bool' | 'json' | 'bytea' | 'helper' {
  switch (oid) {
    case 16: return 'bool'
    case 17: return 'bytea'
    case 114: case 3802: return 'json'
    case 21: case 23: case 26: case 700: case 701: return 'number'   // int2/int4/oid + float4/float8 fit a JS number
    case 20: case 1700: case 790: return 'string'                    // int8/numeric/money -> exact string (avoid f64 loss)
    case 18: case 19: case 25: case 1042: case 1043: case 1082: case 1083: case 1114: case 1184: case 1186: case 2950: return 'string'
    default: return 'helper' // arrays / unknown / user-supplied decoders
  }
}
const INT_OIDS = new Set([20, 21, 23, 26]) // int8/int2/int4/oid — digit-parseable straight to a number

// Inline decode expression for a column: assign the value into `v` from b/o/l, WITHOUT
// advancing o. `js` is the per-column override ('number'/'string'). Returns null → helper.
// Second tuple element is a human label for the debug comment.
function inlineSnippet(oid: number, v: string, js?: JsTarget): [string, string] | null {
  const eff = js ?? defaultJs(oid)
  switch (eff) {
    case 'number':
      if (INT_OIDS.has(oid)) return [intFromBytes(v), js ? 'int<-bytes :number' : 'int<-bytes']
      return [`${v} = Number(${str('o', 'o + l')})`, js ? 'number :number' : 'float']
    case 'string': return [`${v} = ${str('o', 'o + l')}`, js ? 'string :string' : 'string']
    case 'bool': return [`${v} = b[o] === 116`, 'bool']
    case 'json': return [`${v} = JSON.parse(${str('o', 'o + l')})`, 'json']
    case 'bytea': return [`{ const s = ${str('o', 'o + l')}; ${v} = s.charCodeAt(0) === 92 && s.charCodeAt(1) === 120 ? Buffer.from(s.slice(2), 'hex') : Buffer.from(s, 'utf8') }`, 'bytea']
    default: return null // helper closure (the `d` array)
  }
}

function helperFor(oid: number, map: Map<number, Decoder>): AtDecoder {
  if (oid === 17 && map.get(17) === defaultDecoders.get(17)) return byteaAt
  const dec = decoderFor(oid, map)
  return (b, o, l) => dec(b.subarray(o, o + l))
}

// OIDs whose configured decoder differs from the built-in default — these must NOT be
// inlined (the codegen would bake in the default behavior and ignore the override). They
// route to the `d` helper closure instead, so config.types and jsonBigints actually apply.
const NO_CUSTOM = new Set<number>()
function customOidsOf(map: Map<number, Decoder>): Set<number> {
  if (map === defaultDecoders) return NO_CUSTOM
  const s = new Set<number>()
  for (const [oid, fn] of map) if (fn !== defaultDecoders.get(oid)) s.add(oid)
  return s
}

// Header (cursor + scanner helpers + per-shape parser fns) to inject when any column is a
// *scanned* shaped-JSON column, plus the per-marker plan (fast JSON.parse vs scanner call).
function jsonPrep(cols: CodegenCol[]): { header: string; plan: Map<JsonMarker, JsonPlan> } {
  const markers = cols.filter((c) => c.json).map((c) => c.json!)
  if (!markers.length) return { header: '', plan: new Map() }
  const { decls, plan } = genJsonParsers(markers)
  const header = decls.length ? `\n${DEBUG ? '  // --- shaped JSON sub-parsers (positional) ---\n' : ''}${JSON_RUNTIME}\n${decls.join('\n')}\n` : ''
  return { header, plan }
}

// Emit the per-column decode lines (shared by row builder + result-set mapper). `ind` is the
// indent. Pushes the "read length / decode / advance o" triplet for column i into `out`.
function columnLines(out: string[], col: CodegenCol, i: number, ind: string, helperOids: number[], plan: Map<JsonMarker, JsonPlan>, custom: Set<number>): void {
  const v = `v${i}`
  if (col.json) {
    const pl = plan.get(col.json)!
    if (DEBUG) out.push(`${ind}// ${JSON.stringify(col.name)} (${pl.fast ? 'json fast: JSON.parse' : `json shaped: positional ${col.json.type}`})`)
    out.push(`${ind}${readLen} let ${v} = null;`)
    out.push(pl.fast
      ? `${ind}if (l !== -1) { ${v} = JSON.parse(${str('o', 'o + l')}); o += l }`
      : `${ind}if (l !== -1) { jb = b; jp = o; je = o + l; ${v} = ${pl.call}; o += l }`)
    return
  }
  const inl = custom.has(col.oid) ? null : inlineSnippet(col.oid, v, col.js) // custom decoder -> helper, not inline
  const kind = inl ? inl[1] : 'helper'
  if (DEBUG) out.push(`${ind}// ${JSON.stringify(col.name)} oid=${col.oid}${col.js ? ' :' + col.js : ''} (${kind})`)
  out.push(`${ind}${readLen} let ${v} = null;`)
  if (inl) out.push(`${ind}if (l !== -1) { ${inl[0]}; o += l }`)
  else { const hi = helperOids.length; helperOids.push(col.oid); out.push(`${ind}if (l !== -1) { ${v} = d[${hi}](b, o, l); o += l }`) }
}

// Emit one `key: vN` pair for an object literal. A column literally named __proto__ MUST use a
// computed key (`["__proto__"]: v`) — the plain/quoted form `__proto__: v` sets the object's
// prototype instead of creating an own property (matches the interpreted path's defineProperty
// guard). Only __proto__ is special; every other name is a normal own property.
const objKey = (name: string, i: number) => (name === '__proto__' ? `["__proto__"]: v${i}` : `${JSON.stringify(name)}: v${i}`)

export type RowBuilder = ((body: Buffer) => unknown) & { source: string }

/** Build the readable source of a single-row builder (also used as the compiled body). */
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

/** Build the source of a whole-result-set mapper: takes an array of DataRow bodies,
 *  pre-allocates `new Array(n)`, and decodes every row in one loop (columns inlined). */
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

/** Compile a cached, monomorphic single-row builder for the given columns + mode. */
export function compileRow(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): RowBuilder {
  const { source, helperOids } = rowBuilderSource(cols, mode, customOidsOf(map))
  const helpers = helperOids.map((oid) => helperFor(oid, map))
  if (DEBUG) console.error(`\n[minipg codegen] ${mode} builder for (${cols.map((c) => c.name).join(', ')}):\n${source}\n`)
  const fn = new Function('d', `return (${source})`)(helpers) as RowBuilder
  Object.defineProperty(fn, 'source', { value: source, enumerable: false })
  return fn
}

export type ResultSetMapper = ((rows: Buffer[]) => unknown[]) & { source: string }

/** Compile a cached, monomorphic WHOLE-RESULT-SET mapper: (DataRow bodies[]) -> rows[]. */
export function compileResultSet(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): ResultSetMapper {
  const { source, helperOids } = resultSetSource(cols, mode, customOidsOf(map))
  const helpers = helperOids.map((oid) => helperFor(oid, map))
  if (DEBUG) console.error(`\n[minipg codegen] ${mode} result-set mapper for (${cols.map((c) => c.name).join(', ')}):\n${source}\n`)
  const fn = new Function('d', `return (${source})`)(helpers) as ResultSetMapper
  Object.defineProperty(fn, 'source', { value: source, enumerable: false })
  return fn
}
