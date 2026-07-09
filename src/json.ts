// Shape-aware JSON decoding. When you know a json/jsonb column's shape upfront
// (e.g. row_to_json / json_agg(row_to_json(...)) over a LATERAL join), declare it with
// Json({...}) / JsonArray({...}). The codegen then picks the decode path automatically:
//
//   wire type | needs an EXACT value JSON.parse can't give (bigint/numeric as string)? | path
//   ----------+---------------------------------------------------------------------+----------------------------
//   json      | no                                                                  | JSON.parse (native, fastest)
//   json      | yes                                                                 | positional scanner, declared order
//   jsonb     | no                                                                  | JSON.parse (native, fastest)
//   jsonb     | yes                                                                 | positional scanner, jsonb sort order
//
// row_to_json/to_jsonb render bigint & numeric as JSON *numbers*, so JSON.parse silently
// truncates them to f64 (9007199254740993 -> ...992). The scanner extracts them as their
// exact raw token STRING. When no field needs that, JSON.parse is correct AND faster.
//
// JS-target override: a field type may be "<pgtype>" or "<pgtype>:number" / "<pgtype>:string".
//   bigint         -> exact string (default; needs the scanner)
//   bigint:string  -> exact string (explicit; needs the scanner)
//   bigint:number  -> JS number — you assert it fits 2^53, so JSON.parse is safe (fast path)
//
// ORDERING: the scanner is positional (skips key text, doesn't match it). `json` preserves
// production order (row_to_json -> column order, json_build_object -> arg order) so we read
// in DECLARED order; `jsonb` normalizes keys to (length, then bytewise) so for type:'jsonb'
// we read in that computed order and reassemble into your declared order. The protocol does
// NOT expose a JSON column's inner types — only the OID (json=114 / jsonb=3802) — which is
// why `type` is declared here.
import type { TypeSpec, ShapeOf, ShapeSpec } from './spec.ts' // type-only (erased): typed value union + generic mapped form

export interface JsonMarker {
  readonly __json: 'object' | 'array'
  readonly type: 'json' | 'jsonb'
  readonly spec: JsonSpec
}
/** A nested JSON shape: field name -> TypeSpec (PG alias, optionally `:number`/`:string`), or a nested Json()/Jsonb()/array. */
export type JsonSpec = Record<string, TypeSpec | JsonMarker>

// Generic over the field names (ShapeOf<K>) so nested json field values autocomplete, same as Shape().
/** Declare a `json` column that is a single object of a known shape (keys in declared order). */
export function Json<K extends string>(spec: ShapeOf<K>): JsonMarker { return { __json: 'object', type: 'json', spec: spec as JsonSpec } }
/** Declare a `json` column that is an array of objects of a known shape (json_agg). */
export function JsonArray<K extends string>(spec: ShapeOf<K>): JsonMarker { return { __json: 'array', type: 'json', spec: spec as JsonSpec } }
/** Declare a `jsonb` column that is a single object (keys read in jsonb's sorted order). */
export function Jsonb<K extends string>(spec: ShapeOf<K>): JsonMarker { return { __json: 'object', type: 'jsonb', spec: spec as JsonSpec } }
/** Declare a `jsonb` column that is an array of objects of a known shape (jsonb_agg). */
export function JsonbArray<K extends string>(spec: ShapeOf<K>): JsonMarker { return { __json: 'array', type: 'jsonb', spec: spec as JsonSpec } }
export function isJsonMarker(x: unknown): x is JsonMarker {
  return typeof x === 'object' && x !== null && typeof (x as { __json?: unknown }).__json === 'string'
}

// ---- Collect / Map / Nullable: TOP-LEVEL row shaping over real RESULT COLUMNS ----------------------------
// These operate on wire columns (unlike Json() which parses ONE json cell), so they are valid at the top of a
// Shape and inside a Collect — NOT inside a Json()/Jsonb() spec (JsonSpec stays TypeSpec|JsonMarker).

/** Group several flat result columns (an ORM join) into ONE nested object per row. Multi-column nesting. Fields
 *  are REQUIRED by default; wrap one in Nullable() to allow a legit NULL. The whole group decodes to `null` when
 *  any required field is NULL, or — when every field is Nullable() — when ALL fields are NULL (a required column
 *  is NOT-NULL in the DB, so either condition only fires on a LEFT-JOIN miss). */
export interface CollectMarker { readonly __collect: true; readonly spec: ShapeSpec }
export function Collect<K extends string>(spec: ShapeOf<K>): CollectMarker { return { __collect: true, spec: spec as ShapeSpec } }
export const isCollectMarker = (x: unknown): x is CollectMarker => typeof x === 'object' && x !== null && (x as { __collect?: unknown }).__collect === true

/** A per-column DECODE-TIME transform: the cell decodes per `type`, then `fn(decoded)` runs during assembly.
 *  `fn` is NOT called for a NULL cell — null passes through unchanged (so fn never has to null-check).
 *  e.g. id: Transform('bigint:number', BigInt), or name: Transform('text', s => s.toUpperCase()). */
export interface TransformMarker { readonly __transform: true; readonly id: number; readonly type: TypeSpec; readonly fn: (v: never) => unknown }
let __xformId = 0
export function Transform<T extends TypeSpec, R>(type: T, fn: (v: never) => R): TransformMarker { return { __transform: true, id: __xformId++, type, fn: fn as (v: never) => unknown } }
export const isTransformMarker = (x: unknown): x is TransformMarker => typeof x === 'object' && x !== null && (x as { __transform?: unknown }).__transform === true

/** Inside a Collect: mark a field as legitimately nullable — excluded from the group's required-presence check. */
export interface NullableMarker { readonly __nullable: true; readonly inner: TypeSpec | TransformMarker | JsonMarker }
export function Nullable(inner: TypeSpec | TransformMarker | JsonMarker): NullableMarker { return { __nullable: true, inner } }
export const isNullableMarker = (x: unknown): x is NullableMarker => typeof x === 'object' && x !== null && (x as { __nullable?: unknown }).__nullable === true

// JS-target overrides:
//   number  -> JS number         string -> string (default)
//   latin1  -> string via single-byte latin1 (you assert ASCII/latin1, e.g. an email — faster than utf8)
//   date    -> Date object        epoch  -> epoch milliseconds (number)
// date/epoch apply to timestamp/date/timestamptz: top-level columns parse the wire value; inside a shaped
// json column they re-parse the ISO string PG serialized into the JSON (Date.parse / new Date).
export type JsTarget = 'number' | 'string' | 'latin1' | 'date' | 'ms' | 'bigint' | 'precise' | 'pretty' | 'tuple' | 'xy' | 'array' | 'f32' | 'sparse' | 'hex' | 'wkb' | 'geojson'
/** Split a "<pgtype>" or "<pgtype>:number|string|latin1|date|ms|bigint|precise|pretty" spec into PG type + JS-target override. */
export function splitType(s: string): { pg: string; js?: JsTarget } {
  const i = s.indexOf(':')
  if (i === -1) return { pg: s.trim() }
  const js = s.slice(i + 1).trim().toLowerCase()
  if (js !== 'number' && js !== 'string' && js !== 'latin1' && js !== 'date' && js !== 'ms' && js !== 'bigint' && js !== 'precise' && js !== 'pretty'
    && js !== 'tuple' && js !== 'xy' && js !== 'array' && js !== 'f32' && js !== 'sparse' && js !== 'hex' && js !== 'wkb' && js !== 'geojson') {
    throw new Error(`unknown JS target ${JSON.stringify(js)} in ${JSON.stringify(s)} (use ':number', ':string', ':latin1', ':date', ':ms', ':bigint', ':precise', ':pretty', ':tuple', ':xy', ':array', ':f32', ':sparse', ':hex', ':wkb' or ':geojson')`)
  }
  return { pg: s.slice(0, i).trim(), js: js as JsTarget }
}

// PG type alias -> decode category.
const NUMBER = new Set(['int2', 'smallint', 'int4', 'int', 'integer', 'serial', 'oid', 'float4', 'real', 'float8', 'double precision'])
const PRECISION = new Set(['int8', 'bigint', 'numeric', 'decimal', 'money']) // exact-string by default; JSON.parse would lose precision
const BOOL = new Set(['bool', 'boolean'])
const RAWJSON = new Set(['json', 'jsonb']) // a sub-field that is itself arbitrary JSON (no declared shape)
function category(alias: string): 'number' | 'precision' | 'bool' | 'json' | 'string' {
  const t = alias.toLowerCase().trim()
  if (NUMBER.has(t)) return 'number'
  if (PRECISION.has(t)) return 'precision'
  if (BOOL.has(t)) return 'bool'
  if (RAWJSON.has(t)) return 'json'
  return 'string' // text/varchar/uuid/date/time/timestamp(tz)/interval/...
}
// Effective JS target for a field, after applying its default-by-category and any override.
function effJs(cat: ReturnType<typeof category>, js?: JsTarget): 'number' | 'string' | 'bool' | 'json' {
  if (js) return js === 'number' ? 'number' : 'string' // latin1/date/epoch collapse to 'string' here; date/epoch are applied as a transform in valueSnippet
  if (cat === 'number') return 'number'
  if (cat === 'bool') return 'bool'
  if (cat === 'json') return 'json'
  return 'string' // precision defaults to exact string; text stays string
}

// date/timestamp/timestamptz default to a JS Date (:string/:number opts out, :ms -> number). Returns
// the temporal target for a field, or null if it isn't a temporal conversion.
const TEMPORAL = new Set(['date', 'timestamp', 'timestamptz'])
function temporalTarget(pg: string, js?: JsTarget): 'date' | 'ms' | null {
  if (js === 'date' || js === 'ms') return js
  if (!js && TEMPORAL.has(pg.toLowerCase().trim())) return 'date'
  return null
}

// int8/bigint default to a JS BigInt (:number -> lossy number, :string -> exact string). numeric/decimal
// have fractional parts so they stay string. True if the field decodes to BigInt.
const BIGINT_PG = new Set(['int8', 'bigint'])
function bigintField(pg: string, js?: JsTarget): boolean {
  if (js === 'bigint') return true
  return !js && BIGINT_PG.has(pg.toLowerCase().trim())
}

// float4/real: 'pretty' (default) = PG's canonical shortest decimal via Number(text); 'precise' = the exact
// stored f32 via Math.fround(Number(text)). Returns the target, or null if the field isn't a float4 number.
const FLOAT4_PG = new Set(['float4', 'real'])
export function float4Target(pg: string, js?: JsTarget): 'precise' | 'pretty' | null {
  if (js === 'string') return null // :string -> raw text, handled elsewhere
  if (!FLOAT4_PG.has(pg.toLowerCase().trim())) return null
  return js === 'precise' ? 'precise' : 'pretty' // bare / :pretty -> pretty
}

/** True if anywhere in the shape there's a field whose exact value JSON.parse can't produce
 *  (a precision type left as string). A `:number` override opts out — Number is JSON.parse-safe. */
export function specHasPrecision(spec: JsonSpec): boolean {
  for (const v of Object.values(spec)) {
    if (isJsonMarker(v)) { if (specHasPrecision(v.spec)) return true; continue }
    const { pg, js } = splitType(v)
    if (temporalTarget(pg, js)) return true // temporal (default Date, or :date/:ms) needs the positional scanner
    if (PRECISION.has(pg.toLowerCase().trim()) && js !== 'number') return true
  }
  return false
}

/** True if the shape has any field the interpreted (no-eval) path must fix up after JSON.parse — a
 *  :ms/:date temporal (ISO string -> Date/number) or an int8/bigint (number/string -> BigInt). Only
 *  shapes that answer true need the post-parse walk below. */
export function specNeedsWalk(spec: JsonSpec): boolean {
  for (const v of Object.values(spec)) {
    if (isJsonMarker(v)) { if (specNeedsWalk(v.spec)) return true; continue }
    const { pg, js } = splitType(v)
    if (temporalTarget(pg, js) || bigintField(pg, js) || float4Target(pg, js) === 'precise') return true
  }
  return false
}

// ISO string -> epoch ms. A naive date-TIME (no 'Z'/offset) is read as UTC (append 'Z') so it matches the
// jit fast-ISO path; a tz-aware datetime and a date-only value go straight to Date.parse.
function isoEpoch(s: string): number {
  if (s.length > 10 && s.charCodeAt(s.length - 1) !== 90) {
    const tail = s.slice(10) // time (+ maybe offset); the date's dashes are already excluded
    if (tail.indexOf('+') === -1 && tail.indexOf('-') === -1) return Date.parse(s + 'Z')
  }
  return Date.parse(s)
}

/** Build a recursive post-JSON.parse walk that converts the shape's :ms/:date (ISO string -> Date/number)
 *  and int8/bigint (number/string -> BigInt) fields in place. The interpreted path decodes shaped json with
 *  JSON.parse, then applies this to match the jit scanner. NB: for int8 > 2^53 JSON.parse already lost
 *  precision, so BigInt(that) is a BigInt of the ROUNDED value — exact only with jsonBigints preserving it
 *  as a string. Access is by key, so jsonb's sorted wire order is irrelevant. */
export function buildJsonWalk(marker: JsonMarker): (v: unknown) => unknown {
  const fns: Array<[string, (x: unknown) => unknown]> = []
  for (const [key, field] of Object.entries(marker.spec)) {
    if (isJsonMarker(field)) { if (specNeedsWalk(field.spec)) fns.push([key, buildJsonWalk(field)]); continue }
    const { pg, js } = splitType(field)
    const tt = temporalTarget(pg, js)
    if (tt === 'ms') fns.push([key, (x) => (typeof x === 'string' ? isoEpoch(x) : x)])
    else if (tt === 'date') fns.push([key, (x) => (typeof x === 'string' ? new Date(isoEpoch(x)) : x)])
    else if (bigintField(pg, js)) fns.push([key, (x) => (typeof x === 'bigint' ? x : BigInt(x as string | number))])
    else if (float4Target(pg, js) === 'precise') fns.push([key, (x) => (typeof x === 'number' ? Math.fround(x) : x)])
  }
  const walkObj = (o: Record<string, unknown>): Record<string, unknown> => {
    for (const [k, f] of fns) { const val = o[k]; if (val != null) o[k] = f(val) }
    return o
  }
  return marker.__json === 'array'
    ? (arr) => { if (Array.isArray(arr)) for (const el of arr) if (el != null) walkObj(el as Record<string, unknown>); return arr }
    : (o) => (o != null ? walkObj(o as Record<string, unknown>) : o)
}

// The order fields appear ON THE WIRE: declared order for json; (length, then bytewise on
// UTF-8) for jsonb. Returns declared-indices in wire order. Stable for equal keys.
function wireOrder(entries: [string, string | JsonMarker][], type: 'json' | 'jsonb'): number[] {
  const idx = entries.map((_, i) => i)
  if (type === 'json') return idx
  return idx.sort((a, b) => {
    const ka = entries[a]![0], kb = entries[b]![0]
    const la = Buffer.byteLength(ka, 'utf8'), lb = Buffer.byteLength(kb, 'utf8')
    return la !== lb ? la - lb : Buffer.compare(Buffer.from(ka, 'utf8'), Buffer.from(kb, 'utf8'))
  })
}

// The shaped-JSON cursor (jb buffer, jp position, je end). The per-shape __jN() parsers close over these.
// The scan PRIMITIVES are INLINED at each field site (below) rather than called — per-field call overhead
// (skipWs/readStr/… run ~dozens of times per object) is the dominant cost, so splicing the bodies in is
// ~2x faster and lets a shaped decode beat JSON.parse on real array-of-objects results.
export const JSON_RUNTIME = '  let jb, jp, je; // shaped-JSON cursor: buffer, position, end'

// Inline snippets over jb/jp/je. All locals are block-scoped (or while-body-scoped) with distinct names,
// so a snippet can be spliced many times into one function body without redeclaration.
// Buffer.utf8Slice/latin1Slice are the internal fast slicers (faster than toString); fall back where absent.
type Slicer = { utf8Slice(s: number, e: number): string; latin1Slice(s: number, e: number): string }
const HAS_U8 = typeof (Buffer.prototype as Partial<Slicer>).utf8Slice === 'function'
const HAS_L1 = typeof (Buffer.prototype as Partial<Slicer>).latin1Slice === 'function'
const U8 = (s: string, e: string) => (HAS_U8 ? `jb.utf8Slice(${s}, ${e})` : `jb.toString('utf8', ${s}, ${e})`)
const L1 = (s: string, e: string) => (HAS_L1 ? `jb.latin1Slice(${s}, ${e})` : `jb.toString('latin1', ${s}, ${e})`) // numbers/tokens are ASCII
const SKIPWS = 'while (jp < je) { const _w = jb[jp]; if (_w === 32 || _w === 9 || _w === 10 || _w === 13) jp++; else break }'
const SKIPSTR = 'jp++; while (jp < je) { const _k = jb[jp]; if (_k === 92) { jp += 2; continue } if (_k === 34) { jp++; break } jp++ }'
// read the quoted string at jp into `v` (escapes -> JSON.parse the token; else raw slice via utf8)
const rdStr = (v: string) => `{ jp++; const _s = jp; let _e = false; while (jp < je) { const _rc = jb[jp]; if (_rc === 92) { _e = true; jp += 2; continue } if (_rc === 34) break; jp++ } ${v} = _e ? JSON.parse(jb.toString('utf8', _s - 1, jp + 1)) : ${U8('_s', 'jp')}; jp++ }`
// read a quoted number token (bigint-as-string in json) -> Number
const rdStrNum = (v: string) => `{ jp++; const _s = jp; while (jp < je && jb[jp] !== 34) jp++; ${v} = Number(${L1('_s', 'jp')}); jp++ }`
// read a bare number token -> Number
const rdNum = (v: string) => `{ const _s = jp; if (jb[jp] === 45) jp++; while (jp < je) { const _n = jb[jp]; if ((_n >= 48 && _n <= 57) || _n === 46 || _n === 43 || _n === 45 || _n === 101 || _n === 69) jp++; else break } ${v} = Number(${L1('_s', 'jp')}) }`
// read a bare number token as an EXACT string
const rdRaw = (v: string) => `{ const _s = jp; if (jb[jp] === 45) jp++; while (jp < je) { const _n = jb[jp]; if ((_n >= 48 && _n <= 57) || _n === 46 || _n === 43 || _n === 45 || _n === 101 || _n === 69) jp++; else break } ${v} = ${L1('_s', 'jp')} }`
// read an integer token (bare, or bigint-as-quoted-string) -> exact JS BigInt
const rdBigInt = (v: string) => `{ if (jb[jp] === 34) { jp++; const _s = jp; while (jp < je && jb[jp] !== 34) jp++; ${v} = BigInt(${U8('_s', 'jp')}); jp++ } else { const _s = jp; if (jb[jp] === 45) jp++; while (jp < je) { const _n = jb[jp]; if (_n >= 48 && _n <= 57) jp++; else break } ${v} = BigInt(${L1('_s', 'jp')}) } }`
// skip an arbitrary value (string/object/array/scalar); leaves jp just past it
const SKIPVALUE = `{ ${SKIPWS} const _vc = jb[jp]; if (_vc === 34) { ${SKIPSTR} } else if (_vc === 123 || _vc === 91) { const _open = _vc, _close = _vc === 123 ? 125 : 93; let _depth = 0; while (jp < je) { const _d = jb[jp]; if (_d === 34) { ${SKIPSTR} continue } if (_d === _open) { _depth++; jp++; continue } if (_d === _close) { _depth--; jp++; if (_depth === 0) break; continue } jp++ } } else { while (jp < je) { const _d = jb[jp]; if (_d === 44 || _d === 125 || _d === 93) break; jp++ } } }`
// advance past the current object's matching '}' (tolerates extra/trailing fields; string-safe)
const SKIPOBJEND = `{ let _od = 1; while (jp < je) { const _oc = jb[jp]; if (_oc === 34) { ${SKIPSTR} continue } if (_oc === 123) _od++; else if (_oc === 125) { _od--; jp++; if (_od === 0) break; continue } jp++ } }`

// ---- Inline scan codegen. Instead of per-shape __jN() functions (which CAPTURE the jb/jp/je cursor ->
// V8 context-allocates it -> slow access in the hot loop), the whole object/array parse is spliced INLINE
// at the column site, keeping the cursor a plain local. `ctx.n` hands out unique variable suffixes so
// multiple/nested markers can share one function scope. ----

// Assign the value at the cursor into `target`, advancing jp. Nested markers recurse inline.
function inlineValue(field: string | JsonMarker, target: string, type: 'json' | 'jsonb', ctx: { n: number }): string {
  if (isJsonMarker(field)) {
    if (!specHasPrecision(field.spec)) { // nested shape with no exact value -> JSON.parse its sub-slice
      const id = ctx.n++
      return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else { const _sv${id} = jp; ${SKIPVALUE} ${target} = JSON.parse(jb.toString('utf8', _sv${id}, jp)) }`
    }
    return inlineMarker(field, target, ctx) // recurse into the scanned object/array
  }
  const { pg, js } = splitType(field)
  const tt = temporalTarget(pg, js)
  if (tt) return inlineEpoch(target, tt, ctx)
  if (bigintField(pg, js)) return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else ${rdBigInt(target)}`
  const f4 = float4Target(pg, js) // float4: bare/:pretty -> Number(token); :precise -> fround it
  if (f4) {
    const rd = `if (jb[jp] === 110) { ${target} = null; jp += 4 } else if (jb[jp] === 34) ${rdStrNum(target)} else ${rdNum(target)}`
    return f4 === 'precise' ? `${rd} if (${target} !== null) ${target} = Math.fround(${target})` : rd
  }
  switch (effJs(category(pg), js)) {
    case 'number': return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else if (jb[jp] === 34) ${rdStrNum(target)} else ${rdNum(target)}`
    case 'bool': return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else { ${target} = jb[jp] === 116; jp += ${target} ? 4 : 5 }`
    case 'json': { const id = ctx.n++; return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else { const _sv${id} = jp; ${SKIPVALUE} ${target} = JSON.parse(jb.toString('utf8', _sv${id}, jp)) }` }
    default: return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else if (jb[jp] === 34) ${rdStr(target)} else ${rdRaw(target)}` // 'string' (exact)
  }
}

// :ms / :date — fast fixed-format ISO byte-parse (naive time = UTC, matching the wire temporal
// convention), falling back to Date.parse for anything not matching YYYY-MM-DD[T ]HH:MM:SS[.f][±HH:MM|Z].
function inlineEpoch(target: string, js: 'ms' | 'date', ctx: { n: number }): string {
  const id = ctx.n++, S = `_ss${id}`, E = `_se${id}`, P = `_ep${id}`
  return `if (jb[jp] === 110) { ${target} = null; jp += 4 } else { `
    + `jp++; const ${S} = jp; while (jp < je && jb[jp] !== 34) jp++; const ${E} = jp; jp++; let ${P}; `
    + `if (${E} - ${S} >= 19 && jb[${S} + 4] === 45 && jb[${S} + 7] === 45 && (jb[${S} + 10] === 84 || jb[${S} + 10] === 32) && jb[${S} + 13] === 58) { `
    + `const _p = ${S}; const _Y=(jb[_p]-48)*1000+(jb[_p+1]-48)*100+(jb[_p+2]-48)*10+(jb[_p+3]-48), _Mo=(jb[_p+5]-48)*10+(jb[_p+6]-48), _D=(jb[_p+8]-48)*10+(jb[_p+9]-48); `
    + `const _H=(jb[_p+11]-48)*10+(jb[_p+12]-48), _Mi=(jb[_p+14]-48)*10+(jb[_p+15]-48), _Sc=(jb[_p+17]-48)*10+(jb[_p+18]-48); `
    + `let _q=_p+19, _ms=0; if (jb[_q]===46){ _q++; let _f=0,_k=0; while(_k<3&&jb[_q]>=48&&jb[_q]<=57){_f=_f*10+(jb[_q]-48);_q++;_k++} while(_k<3){_f*=10;_k++} _ms=_f; while(_q<${E}&&jb[_q]>=48&&jb[_q]<=57)_q++ } `
    + `let _off=0; const _sg=jb[_q]; if(_sg===43||_sg===45){ _q++; const _oh=(jb[_q]-48)*10+(jb[_q+1]-48); _q+=2; let _om=0; if(jb[_q]===58){_q++;_om=(jb[_q]-48)*10+(jb[_q+1]-48);_q+=2} _off=(_sg===45?-1:1)*(_oh*60+_om)*60000 } `
    + `${P} = Date.UTC(_Y,_Mo-1,_D,_H,_Mi,_Sc,_ms); if (_Y <= 99) { const _dd = new Date(${P}); _dd.setUTCFullYear(_Y); ${P} = _dd.getTime() } ${P} -= _off; } else { ${P} = Date.parse(jb.toString('utf8', ${S}, ${E})) } `
    + `${target} = ${js === 'date' ? `new Date(${P})` : P}; }`
}

function inlineMarker(marker: JsonMarker, target: string, ctx: { n: number }): string {
  return marker.__json === 'array' ? inlineArray(marker.spec, marker.type, target, ctx) : inlineObject(marker.spec, marker.type, target, ctx)
}

// Inline object parse into `target`: null-check, '{', per-field (positional) reads, skip to '}', build the
// monomorphic literal. Fields read in WIRE order; the literal is emitted in DECLARED order.
function inlineObject(spec: JsonSpec, type: 'json' | 'jsonb', target: string, ctx: { n: number }): string {
  const id = ctx.n++
  const entries = Object.entries(spec)
  const order = wireOrder(entries, type)
  const vs = entries.map((_, i) => `_o${id}_${i}`)
  // computed key for __proto__ so it becomes an own property, not the object's prototype
  const ret = '{ ' + entries.map(([k], i) => (k === '__proto__' ? `["__proto__"]: ${vs[i]}` : `${JSON.stringify(k)}: ${vs[i]}`)).join(', ') + ' }'
  const fields: string[] = []
  order.forEach((di, pos) => {
    const [, field] = entries[di]!
    fields.push(pos === 0 ? `${SKIPSTR} ${SKIPWS} jp++; ${SKIPWS}` : `${SKIPWS} jp++; ${SKIPWS} ${SKIPSTR} ${SKIPWS} jp++; ${SKIPWS}`)
    fields.push(`${inlineValue(field, vs[di]!, type, ctx)};`)
  })
  return `${SKIPWS} if (jb[jp] === 110) { ${target} = null; jp += 4 } else { jp++; let ${vs.map((v) => `${v} = null`).join(', ')}; `
    + `${SKIPWS} if (jb[jp] === 125) { jp++; ${target} = ${ret} } else { ${fields.join(' ')} ${SKIPOBJEND} ${target} = ${ret}; } }`
}

// Inline array-of-objects parse into `target`: '[', loop each element through an inlined object parse.
function inlineArray(elemSpec: JsonSpec, type: 'json' | 'jsonb', target: string, ctx: { n: number }): string {
  const id = ctx.n++, A = `_a${id}`, EL = `_el${id}`, C = `_ac${id}`
  return `${SKIPWS} if (jb[jp] === 110) { ${target} = null; jp += 4 } else { jp++; const ${A} = []; `
    + `${SKIPWS} if (jb[jp] === 93) { jp++ } else { while (jp < je) { let ${EL}; ${inlineObject(elemSpec, type, EL, ctx)} ${A}.push(${EL}); `
    + `${SKIPWS} const ${C} = jb[jp]; if (${C} === 44) { jp++; ${SKIPWS} continue } if (${C} === 93) { jp++; break } break; } } ${target} = ${A}; }`
}

export type JsonPlan = { fast: true } | { fast: false; inline: (target: string) => string }

/** Plan the top-level markers. Fast (no exact value needed) -> { fast: true } (JSON.parse at the call
 *  site). Otherwise -> { fast: false, inline } where `inline(v)` emits the positional scan code that
 *  assigns the decoded value into `v`, spliced directly at the column site (no per-shape function, so the
 *  jb/jp/je cursor stays a local — ~2x faster than closure-captured __jN parsers). */
export function genJsonParsers(markers: JsonMarker[]): { plan: Map<JsonMarker, JsonPlan> } {
  const plan = new Map<JsonMarker, JsonPlan>()
  for (const m of markers) {
    if (!specHasPrecision(m.spec)) plan.set(m, { fast: true })
    else plan.set(m, { fast: false, inline: (target: string) => inlineMarker(m, target, { n: 0 }) })
  }
  return { plan }
}
