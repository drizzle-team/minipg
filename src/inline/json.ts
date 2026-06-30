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

export interface JsonMarker {
  readonly __json: 'object' | 'array'
  readonly type: 'json' | 'jsonb'
  readonly spec: JsonSpec
}
/** A nested JSON shape: field name -> PG type alias (optionally `:number`/`:string`), or a nested Json()/Jsonb()/array. */
export type JsonSpec = Record<string, string | JsonMarker>

/** Declare a `json` column that is a single object of a known shape (keys in declared order). */
export function Json(spec: JsonSpec): JsonMarker { return { __json: 'object', type: 'json', spec } }
/** Declare a `json` column that is an array of objects of a known shape (json_agg). */
export function JsonArray(spec: JsonSpec): JsonMarker { return { __json: 'array', type: 'json', spec } }
/** Declare a `jsonb` column that is a single object (keys read in jsonb's sorted order). */
export function Jsonb(spec: JsonSpec): JsonMarker { return { __json: 'object', type: 'jsonb', spec } }
/** Declare a `jsonb` column that is an array of objects of a known shape (jsonb_agg). */
export function JsonbArray(spec: JsonSpec): JsonMarker { return { __json: 'array', type: 'jsonb', spec } }
export function isJsonMarker(x: unknown): x is JsonMarker {
  return typeof x === 'object' && x !== null && typeof (x as { __json?: unknown }).__json === 'string'
}

export type JsTarget = 'number' | 'string'
/** Split a "<pgtype>" or "<pgtype>:number|string" spec into its PG type and JS-target override. */
export function splitType(s: string): { pg: string; js?: JsTarget } {
  const i = s.indexOf(':')
  if (i === -1) return { pg: s.trim() }
  const js = s.slice(i + 1).trim().toLowerCase()
  if (js !== 'number' && js !== 'string') throw new Error(`unknown JS target ${JSON.stringify(js)} in ${JSON.stringify(s)} (use ':number' or ':string')`)
  return { pg: s.slice(0, i).trim(), js }
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
  if (js) return js
  if (cat === 'number') return 'number'
  if (cat === 'bool') return 'bool'
  if (cat === 'json') return 'json'
  return 'string' // precision defaults to exact string; text stays string
}

/** True if anywhere in the shape there's a field whose exact value JSON.parse can't produce
 *  (a precision type left as string). A `:number` override opts out — Number is JSON.parse-safe. */
export function specHasPrecision(spec: JsonSpec): boolean {
  for (const v of Object.values(spec)) {
    if (isJsonMarker(v)) { if (specHasPrecision(v.spec)) return true; continue }
    const { pg, js } = splitType(v)
    if (PRECISION.has(pg.toLowerCase().trim()) && js !== 'number') return true
  }
  return false
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

// Shared scanner helpers + the cursor (jb buffer, jp position, je end). Emitted once at the
// top of any generated mapper that has at least one *scanned* JSON column. The per-shape
// __jN() parsers close over jb/jp/je.
export const JSON_RUNTIME = [
  '  let jb, jp, je; // shaped-JSON cursor: buffer, position, end',
  '  function skipWs() { while (jp < je) { const c = jb[jp]; if (c === 32 || c === 9 || c === 10 || c === 13) jp++; else break } }',
  '  function skipStr() { jp++; while (jp < je) { const c = jb[jp]; if (c === 92) { jp += 2; continue } if (c === 34) { jp++; return } jp++ } }',
  "  function readStr() { jp++; const s = jp; let esc = false; while (jp < je) { const c = jb[jp]; if (c === 92) { esc = true; jp += 2; continue } if (c === 34) break; jp++ } const out = esc ? JSON.parse(jb.toString('utf8', s - 1, jp + 1)) : jb.toString('utf8', s, jp); jp++; return out }",
  "  function readNum() { const s = jp; if (jb[jp] === 45) jp++; while (jp < je) { const c = jb[jp]; if ((c >= 48 && c <= 57) || c === 46 || c === 43 || c === 45 || c === 101 || c === 69) jp++; else break } return Number(jb.toString('utf8', s, jp)) }",
  "  function readRaw() { const s = jp; if (jb[jp] === 45) jp++; while (jp < je) { const c = jb[jp]; if ((c >= 48 && c <= 57) || c === 46 || c === 43 || c === 45 || c === 101 || c === 69) jp++; else break } return jb.toString('utf8', s, jp) }",
  '  function skipValue() { skipWs(); const c = jb[jp]; if (c === 34) { skipStr(); return } if (c === 123 || c === 91) { const open = c, close = c === 123 ? 125 : 93; let depth = 0; while (jp < je) { const d = jb[jp]; if (d === 34) { skipStr(); continue } if (d === open) { depth++; jp++; continue } if (d === close) { depth--; jp++; if (depth === 0) return; continue } jp++ } return } while (jp < je) { const d = jb[jp]; if (d === 44 || d === 125 || d === 93) break; jp++ } }',
  '  function skipToObjEnd() { let depth = 1; while (jp < je) { const c = jb[jp]; if (c === 34) { skipStr(); continue } if (c === 123) depth++; else if (c === 125) { depth--; jp++; if (depth === 0) return; continue } jp++ } }', // advance past the current object\'s matching '}', tolerating extra/trailing fields
].join('\n')

// Snippet that assigns the next JSON value (cursor at value start) into `v`, advancing jp.
// jb[jp]===34 means the value is a quoted string; otherwise a bare number/literal.
function valueSnippet(field: string | JsonMarker, v: string, type: 'json' | 'jsonb', emit: (m: JsonMarker, t: 'json' | 'jsonb') => string): string {
  if (isJsonMarker(field)) {
    // nested shape: if nothing inside needs the scanner, JSON.parse its sub-slice (fast); else recurse
    if (!specHasPrecision(field.spec)) return `if (jb[jp] === 110) { ${v} = null; jp += 4 } else { const _s = jp; skipValue(); ${v} = JSON.parse(jb.toString('utf8', _s, jp)) }`
    return `${v} = ${emit(field, type)}` // __jN() handles its own leading null
  }
  const { pg, js } = splitType(field)
  switch (effJs(category(pg), js)) {
    case 'number': return `if (jb[jp] === 110) { ${v} = null; jp += 4 } else { ${v} = jb[jp] === 34 ? Number(readStr()) : readNum() }`
    case 'bool': return `if (jb[jp] === 110) { ${v} = null; jp += 4 } else { ${v} = jb[jp] === 116; jp += ${v} ? 4 : 5 }`
    case 'json': return `if (jb[jp] === 110) { ${v} = null; jp += 4 } else { const _s = jp; skipValue(); ${v} = JSON.parse(jb.toString('utf8', _s, jp)) }`
    default: return `if (jb[jp] === 110) { ${v} = null; jp += 4 } else { ${v} = jb[jp] === 34 ? readStr() : readRaw() }` // 'string' (exact)
  }
}

function objectSource(fn: string, spec: JsonSpec, type: 'json' | 'jsonb', emit: (m: JsonMarker, t: 'json' | 'jsonb') => string): string {
  const entries = Object.entries(spec)
  const order = wireOrder(entries, type)
  const ret = '{ ' + entries.map(([k], i) => `${JSON.stringify(k)}: v${i}`).join(', ') + ' }'
  const note = type === 'jsonb' ? 'jsonb, wire key order: length,bytewise' : 'json, declared order'
  const lines = [
    `function ${fn}() { // ${note}`,
    `  skipWs(); if (jb[jp] === 110) { jp += 4; return null }       // JSON null`,
    `  jp++;                                                        // '{'`,
    `  let ${entries.map((_, i) => `v${i} = null`).join(', ')};`,
    `  skipWs(); if (jb[jp] === 125) { jp++; return ${ret} }        // '{}'`,
  ]
  order.forEach((di, pos) => {
    const [k, field] = entries[di]!
    lines.push(pos === 0
      ? `  skipStr(); skipWs(); jp++; skipWs();                         // ${JSON.stringify(k)} key + ':'`
      : `  skipWs(); jp++; skipWs(); skipStr(); skipWs(); jp++; skipWs();  // ',' ${JSON.stringify(k)} key + ':'`)
    lines.push(`  ${valueSnippet(field, 'v' + di, type, emit)};`)
  })
  lines.push(`  skipToObjEnd();                                              // past this object's '}' (+ any extra fields)`, `  return ${ret};`, '}')
  return lines.join('\n')
}

function arraySource(fn: string, elemCall: string): string {
  return [
    `function ${fn}() { // array of objects`,
    `  skipWs(); if (jb[jp] === 110) { jp += 4; return null }       // JSON null`,
    `  jp++;                                                        // '['`,
    `  const out = [];`,
    `  skipWs(); if (jb[jp] === 93) { jp++; return out }            // '[]'`,
    `  while (jp < je) {`,
    `    out.push(${elemCall});`,
    `    skipWs(); const c = jb[jp];`,
    `    if (c === 44) { jp++; skipWs(); continue }                 // ','`,
    `    if (c === 93) { jp++; break }                              // ']'`,
    `    break;`,
    `  }`,
    `  return out;`,
    '}',
  ].join('\n')
}

export type JsonPlan = { fast: true } | { fast: false; call: string }

/** Plan + generate parsers for the given top-level markers. A marker that needs no exact
 *  value is { fast: true } (decode via JSON.parse at the call site); otherwise a positional
 *  scanner is generated and { call } is its invocation. `decls` are the scanner functions. */
export function genJsonParsers(markers: JsonMarker[]): { decls: string[]; plan: Map<JsonMarker, JsonPlan> } {
  const decls: string[] = []
  const call = new Map<JsonMarker, string>()
  const plan = new Map<JsonMarker, JsonPlan>()
  let id = 0
  function emit(marker: JsonMarker, type: 'json' | 'jsonb'): string {
    const seen = call.get(marker)
    if (seen) return seen
    const fn = '__j' + id++
    call.set(marker, fn + '()') // set before body so recursive shapes terminate
    if (marker.__json === 'array') decls.push(arraySource(fn, emit({ __json: 'object', type, spec: marker.spec }, type)))
    else decls.push(objectSource(fn, marker.spec, type, emit))
    return fn + '()'
  }
  for (const m of markers) {
    if (!specHasPrecision(m.spec)) plan.set(m, { fast: true })
    else plan.set(m, { fast: false, call: emit(m, m.type) })
  }
  return { decls, plan }
}
