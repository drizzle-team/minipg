// Resolve a declarative row shape (column name -> PG type alias, optionally with a `:number`/`:string`
// JS-target override, or a Json()/Jsonb() marker) into the driver's column plan (CodegenCol[]). Kept
// separate from shape.ts so the connection can resolve a `{ shape }` query option WITHOUT pulling in the
// whole-result-set codegen used by the standalone Shape() helper. No node deps (imports only json/types).
import { isJsonMarker, splitType, type JsonMarker } from './json.ts'
import { BINARY_FAST, type CodegenCol } from './decode2.ts'

// PG type alias -> OID. `satisfies` (not a `: Record<…>` annotation) keeps the literal keys so PgType can
// derive the alias union straight from this map — the type list and the runtime map can never drift apart.
const TYPE_OID = {
  int2: 21, smallint: 21, int4: 23, int: 23, integer: 23, serial: 23, oid: 26,
  int8: 20, bigint: 20, float4: 700, real: 700, float8: 701, 'double precision': 701,
  numeric: 1700, decimal: 1700, money: 790, bool: 16, boolean: 16,
  text: 25, varchar: 1043, bpchar: 1042, char: 18, name: 19,
  json: 114, jsonb: 3802, bytea: 17, uuid: 2950,
  date: 1082, time: 1083, timestamp: 1114, timestamptz: 1184, interval: 1186,
} satisfies Record<string, number>

/** A known PG type alias (e.g. 'int4', 'bigint', 'timestamptz'). */
export type PgType = keyof typeof TYPE_OID

// scalar OID -> its array-type OID (pg_type.typarray), for `params: ['int8[]']` & insertMany casts
const ARRAY_OID: Record<number, number> = {
  16: 1000, 21: 1005, 23: 1007, 26: 1028, 20: 1016, 700: 1021, 701: 1022, 1700: 1231, 790: 791,
  25: 1009, 1043: 1015, 1042: 1014, 18: 1002, 19: 1003, 114: 199, 3802: 3807, 17: 1001, 2950: 2951,
  1082: 1182, 1083: 1183, 1114: 1115, 1184: 1185, 1186: 1187,
}

/** A declared param type: a PG alias, its array form, or a raw OID. */
export type ParamType = number | PgType | `${PgType}[]`

/** Resolve ONE declared param type ('int8', 'text[]', or a raw OID) to its wire OID. */
export function paramTypeOid(t: number | string): number {
  if (typeof t === 'number') return t
  const arr = t.endsWith('[]')
  const base = (TYPE_OID as Record<string, number | undefined>)[(arr ? t.slice(0, -2) : t).toLowerCase()]
  if (base === undefined) throw new Error(`minipg: unknown param type ${JSON.stringify(t)} (known: ${Object.keys(TYPE_OID).join(', ')}, each also with [])`)
  if (!arr) return base
  const a = ARRAY_OID[base]
  if (a === undefined) throw new Error(`minipg: no array type known for ${JSON.stringify(t)}`)
  return a
}

// declared-params arrays are meant to be module-scope constants — cache resolution by identity
const resolvedParams = new WeakMap<readonly (number | string)[], number[]>()
export function resolveParamTypes(ts: readonly (number | string)[]): readonly number[] {
  let r = resolvedParams.get(ts)
  if (!r) { r = ts.map(paramTypeOid); resolvedParams.set(ts, r) }
  return r
}

// Per-type JS-target overrides (mirrors the runtime groups in json.ts). Every type also accepts `:string`
// (its exact PG text). Temporal adds `:date` (default) / `:ms` (epoch number); precision adds `:number`;
// text adds `:latin1`. So e.g. `timestamptz` offers date/ms/string — NOT :number/:latin1.
type TemporalType = 'date' | 'timestamp' | 'timestamptz'
type IntType = 'int8' | 'bigint'                     // default BigInt; :number (lossy) / :string
type NumericType = 'numeric' | 'decimal' | 'money'   // default exact string; :number (lossy)
type Float4Type = 'float4' | 'real'                  // default/:pretty -> canonical shortest; :precise -> exact f32
type TextType = 'text' | 'varchar' | 'bpchar' | 'char' | 'name'
/** A column's type in a shape: a PG alias, plus the JS-target overrides valid for it (autocompletes to the
 *  known list). e.g. `'bigint'` -> BigInt, `'bigint:number'` -> JS number, `'timestamptz:ms'` -> epoch ms,
 *  `'float4:precise'` -> exact stored f32, `'float4:pretty'` -> PG's canonical shortest decimal. */
export type TypeSpec =
  | PgType
  | `${PgType}:string`
  | `${TemporalType}:${'date' | 'ms'}`
  | `${IntType}:${'number' | 'bigint'}`
  | `${NumericType}:number`
  | `${Float4Type}:${'precise' | 'pretty'}`
  | `${TextType}:latin1`
/** A row shape: column name -> TypeSpec, or a Json()/Jsonb()/…Array() marker for a shaped json column. */
export type ShapeSpec = Record<string, TypeSpec | JsonMarker>
/** The same value type as ShapeSpec, but over KNOWN keys `K`. The public shape-taking functions use this
 *  generic form (`fn<K extends string>(spec: ShapeOf<K>)`) so editors offer value autocomplete — TypeScript
 *  does NOT surface value completions through a `Record<string, …>` index signature, but does through a
 *  mapped type over inferred keys. Same constraint either way; only the completion UX differs. */
export type ShapeOf<K extends string> = { [P in K]: TypeSpec | JsonMarker }

/** Resolve a ShapeSpec into columns (name + wire OID + optional JS target / JSON marker / wire format). */
export function shapeCols(spec: ShapeSpec): CodegenCol[] {
  return Object.entries(spec).map(([name, t]) => {
    if (isJsonMarker(t)) return { name, oid: t.type === 'jsonb' ? 3802 : 114, json: t }
    const { pg, js } = splitType(t)
    const oid = (TYPE_OID as Record<string, number | undefined>)[pg.toLowerCase()] // pg is user text -> string index
    if (oid === undefined) throw new Error(`minipg: unknown type ${JSON.stringify(pg)} for column "${name}" in shape (known: ${Object.keys(TYPE_OID).join(', ')})`)
    // Auto-request BINARY wire format for bench-proven-faster types (see BINARY_FAST). The ONLY unsafe case
    // is `:string` on a non-int8 type: binary yields the decoded value (number/Date/Buffer), never the PG
    // text — only int8:string reconstructs the exact decimal string from the int64. (int8:number DOES go
    // binary — the >2^53 rounding difference is accepted. temporal:'string' via the global config is
    // downgraded to text in Connection.resolveCols, after this.)
    const binaryUnsafe = js === 'string' && oid !== 20
    let binary = BINARY_FAST.has(oid) && !binaryUnsafe
    if (oid === 700) binary = js === 'precise' // float4: only :precise (exact f32) goes binary; bare/:pretty/:string stay text (canonical)
    return binary ? { name, oid, js, format: 'binary' } : { name, oid, js }
  })
}
