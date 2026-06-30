// Public Shape API: declare a row shape by column name -> PG type (optionally with a
// `:number`/`:string` JS-target override), or a nested Json()/JsonArray() shape. Get back a
// callable mapper compiled via the same codegen the driver uses, with `$printMapper()` to
// inspect the generated function and `$source` to read it.
//
//   const user = Shape({
//     id: 'int4',
//     big: 'int8',                                   // -> exact string (precise)
//     views: 'int8:number',                          // -> JS number (you assert <= 2^53)
//     subscription: Json({ id: 'int4', price: 'numeric' }),          // json, declared order
//     invoices: JsonArray({ id: 'int8', total: 'numeric' }),        // json_agg array
//     prefs: Jsonb({ a: 'int8', b: 'numeric' }),                   // jsonb: read in sorted order
//   })
//   user.$printMapper()              // prints the generated decoder
//   const rows = user(dataRowBodies) // decode an array of DataRow bodies -> rows[]
import { compileResultSet, type CodegenCol } from './codegen.ts'
import { isJsonMarker, splitType, type JsonMarker } from './json.ts'
import { buildDecoders } from './codec.ts'

// PG type alias -> OID (decode kind is chosen by the codegen, same as a live query).
const TYPE_OID: Record<string, number> = {
  int2: 21, smallint: 21, int4: 23, int: 23, integer: 23, serial: 23, oid: 26,
  int8: 20, bigint: 20, float4: 700, real: 700, float8: 701, 'double precision': 701,
  numeric: 1700, decimal: 1700, money: 790, bool: 16, boolean: 16,
  text: 25, varchar: 1043, bpchar: 1042, char: 18, name: 19,
  json: 114, jsonb: 3802, bytea: 17, uuid: 2950,
  date: 1082, time: 1083, timestamp: 1114, timestamptz: 1184, interval: 1186,
}

export type ShapeSpec = Record<string, string | JsonMarker>
export interface ShapeMapper {
  /** Decode all DataRow bodies of a result set into rows (object or array per `mode`). */
  (rows: Buffer[]): unknown[]
  /** The generated mapper source (the JIT mapper over the whole result set). */
  readonly $source: string
  /** The resolved columns (name + OID + overrides). */
  readonly $cols: ReadonlyArray<CodegenCol>
  /** Return the generated mapper source (for pretty/colorize/inspection). */
  $mapper(): string
  /** Print the generated mapper to stdout. */
  $printMapper(): void
}

/** Build a callable, codegen-compiled whole-result-set mapper from a declared shape. */
export function Shape(spec: ShapeSpec, mode: 'object' | 'array' = 'object'): ShapeMapper {
  const cols: CodegenCol[] = Object.entries(spec).map(([name, t]) => {
    if (isJsonMarker(t)) return { name, oid: t.type === 'jsonb' ? 3802 : 114, json: t }
    const { pg, js } = splitType(t)
    const oid = TYPE_OID[pg.toLowerCase()]
    if (oid === undefined) throw new Error(`Shape: unknown type ${JSON.stringify(pg)} for column "${name}" (known: ${Object.keys(TYPE_OID).join(', ')})`)
    return { name, oid, js }
  })
  const mapper = compileResultSet(cols, mode, buildDecoders())
  const fn = ((rows: Buffer[]) => mapper(rows)) as ShapeMapper
  Object.defineProperties(fn, {
    $source: { value: mapper.source, enumerable: false },
    $cols: { value: cols, enumerable: false },
    $mapper: { value: () => mapper.source, enumerable: false },
    $printMapper: { value: () => console.log(mapper.source), enumerable: false },
  })
  return fn
}
