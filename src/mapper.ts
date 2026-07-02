// The mapper SEAM: one place the connection asks for a row mapper, given the columns + result mode +
// decoder map. The ONLY difference between the two implementations is how the buffer→POJO mapping is
// assembled — the JIT path compiles a monomorphic per-shape function (new Function, needs eval), the
// interpreted path resolves a per-column CellDecoder[] and loops (no eval). Both call the SAME decode
// strategies (JIT inlines them; interpreted picks them from the catalog in ./decoders.ts), so they
// produce identical rows. `decode:'auto'` uses JIT where eval is available, else interpreted.
import type { Decoder } from './types.ts'
import { compileRow, type CodegenCol } from './decode2.ts'
import { pickDecoder } from './decoders.ts'

export type RowMapper = (body: Buffer) => unknown
export type RowMapperFactory = (cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>) => RowMapper

/** Can this runtime compile functions from strings? (false under strict CSP / Cloudflare Workers.) */
export function isEvalAvailable(): boolean { try { new Function('return 1')(); return true } catch { return false } }

// ---- interpreted: resolve a CellDecoder per column once, decode each row in a loop ----
function interpretedMapper(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): RowMapper {
  const d = cols.map((c) => pickDecoder(c, map))
  const n = d.length
  if (mode === 'object') {
    const names = cols.map((c) => c.name)
    return (b) => {
      let o = 2 // skip the Int16 column count
      const row: Record<string, unknown> = {}
      for (let i = 0; i < n; i++) {
        const l = (b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!; o += 4
        let v: unknown = null
        if (l !== -1) { v = d[i]!(b, o, l); o += l }
        const name = names[i]!
        if (name === '__proto__') Object.defineProperty(row, name, { value: v, writable: true, enumerable: true, configurable: true })
        else row[name] = v
      }
      return row
    }
  }
  return (b) => {
    let o = 2
    const r = new Array(n)
    for (let i = 0; i < n; i++) { const l = (b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!; o += 4; if (l === -1) r[i] = null; else { r[i] = d[i]!(b, o, l); o += l } }
    return r
  }
}

// ---- jit: compile a monomorphic per-shape builder (inlined decode) ----
function jitMapper(cols: CodegenCol[], mode: 'array' | 'object', map: Map<number, Decoder>): RowMapper {
  return compileRow(cols, mode, map)
}

/** Pick the mapper strategy for a `decode` setting. `'jit'` on a no-eval runtime throws (fail fast). */
export function buildMapperFactory(decode: 'auto' | 'jit' | 'interpreted' = 'auto', hasEval = isEvalAvailable()): RowMapperFactory {
  if (decode === 'interpreted') return interpretedMapper
  if (decode === 'jit') { if (!hasEval) throw new Error("minipg: decode:'jit' needs eval (new Function), which this runtime disallows — use decode:'interpreted' or 'auto'"); return jitMapper }
  return hasEval ? jitMapper : interpretedMapper // 'auto'
}
