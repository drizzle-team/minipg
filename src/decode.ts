// The row MAPPER: converts DataRow body buffers into JS rows. This is deliberately decoupled from
// the socket/transport and the protocol framing — it just receives a DataRow body (an Int16 column
// count followed by [Int32 length][value bytes] cells) plus the per-column decoders resolved once
// at RowDescription time, and produces a JS row. Pure and side-effect-free, so it can be unit-tested
// and microbenchmarked in isolation (see test/unit/mapper.test.ts, bench/mapper.bench.ts).
import type { Field, ResultMode } from './types.ts'
import type { CellDecoder } from './codec.ts'

// Decode one DataRow body into a JS row — fused parse+decode (no intermediate cells array) using
// the per-column decoders. The body starts with an Int16 column count, hence o = 2; each field is
// [Int32 length][value bytes]. The length is read as a manual signed-BE int32 straight from the four
// bytes (faster than readInt32BE — a method call + bounds check; 0xFFFFFFFF -> -1 for NULL), and the
// value is decoded in place by (buffer, offset, length) — no per-cell subarray.
export function decodeRow(body: Buffer, mode: ResultMode, fields: Field[], decoders: CellDecoder[]): unknown {
  if (mode === 'raw') return Buffer.from(body)
  const n = fields.length
  let o = 2
  if (mode === 'object') {
    // Plain {} (NOT Object.create(null)): V8 keeps a {}+consistent-keys object in fast
    // hidden-class mode, but demotes a null-proto one to dictionary mode (~2.5x slower decode).
    // A column literally named __proto__ is written via defineProperty so it can't pollute.
    const row: Record<string, unknown> = {}
    for (let i = 0; i < n; i++) {
      const l = (body[o]! << 24) | (body[o + 1]! << 16) | (body[o + 2]! << 8) | body[o + 3]!; o += 4
      const name = fields[i]!.name
      let v: unknown = null
      if (l !== -1) { v = decoders[i]!(body, o, l); o += l } // offset decode: no per-cell subarray
      if (name === '__proto__') Object.defineProperty(row, name, { value: v, writable: true, enumerable: true, configurable: true })
      else row[name] = v
    }
    return row
  }
  if (mode === 'buffer') {
    const r = new Array(n)
    // copy each cell into a fresh Buffer (detached from the socket chunk); no subarray view
    for (let i = 0; i < n; i++) { const l = (body[o]! << 24) | (body[o + 1]! << 16) | (body[o + 2]! << 8) | body[o + 3]!; o += 4; if (l === -1) r[i] = null; else { const c = Buffer.allocUnsafe(l); body.copy(c, 0, o, o + l); r[i] = c; o += l } }
    return r
  }
  const r = new Array(n) // 'array'
  for (let i = 0; i < n; i++) { const l = (body[o]! << 24) | (body[o + 1]! << 16) | (body[o + 2]! << 8) | body[o + 3]!; o += 4; if (l === -1) r[i] = null; else { r[i] = decoders[i]!(body, o, l); o += l } }
  return r
}

// Decode a whole result set (many DataRow bodies) into a pre-sized array — the non-stream fast path.
export function decodeRows(bodies: Buffer[], mode: ResultMode, fields: Field[], decoders: CellDecoder[]): unknown[] {
  const n = bodies.length
  const rows = new Array(n)
  for (let i = 0; i < n; i++) rows[i] = decodeRow(bodies[i]!, mode, fields, decoders)
  return rows
}
