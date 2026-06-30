// PostgreSQL frontend/backend wire protocol v3 — message framing only.
// No I/O here: writers return Buffers; Parser turns socket bytes into messages.
import type { Field } from './types.ts'
import type { EncodedParam } from './codec.ts'

const PROTOCOL_VERSION = 196608 // 3 << 16

const i16 = (n: number) => { const b = Buffer.allocUnsafe(2); b.writeUInt16BE(n); return b } // protocol int16 counts are non-negative
const i32 = (n: number) => { const b = Buffer.allocUnsafe(4); b.writeInt32BE(n); return b }
const cstr = (s: string) => Buffer.concat([Buffer.from(s, 'utf8'), Buffer.from([0])])
const guardNul = (s: string, what: string) => { if (s.indexOf('\0') !== -1) throw new Error(`${what} contains NUL byte (0x00)`) }

// type byte + int32 length (length covers itself + payload) + payload
const msg = (type: string, payload: Buffer = Buffer.alloc(0)) =>
  Buffer.concat([Buffer.from(type, 'latin1'), i32(payload.length + 4), payload])

export const W = {
  startup(params: Record<string, string | undefined>): Buffer {
    const parts: Buffer[] = [i32(PROTOCOL_VERSION)]
    for (const [k, v] of Object.entries(params)) { if (v == null) continue; parts.push(cstr(k), cstr(v)) }
    parts.push(Buffer.from([0]))
    const payload = Buffer.concat(parts)
    return Buffer.concat([i32(payload.length + 4), payload]) // startup has no type byte
  },
  password: (s: string) => msg('p', cstr(s)),
  saslInitial(mechanism: string, clientFirst: string): Buffer {
    const cf = Buffer.from(clientFirst, 'utf8')
    return msg('p', Buffer.concat([cstr(mechanism), i32(cf.length), cf]))
  },
  saslResponse: (clientFinal: string) => msg('p', Buffer.from(clientFinal, 'utf8')),

  parse(name: string, sql: string): Buffer {
    guardNul(sql, 'query text')
    return msg('P', Buffer.concat([cstr(name), cstr(sql), i16(0)])) // 0 param-type oids => server infers
  },
  bind(portal: string, statement: string, params: EncodedParam[], resultFormat = 0): Buffer {
    if (params.length > 65535) throw new Error(`too many bind parameters: ${params.length} (max 65535)`)
    const parts: Buffer[] = [cstr(portal), cstr(statement), i16(params.length)]
    for (const p of params) parts.push(i16(p.format))
    parts.push(i16(params.length))
    for (const p of params) {
      if (p.bytes == null) parts.push(i32(-1))
      else parts.push(i32(p.bytes.length), p.bytes)
    }
    parts.push(i16(1), i16(resultFormat)) // one result-format code applied to all columns
    return msg('B', Buffer.concat(parts))
  },
  describe: (kind: 'S' | 'P', name: string) => msg('D', Buffer.concat([Buffer.from(kind, 'latin1'), cstr(name)])),
  execute: (portal: string, maxRows = 0) => msg('E', Buffer.concat([cstr(portal), i32(maxRows)])),
  close: (kind: 'S' | 'P', name: string) => msg('C', Buffer.concat([Buffer.from(kind, 'latin1'), cstr(name)])),
  sync: () => msg('S'),
  flush: () => msg('H'),
  terminate: () => msg('X'),
  copyFail: (reason: string) => msg('f', cstr(reason)),
  sslRequest: () => Buffer.concat([i32(8), i32(80877103)]),
  // Sent on a SEPARATE connection to cancel an in-flight query on the backend
  // identified by (pid, secretKey) from BackendKeyData. No reply; server closes it.
  cancelRequest: (pid: number, secret: number) => Buffer.concat([i32(16), i32(80877102), i32(pid), i32(secret)]),
}

// Reusable growable write buffer with back-patched int32 message lengths — eliminates the
// per-message Buffer.from/allocUnsafe/Buffer.concat the `W` builders do. One instance is reused
// per query (one query in flight at a time), so the hot extended-query packet is ~1 allocation.
export class Writer {
  private buf: Buffer
  private off = 0
  private msgStart = 0
  constructor(size = 4096) { this.buf = Buffer.allocUnsafe(size) }
  reset(): void { this.off = 0 }
  private ensure(n: number): void {
    const need = this.off + n
    if (need <= this.buf.length) return
    let cap = this.buf.length * 2
    while (cap < need) cap *= 2
    const nb = Buffer.allocUnsafe(cap)
    this.buf.copy(nb, 0, 0, this.off)
    this.buf = nb
  }
  byte(n: number): void { this.ensure(1); this.buf[this.off++] = n }
  int16(n: number): void { this.ensure(2); this.buf.writeUInt16BE(n, this.off); this.off += 2 }
  int32(n: number): void { this.ensure(4); this.buf.writeInt32BE(n, this.off); this.off += 4 }
  str(s: string): void { const len = Buffer.byteLength(s, 'utf8'); this.ensure(len); this.buf.write(s, this.off, 'utf8'); this.off += len } // no NUL terminator
  cstr(s: string): void { this.str(s); this.byte(0) }
  bytes(b: Buffer): void { this.ensure(b.length); b.copy(this.buf, this.off); this.off += b.length }
  // type byte + reserved int32 length (back-patched in end() to cover itself + payload)
  start(type: string): void { this.byte(type.charCodeAt(0)); this.msgStart = this.off; this.ensure(4); this.off += 4 }
  end(): void { this.buf.writeInt32BE(this.off - this.msgStart, this.msgStart) }
  slice(): Buffer { return this.buf.subarray(0, this.off) }
}

export function writeParse(w: Writer, name: string, sql: string): void {
  guardNul(sql, 'query text')
  w.start('P'); w.cstr(name); w.cstr(sql); w.int16(0); w.end() // 0 param-type oids => server infers
}
export function writeDescribe(w: Writer, kind: 'S' | 'P', name: string): void {
  w.start('D'); w.byte(kind.charCodeAt(0)); w.cstr(name); w.end()
}
export function writeExecute(w: Writer, portal: string, maxRows = 0): void {
  w.start('E'); w.cstr(portal); w.int32(maxRows); w.end()
}
export function writeClose(w: Writer, kind: 'S' | 'P', name: string): void {
  w.start('C'); w.byte(kind.charCodeAt(0)); w.cstr(name); w.end()
}
export function writeSync(w: Writer): void { w.start('S'); w.end() }

// Encode one Bind parameter straight into the write buffer — no intermediate per-param Buffer.
// Length-prefixed: int32 byte-length (computed without allocating) then the bytes.
function writeParam(w: Writer, p: unknown): void {
  if (p == null) { w.int32(-1); return }            // SQL NULL
  if (Buffer.isBuffer(p)) { w.int32(p.length); w.bytes(p); return } // binary param (format 1)
  if (typeof p === 'boolean') { w.int32(1); w.byte(p ? 116 : 102); return } // 't' / 'f'
  const s = p instanceof Date ? p.toISOString() : typeof p === 'object' ? JSON.stringify(p) : String(p)
  guardNul(s, 'parameter')
  w.int32(Buffer.byteLength(s, 'utf8')); w.str(s)
}

export function writeBind(w: Writer, portal: string, statement: string, params: unknown[], resultFormat = 0): void {
  if (!Array.isArray(params)) throw new TypeError('params must be an array')
  if (params.length > 65535) throw new Error(`too many bind parameters: ${params.length} (max 65535)`)
  w.start('B'); w.cstr(portal); w.cstr(statement)
  w.int16(params.length)
  for (const p of params) w.int16(Buffer.isBuffer(p) ? 1 : 0) // per-param format: 1 binary (Buffer), else 0 text
  w.int16(params.length)
  for (const p of params) writeParam(w, p)
  w.int16(1); w.int16(resultFormat) // one result-format code applied to all columns
  w.end()
}

export interface RawMessage { type: string; body: Buffer }

/** Incremental parser: feed socket chunks, get back complete messages. */
export class Parser {
  private buf: Buffer = Buffer.alloc(0)
  push(chunk: Buffer): RawMessage[] {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk
    const out: RawMessage[] = []
    let off = 0
    while (this.buf.length - off >= 5) {
      const len = this.buf.readInt32BE(off + 1)
      if (len < 4) throw new Error(`invalid backend message length: ${len}`) // guard: avoid non-advancing offset / infinite loop
      const total = len + 1
      if (this.buf.length - off < total) break
      out.push({ type: String.fromCharCode(this.buf[off]!), body: this.buf.subarray(off + 5, off + total) })
      off += total
    }
    this.buf = off === this.buf.length ? Buffer.alloc(0) : this.buf.subarray(off)
    return out
  }
}

/** Decode a RowDescription ('T') body into field descriptors. */
export function parseRowDescription(body: Buffer): Field[] {
  const n = body.readInt16BE(0)
  const fields: Field[] = []
  let off = 2
  for (let i = 0; i < n; i++) {
    let end = off
    while (body[end] !== 0) end += 1
    const name = body.toString('utf8', off, end)
    off = end + 1
    fields.push({
      name,
      tableOid: body.readInt32BE(off),
      columnId: body.readInt16BE(off + 4),
      dataTypeOid: body.readInt32BE(off + 6),
      dataTypeSize: body.readInt16BE(off + 10),
      typeModifier: body.readInt32BE(off + 12),
      format: body.readInt16BE(off + 16),
    })
    off += 18
  }
  return fields
}

/** Decode a DataRow ('D') body into raw (Buffer|null) field values. */
export function parseDataRow(body: Buffer): (Buffer | null)[] {
  const n = body.readInt16BE(0)
  const cells: (Buffer | null)[] = new Array(n)
  let off = 2
  for (let i = 0; i < n; i++) {
    const len = body.readInt32BE(off); off += 4
    if (len === -1) cells[i] = null
    else { cells[i] = body.subarray(off, off + len); off += len }
  }
  return cells
}
