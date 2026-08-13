// Proves the receive-timeout/slot-name/keepAlive seams over an in-process fake walsender — no
// cluster required. The fake speaks just enough of the protocol (startup, simple query, CopyBoth)
// to drive replication()'s state machine deterministically; it never sends a ParameterStatus, so
// serverMajor stays 0 and the binary 'auto' probe never engages against it.
import { test, expect } from 'bun:test'
import { Duplex } from 'node:stream'
import { frame } from '../helpers/wire.ts'
import { replication, ReplicationReceiveTimeout, InvalidSlotName, type ReplicationConfig } from '../../src/index.ts'

const i32 = (n: number) => { const b = Buffer.allocUnsafe(4); b.writeInt32BE(n); return b }
const u16 = (n: number) => { const b = Buffer.allocUnsafe(2); b.writeUInt16BE(n); return b }
const cstr = (s: string) => Buffer.concat([Buffer.from(s, 'utf8'), Buffer.from([0])])
const i64zero = Buffer.alloc(8)

const authOk = () => frame('R', i32(0))
const ready = (s = 'I') => frame('Z', Buffer.from(s, 'latin1'))
const rowDesc = (fields: { name: string; oid?: number }[]) => frame('T', Buffer.concat([
  u16(fields.length),
  ...fields.map((f) => Buffer.concat([cstr(f.name), i32(0), u16(0), i32(f.oid ?? 25), u16(0xffff), i32(-1), u16(0)])),
]))
const dataRow = (row: (string | null)[]) => frame('D', Buffer.concat([
  u16(row.length),
  ...row.map((v) => (v === null ? i32(-1) : (() => { const b = Buffer.from(v, 'utf8'); return Buffer.concat([i32(b.length), b]) })())),
]))
const copyBoth = () => frame('W', Buffer.from([0, 0, 0]))
const xlogData = (payload: Buffer) => frame('d', Buffer.concat([Buffer.from('w', 'latin1'), i64zero, i64zero, i64zero, payload]))
const keepalive = (reply = 0) => frame('d', Buffer.concat([Buffer.from('k', 'latin1'), i64zero, i64zero, Buffer.from([reply])]))
const pgBegin = () => xlogData(Buffer.concat([Buffer.from('B', 'latin1'), i64zero, i64zero, i32(1)]))
// unused by this plan's tests — kept for 02-02 (backpressure) and 02-03 (publication probe) to extend
void keepalive; void pgBegin

/** An in-process fake walsender: authenticates unconditionally, then answers each simple-protocol
 *  'Q' via onQuery (default: START_REPLICATION -> CopyBothResponse, else -> ReadyForQuery). */
function fakeBackend(opts: { onQuery?: (sql: string) => Buffer[] } = {}) {
  const sent: Buffer[] = []    // CopyData/Terminate frames received after streaming begins
  const queries: string[] = [] // every 'Q' frame's SQL text, in order
  let authenticated = false
  const onQuery = opts.onQuery ?? ((sql: string) => (sql.startsWith('START_REPLICATION') ? [copyBoth()] : [ready()]))
  const dx: Duplex = new Duplex({
    write(chunk: Buffer, _enc, cb) {
      if (!authenticated) {
        authenticated = true // whole startup packet arrives in one write; no type byte to parse
        dx.push(authOk())
        dx.push(ready())
        cb()
        return
      }
      let off = 0
      while (off < chunk.length) {
        const type = String.fromCharCode(chunk[off]!)
        const len = chunk.readInt32BE(off + 1)
        const body = chunk.subarray(off + 5, off + 1 + len)
        off += 1 + len
        if (type === 'Q') {
          let z = 0; while (body[z] !== 0) z++
          const sql = body.toString('utf8', 0, z)
          queries.push(sql)
          for (const f of onQuery(sql)) dx.push(f)
        } else if (type === 'd' || type === 'X') {
          sent.push(Buffer.from(body))
        }
      }
      cb()
    },
    read() { /* pushed manually */ },
  })
  return { socket: () => dx, dx, sent, queries }
}

const cfg = (o: Partial<ReplicationConfig> = {}): ReplicationConfig => ({ user: 'u', database: 'd', ...o })

test('receive timeout: a silent stream fails the iterator with ReplicationReceiveTimeout', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket }))
  try {
    const gen = repl.start({ slot: 'repl_1_ok', publications: ['pub'], receiveTimeoutMs: 300 })
    const t0 = Date.now()
    let err: unknown
    try { await gen.next() } catch (e) { err = e }
    expect(err).toBeInstanceOf(ReplicationReceiveTimeout)
    expect((err as ReplicationReceiveTimeout).reason).toBe('receive-timeout')
    expect(Date.now() - t0).toBeLessThan(1000)
  } finally { repl.end() }
})

test('receive timeout disarmed: omitting receiveTimeoutMs never fires during a silent window', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket }))
  const gen = repl.start({ slot: 'repl_1_ok', publications: ['pub'] })
  let settled = false
  const p = gen.next().then(() => { settled = true }, () => { settled = true })
  await Bun.sleep(700)
  expect(settled).toBe(false)
  repl.end()
  await p
})

test('slot name: createSlot and dropSlot reject invalid names before any walsender command', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket }))
  try {
    await expect(repl.createSlot('Bad-Name')).rejects.toThrow(InvalidSlotName)
    await expect(repl.dropSlot('has space')).rejects.toThrow(InvalidSlotName)
    expect(backend.queries.some((q) => q.startsWith('CREATE_REPLICATION_SLOT') || q.startsWith('DROP_REPLICATION_SLOT'))).toBe(false)
  } finally { repl.end() }
})

test('slot name: start() rejects lazily on first iteration, writing no START_REPLICATION frame', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket }))
  try {
    const gen = repl.start({ slot: '', publications: ['pub'] })
    await expect(gen.next()).rejects.toThrow(InvalidSlotName)
    expect(backend.queries.some((q) => q.startsWith('START_REPLICATION'))).toBe(false)
  } finally { repl.end() }
})

test('slot name: non-ASCII and over-length names reject; boundary and normal names pass through verbatim', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket }))
  try {
    await expect(repl.createSlot('слот')).rejects.toThrow(InvalidSlotName)
    await expect(repl.createSlot('a'.repeat(64))).rejects.toThrow(InvalidSlotName)
  } finally { repl.end() }

  const backend2 = fakeBackend({
    onQuery(sql) {
      if (sql.startsWith('CREATE_REPLICATION_SLOT')) {
        const name = sql.split(' ')[1]!
        return [
          rowDesc([{ name: 'slot_name' }, { name: 'consistent_point' }, { name: 'snapshot_name' }, { name: 'output_plugin' }]),
          dataRow([name, '0/0', null, 'pgoutput']),
          ready(),
        ]
      }
      return [ready()]
    },
  })
  const repl2 = await replication(cfg({ socket: backend2.socket }))
  try {
    const longName = 'a'.repeat(63)
    const r1 = await repl2.createSlot(longName)
    expect(r1.slot).toBe(longName)
    const r2 = await repl2.createSlot('repl_1_ok')
    expect(r2.slot).toBe('repl_1_ok')
    expect(backend2.queries.some((q) => q.startsWith(`CREATE_REPLICATION_SLOT ${longName}`))).toBe(true)
  } finally { repl2.end() }
})

test('keepAlive: object form reaches setKeepAlive(true, initialDelayMs)', async () => {
  const backend = fakeBackend()
  const calls: [boolean, number | undefined][] = []
  ;(backend.dx as unknown as { setKeepAlive?: (e: boolean, d?: number) => unknown }).setKeepAlive = (e, d) => { calls.push([e, d]) }
  const repl = await replication(cfg({ socket: backend.socket, keepAlive: { initialDelayMs: 7000 } }))
  try { expect(calls).toEqual([[true, 7000]]) } finally { repl.end() }
})

test('keepAlive: true reaches setKeepAlive(true, 0); omitted option never calls it', async () => {
  const backendTrue = fakeBackend()
  const callsTrue: [boolean, number | undefined][] = []
  ;(backendTrue.dx as unknown as { setKeepAlive?: (e: boolean, d?: number) => unknown }).setKeepAlive = (e, d) => { callsTrue.push([e, d]) }
  const replTrue = await replication(cfg({ socket: backendTrue.socket, keepAlive: true }))
  try { expect(callsTrue).toEqual([[true, 0]]) } finally { replTrue.end() }

  const backendOff = fakeBackend()
  const callsOff: [boolean, number | undefined][] = []
  ;(backendOff.dx as unknown as { setKeepAlive?: (e: boolean, d?: number) => unknown }).setKeepAlive = (e, d) => { callsOff.push([e, d]) }
  const replOff = await replication(cfg({ socket: backendOff.socket }))
  try { expect(callsOff).toEqual([]) } finally { replOff.end() }
})

test('keepAlive: a socket lacking setKeepAlive never throws connect()', async () => {
  const backend = fakeBackend()
  const repl = await replication(cfg({ socket: backend.socket, keepAlive: true }))
  repl.end()
})
