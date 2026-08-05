// The Postgres STARTTLS exchange minipg/cf performs before startTls() — unit-tested over plain Web
// streams (no workerd needed): SSLRequest bytes out, one-byte reply in, fail closed on everything
// that isn't a bare 'S'. The workerd side (real cloudflare:sockets) is covered in test/cf.
import { test, expect, describe } from 'bun:test'
import { negotiateSslRequest } from '../../src/webstream.ts'

function fakeSocket(...replies: Uint8Array[]) {
  const written: Uint8Array[] = []
  const readable = new ReadableStream<Uint8Array>({ start(c) { for (const r of replies) c.enqueue(r); c.close() } })
  const writable = new WritableStream<Uint8Array>({ write(chunk) { written.push(chunk) } })
  return { readable, writable, written }
}

describe('minipg/cf STARTTLS negotiation', () => {
  test("sends the exact 8-byte SSLRequest and accepts a bare 'S'", async () => {
    const s = fakeSocket(new Uint8Array([0x53]))
    await negotiateSslRequest(s.readable, s.writable)
    expect(s.written.length).toBe(1)
    const req = Buffer.from(s.written[0]!)
    expect(req.length).toBe(8)
    expect(req.readInt32BE(0)).toBe(8)          // length
    expect(req.readInt32BE(4)).toBe(80877103)   // SSLRequest code
    // both locks released: the streams are lockable again (startTls precondition)
    ;(s.readable as ReadableStream).getReader().releaseLock()
    ;(s.writable as WritableStream).getWriter().releaseLock()
  })

  test("'N' refusal, non-Postgres replies, trailing bytes, and EOF all fail closed", async () => {
    const attempt = (...replies: Uint8Array[]) => { const s = fakeSocket(...replies); return negotiateSslRequest(s.readable, s.writable) }
    await expect(attempt(new Uint8Array([0x4e]))).rejects.toThrow(/server refused TLS \('N'/)
    await expect(attempt(new Uint8Array([0x58]))).rejects.toThrow(/unexpected SSLRequest reply 0x58/)
    await expect(attempt(new Uint8Array([0x53, 0x16]))).rejects.toThrow(/unexpected data after the SSLRequest reply/) // nothing may ride behind 'S'
    await expect(attempt()).rejects.toThrow(/closed during the SSLRequest exchange/)
  })
})

test('the node transport refuses to run on workerd with a pointed error (root-entry-in-Worker trap)', async () => {
  const desc = Object.getOwnPropertyDescriptor(globalThis, 'navigator')
  Object.defineProperty(globalThis, 'navigator', { value: { userAgent: 'Cloudflare-Workers' }, configurable: true })
  try {
    const { nodeTransport } = await import('../../src/transport-node.ts')
    await expect(nodeTransport({} as never, new AbortController().signal)).rejects.toThrow(/cannot run on Cloudflare Workers — import from 'minipg\/cf'/)
  } finally {
    if (desc) Object.defineProperty(globalThis, 'navigator', desc)
    else delete (globalThis as Record<string, unknown>).navigator
  }
})
