import { W } from './protocol.ts'
type ByteReader = { read(): Promise<{ value?: Uint8Array; done: boolean }>; releaseLock(): void }
type ByteWriter = { write(b: Uint8Array): Promise<unknown>; releaseLock(): void }

/** Postgres' STARTTLS exchange over a plaintext socket: send the 8-byte SSLRequest, read the
 *  one-byte reply, fail closed on anything but a bare 'S'. Postgres has NO implicit TLS — a
 *  `secureTransport: 'on'` socket would send a TLS ClientHello where the server expects a startup
 *  packet. Lives here (not cf.ts) so unit tests can import it without the cloudflare:sockets specifier. */
export async function negotiateSslRequest(readable: unknown, writable: unknown): Promise<void> {
  const reader = (readable as { getReader(): ByteReader }).getReader()
  const writer = (writable as { getWriter(): ByteWriter }).getWriter()
  try {
    await writer.write(W.sslRequest())
    const { value, done } = await reader.read()
    if (done || !value || value.byteLength === 0) throw new Error('minipg/cf: connection closed during the SSLRequest exchange')
    const b = value[0]!
    if (b === 0x4e) throw new Error("minipg/cf: server refused TLS ('N' to SSLRequest) — sslmode=require cannot proceed (is ssl enabled server-side?)") // 'N'
    if (b !== 0x53) throw new Error(`minipg/cf: unexpected SSLRequest reply 0x${b.toString(16)} — not a Postgres server?`) // not 'S'
    if (value.byteLength > 1) throw new Error('minipg/cf: unexpected data after the SSLRequest reply — refusing to upgrade') // nothing may be buffered past 'S'
  } finally {
    reader.releaseLock(); writer.releaseLock() // BOTH locks must be free before startTls()
  }
}

// Bridge a runtime's WHATWG Web streams (Deno.Conn / Cloudflare Socket expose `.readable`/`.writable`)
// into a Node Duplex the core connection uses as its transport. Chunks arrive as Uint8Array; the
// connection's onData wraps them as Buffer views (no copy). Requires node:stream (present in Node/Bun,
// and in Deno / Cloudflare Workers with nodejs_compat).
import { Duplex } from 'node:stream'

export function duplexFromWeb(readable: unknown, writable: unknown): Duplex {
  // the { readable, writable } overload is valid at runtime but not in the bundled node:stream types
  return Duplex.from({ readable, writable } as never) as Duplex
}
