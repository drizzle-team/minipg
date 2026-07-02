// Bridge a runtime's WHATWG Web streams (Deno.Conn / Cloudflare Socket expose `.readable`/`.writable`)
// into a Node Duplex the core connection uses as its transport. Chunks arrive as Uint8Array; the
// connection's onData wraps them as Buffer views (no copy). Requires node:stream (present in Node/Bun,
// and in Deno / Cloudflare Workers with nodejs_compat).
import { Duplex } from 'node:stream'

export function duplexFromWeb(readable: unknown, writable: unknown): Duplex {
  // the { readable, writable } overload is valid at runtime but not in the bundled node:stream types
  return Duplex.from({ readable, writable } as never) as Duplex
}
