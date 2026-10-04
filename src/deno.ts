// minipg for Deno — `import { connect, createPool } from 'minipg/deno'`.
// Same driver, Deno's TCP transport: Deno.connect (plus Deno.startTls for the SSLRequest upgrade —
// never connectTls, see below) gives WHATWG streams, bridged to the Node Duplex the core uses.
// (node:stream + node:crypto come from Deno's Node-compat layer.)
import { connect as coreConnect, createPool as corePool, Connection, Pool, PgError, defaultDecoders } from './core.ts'
import type { ConnectConfig, PoolConfig } from './types.ts'
import { refuseChannelBinding, resolveUrl } from './url.ts'
import { duplexFromWeb, negotiateSslRequest } from './webstream.ts'

function denoSocket(config: ConnectConfig) {
  const c = resolveUrl(config) // a `{ url }` config carries host/port/ssl INSIDE the string — parse before dialing, like the cf/neon transports
  const tls = !!(c.ssl && c.ssl !== 'disable')
  const hostname = c.host ?? 'localhost'
  const port = c.port ?? 5432
  const sslObj = typeof c.ssl === 'object' && c.ssl !== null ? (c.ssl as { ca?: unknown; servername?: string }) : undefined
  const caCerts = certList(sslObj?.ca)
  // Deno verifies the certificate against this name; honor an explicit ssl.servername exactly as the
  // node transport does (needed when dialing by IP a cert issued for a hostname).
  const tlsHost = sslObj?.servername ?? hostname
  return async () => {
    if (typeof Deno === 'undefined' || !Deno) throw new Error('minipg/deno: the Deno global is not available (run under Deno, or use minipg/node)')
    if (c.path) { // unix socket (?host=/var/run/postgresql) — never TLS, same as the node transport
      const unix = await Deno.connect({ transport: 'unix', path: c.path })
      return duplexFromWeb(unix.readable, unix.writable)
    }
    const conn = await Deno.connect({ hostname, port })
    if (!tls) return duplexFromWeb(conn.readable, conn.writable)
    // Postgres has NO implicit TLS: Deno.connectTls would send a ClientHello where the server expects a
    // startup packet. Do the SSLRequest dance first, then upgrade the SAME socket (as minipg/cf does).
    await negotiateSslRequest(conn.readable, conn.writable)
    const secure = await Deno.startTls(conn, caCerts ? { hostname: tlsHost, caCerts } : { hostname: tlsHost })
    return duplexFromWeb(secure.readable, secure.writable)
  }
}

// ssl.ca as Deno's caCerts (PEM strings). Accepts the node shapes: one PEM, a Buffer, or an array.
function certList(ca: unknown): string[] | undefined {
  if (ca == null) return undefined
  const one = (v: unknown): string => (typeof v === 'string' ? v : new TextDecoder().decode(v as Uint8Array))
  return (Array.isArray(ca) ? ca : [ca]).map(one)
}

const NO_BINDING = 'Deno.TlsConn exposes only handshake(), and node:tls under Deno returns a certificate with no raw DER'
export async function connect(config: string | ConnectConfig = {}): Promise<Connection> { const c = typeof config === 'string' ? { url: config } : config; return coreConnect({ ...refuseChannelBinding(c, 'deno', NO_BINDING), socket: denoSocket(c) }) }
export function createPool(config: string | PoolConfig = {}): Pool { const c = typeof config === 'string' ? { url: config } : config; return corePool({ ...refuseChannelBinding(c, 'deno', NO_BINDING), socket: denoSocket(c) }) }
export { Connection, Pool, PgError, defaultDecoders }
export type { ConnectConfig, PoolConfig }
