// minipg for Cloudflare Workers — `import { connect, createPool } from 'minipg/cf'`.
// Uses Cloudflare's raw-TCP `connect()` from 'cloudflare:sockets'. Requires the `nodejs_compat`
// compatibility flag (for Buffer, node:crypto, node:stream). TLS is negotiated by the socket
// (secureTransport: 'on'); the core's own SSL step is skipped on the custom-transport path.
import { connect as cfConnect } from 'cloudflare:sockets'
import { connect as coreConnect, createPool as corePool, Connection, Pool, PgError, defaultDecoders } from './core.ts'
import type { ConnectConfig, PoolConfig } from './types.ts'
import { resolveUrl } from './url.ts'
import { duplexFromWeb } from './webstream.ts'

function cfSocket(config: ConnectConfig) {
  const c = resolveUrl(config) // a `{ url }` config carries host/port/ssl INSIDE the string — parse before dialing, like the neon transports
  return async () => {
    // NB cloudflare:sockets verifies the server cert whenever TLS is on, so sslmode=require
    // (encrypt-without-verify elsewhere) effectively behaves like verify-full here.
    const sock = cfConnect({ hostname: c.host ?? 'localhost', port: c.port ?? 5432 }, { secureTransport: c.ssl && c.ssl !== 'disable' ? 'on' : 'off', allowHalfOpen: false })
    await sock.opened // wait for the TCP (and TLS, if on) handshake
    return duplexFromWeb(sock.readable, sock.writable)
  }
}

export function connect(config: string | ConnectConfig = {}): Promise<Connection> { const c = typeof config === 'string' ? { url: config } : config; return coreConnect({ ...c, socket: cfSocket(c) }) }
export function createPool(config: string | PoolConfig = {}): Pool { const c = typeof config === 'string' ? { url: config } : config; return corePool({ ...c, socket: cfSocket(c) }) }
export { Connection, Pool, PgError, defaultDecoders }
export type { ConnectConfig, PoolConfig }
