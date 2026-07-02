// minipg for Cloudflare Workers — `import { connect, createPool } from 'minipg/cf'`.
// Uses Cloudflare's raw-TCP `connect()` from 'cloudflare:sockets'. Requires the `nodejs_compat`
// compatibility flag (for Buffer, node:crypto, node:stream). TLS is negotiated by the socket
// (secureTransport: 'on'); the core's own SSL step is skipped on the custom-transport path.
import { connect as cfConnect } from 'cloudflare:sockets'
import { connect as coreConnect, createPool as corePool, Connection, Pool, PgError, defaultDecoders } from './inline/core.ts'
import type { ConnectConfig, PoolConfig } from './inline/types.ts'
import { duplexFromWeb } from './inline/webstream.ts'

function cfSocket(c: ConnectConfig) {
  return async () => {
    const sock = cfConnect({ hostname: c.host ?? 'localhost', port: c.port ?? 5432 }, { secureTransport: c.ssl ? 'on' : 'off', allowHalfOpen: false })
    await sock.opened // wait for the TCP (and TLS, if on) handshake
    return duplexFromWeb(sock.readable, sock.writable)
  }
}

export function connect(config: ConnectConfig = {}): Promise<Connection> { return coreConnect({ ...config, socket: cfSocket(config) }) }
export function createPool(config: PoolConfig = {}): Pool { return corePool({ ...config, socket: cfSocket(config) }) }
export { Connection, Pool, PgError, defaultDecoders }
export type { ConnectConfig, PoolConfig }
