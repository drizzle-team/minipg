// minipg for Cloudflare Workers — `import { connect, createPool } from 'minipg/cf'`.
// Uses Cloudflare's raw-TCP `connect()` from 'cloudflare:sockets'. Requires the `nodejs_compat`
// compatibility flag (for Buffer, node:crypto, node:stream). TLS uses Postgres' STARTTLS dance:
// plaintext SSLRequest -> 'S' -> startTls() (Postgres has no implicit TLS; see negotiateSslRequest).
import { connect as cfConnect } from 'cloudflare:sockets'
import { connect as coreConnect, createPool as corePool, Connection, Pool, PgError, defaultDecoders } from './core.ts'
import type { ConnectConfig, PoolConfig } from './types.ts'
import { refuseChannelBinding, resolveUrl } from './url.ts'
import { duplexFromWeb, negotiateSslRequest } from './webstream.ts'
export { negotiateSslRequest }

function cfSocket(config: ConnectConfig) {
  const c = resolveUrl(config) // a `{ url }` config carries host/port/ssl INSIDE the string — parse before dialing, like the neon transports
  const tls = !!(c.ssl && c.ssl !== 'disable')
  return async () => {
    const sock = cfConnect({ hostname: c.host ?? 'localhost', port: c.port ?? 5432 }, { secureTransport: tls ? 'starttls' : 'off', allowHalfOpen: false })
    await sock.opened // TCP established (plaintext either way; TLS is negotiated below on request)
    if (!tls) return duplexFromWeb(sock.readable, sock.writable)
    await negotiateSslRequest(sock.readable, sock.writable)
    // NB cloudflare:sockets verifies the server cert whenever TLS is on, so sslmode=require
    // (encrypt-without-verify elsewhere) effectively behaves like verify-full here. No
    // expectedServerHostname: workerd rejects the option; SNI defaults to the connect() hostname.
    const secure = sock.startTls()
    // workerd reports handshake failures on `closed` while `opened` stays PENDING — a bare await
    // on opened hangs forever on e.g. an untrusted certificate. Race them.
    await Promise.race([
      secure.opened,
      secure.closed.then(
        () => { throw new Error('minipg/cf: TLS handshake failed — connection closed during startTls (untrusted certificate?)') },
        (e) => { throw new Error(`minipg/cf: TLS handshake failed during startTls — ${(e as Error)?.message ?? e}`) },
      ),
    ])
    return duplexFromWeb(secure.readable, secure.writable)
  }
}

const NO_BINDING = 'the cloudflare:sockets Socket in workerd exposes no certificate API (only close/closed/opened/readable/startTls/writable)'
export async function connect(config: string | ConnectConfig = {}): Promise<Connection> { const c = typeof config === 'string' ? { url: config } : config; return coreConnect({ ...refuseChannelBinding(c, 'cf', NO_BINDING), socket: cfSocket(c) }) }
export function createPool(config: string | PoolConfig = {}): Pool { const c = typeof config === 'string' ? { url: config } : config; return corePool({ ...refuseChannelBinding(c, 'cf', NO_BINDING), socket: cfSocket(c) }) }
export { Connection, Pool, PgError, defaultDecoders }
export type { ConnectConfig, PoolConfig }
