// minipg for Deno — `import { connect, createPool } from 'minipg/deno'`.
// Same driver, Deno's TCP transport: Deno.connect / Deno.connectTls give WHATWG streams, bridged to
// the Node Duplex the core uses. (node:stream + node:crypto come from Deno's Node-compat layer.)
import { connect as coreConnect, createPool as corePool, Connection, Pool, PgError, defaultDecoders } from './inline/core.ts'
import type { ConnectConfig, PoolConfig } from './inline/types.ts'
import { duplexFromWeb } from './inline/webstream.ts'

function denoSocket(c: ConnectConfig) {
  return async () => {
    if (typeof Deno === 'undefined' || !Deno) throw new Error('minipg/deno: the Deno global is not available (run under Deno, or use minipg/node)')
    const opts = { hostname: c.host ?? 'localhost', port: c.port ?? 5432 }
    const conn = c.ssl ? await Deno.connectTls(opts) : await Deno.connect(opts) // ssl truthy -> TLS at the transport
    return duplexFromWeb(conn.readable, conn.writable)
  }
}

export function connect(config: ConnectConfig = {}): Promise<Connection> { return coreConnect({ ...config, socket: denoSocket(config) }) }
export function createPool(config: PoolConfig = {}): Pool { return corePool({ ...config, socket: denoSocket(config) }) }
export { Connection, Pool, PgError, defaultDecoders }
export type { ConnectConfig, PoolConfig }
