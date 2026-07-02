// minipg — a small, pure-TypeScript PostgreSQL driver: plain SQL strings + params,
// named prepared statements, pluggable result modes, single connection or pool.
// No template tags, no LISTEN/NOTIFY, no COPY.
//
// This is the Node/Bun entry: it registers the node:net/tls transport as the default (so
// connect({ host, port }) works with no socket), then re-exports the runtime-agnostic core. Edge
// runtimes import ./core.ts directly and pass config.socket, so they never pull in node:net/tls.
import { registerDefaultTransport } from './connection.ts'
import { nodeTransport, nodeCancel } from './transport-node.ts'

registerDefaultTransport(nodeTransport, nodeCancel)

export * from './core.ts'
