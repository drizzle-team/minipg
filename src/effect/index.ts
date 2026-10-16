// The Effect entry point for Node, Bun and Deno: registers the node:net/tls transport, like the root entry.
// `@drizzle-team/minipg/effect` picks its file by export condition, as the package root does:
// `workerd` -> ./cf.ts, `node` / `bun` / `deno` -> this file, and no default.
import { registerDefaultTransport } from '../connection.ts'
import { nodeTransport, nodeCancel } from '../transport-node.ts'

registerDefaultTransport(nodeTransport, nodeCancel)

export * from './core.ts'
