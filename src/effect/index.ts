// The Effect entry point for Node and Bun: registers the node:net/tls transport, like the root entry.
// `@drizzle-team/minipg/effect` picks its file by export condition, as the package root does:
// `workerd` -> ./cf.ts, `deno` -> ./deno.ts, `node` / `bun` -> this file, and no default.
import { registerDefaultTransport } from '../connection.ts'
import { nodeTransport, nodeCancel } from '../transport-node.ts'

registerDefaultTransport(nodeTransport, nodeCancel)

export * from './core.ts'
