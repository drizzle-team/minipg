// The Effect entry point for Node and Bun: registers the node:net/tls transport, like the root entry.
import { registerDefaultTransport } from '../connection.ts'
import { nodeTransport, nodeCancel } from '../transport-node.ts'

registerDefaultTransport(nodeTransport, nodeCancel)

export * from './core.ts'
