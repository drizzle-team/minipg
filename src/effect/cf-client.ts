// The MinipgClient namespace for Cloudflare Workers: its pools dial through `cloudflare:sockets`.
import { createPool } from '../cf.ts'
import { makeRuntime } from './runtime.ts'

export * from './MinipgClient.ts'

const runtime = makeRuntime(createPool)
/** Creates a client with its own pool. The pool opens now and ends when the scope closes. */
export const make = runtime.make
/** A layer with its own pool, opened when the layer is built and ended when its scope closes. */
export const layer = runtime.layer
/** Like `layer`, with the settings read from Effect Config. */
export const layerConfig = runtime.layerConfig
