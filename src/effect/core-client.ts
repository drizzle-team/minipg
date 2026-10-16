// The MinipgClient namespace without a transport of its own. The pool dials with `socket` from the
// config, or with the transport that the Node entry point registers.
import { createPool } from '../core.ts'
import { makeRuntime } from './runtime.ts'

export * from './MinipgClient.ts'

const runtime = makeRuntime(createPool)
/** Creates a client with its own pool. The pool opens now and ends when the scope closes. */
export const make = runtime.make
/** A layer with its own pool, opened when the layer is built and ended when its scope closes. */
export const layer = runtime.layer
/** Like `layer`, with the settings read from Effect Config. */
export const layerConfig = runtime.layerConfig
