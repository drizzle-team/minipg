// The Effect entry point for Deno's own TCP transport (Deno.connect). Without this explicit import, Deno
// resolves `@drizzle-team/minipg/effect` to the Node entry through its Node compatibility layer.
import * as MinipgClient from './deno-client.ts'

export { MinipgClient }
