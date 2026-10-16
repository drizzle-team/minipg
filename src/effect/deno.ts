// The Effect entry point for Deno's own TCP transport (Deno.connect). `@drizzle-team/minipg/effect`
// resolves here under the `deno` export condition.
import * as MinipgClient from './deno-client.ts'

export { MinipgClient }
