// The Effect entry point for Cloudflare Workers. `@drizzle-team/minipg/effect` resolves here under the
// `workerd` export condition. Requires the `nodejs_compat` compatibility flag, like ../cf.ts.
import * as MinipgClient from './cf-client.ts'

export { MinipgClient }
