// The Effect entry point without a transport. Pass a pool from another runtime's entry point, or a
// `socket` in the config. The Node and Bun entry point is ./index.ts.
import * as MinipgClient from './MinipgClient.ts'

export { MinipgClient }
