// Runs the CF tests inside REAL workerd (via @cloudflare/vitest-pool-workers / Miniflare v3), so
// cloudflare:sockets, nodejs_compat (Buffer/node:crypto/node:stream), and Duplex.from are exercised
// exactly as they'd behave on Cloudflare — not simulated on Node.
import { fileURLToPath } from 'node:url'
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersConfig({
  // scope to this dir so we don't glob the repo's bun:test suites
  root: fileURLToPath(new URL('.', import.meta.url)),
  test: {
    include: ['**/*.test.ts'],
    poolOptions: {
      workers: {
        // read compatibility_date / compatibility_flags from the sibling wrangler config
        wrangler: { configPath: './wrangler.jsonc' },
      },
    },
  },
})
