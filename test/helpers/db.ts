// Shared test harness: connection/pool factories + small assertion helpers.
// Integration tests expect a running cluster from `bun run test:setup`
// (override with PGTEST_* env vars).
// ONE merged driver; MINIPG_VARIANT selects the decode strategy (jit default, or interpreted), which
// testConnect/testPool inject as `decode`. So the query/decode suite runs against BOTH mappers of the
// same driver — `bun run test:query` (jit) and MINIPG_VARIANT=interpreted (interpreted catalog).
import * as minipg from '../../src/index.ts'
import type { ConnectConfig, PoolConfig } from '../../src/index.ts'

export const VARIANT: 'jit' | 'interpreted' = process.env.MINIPG_VARIANT === 'interpreted' ? 'interpreted' : 'jit'
const connect = minipg.connect
const createPool = minipg.createPool
export type PgError = minipg.PgError
export const PgError = minipg.PgError

export const TEST_CONFIG: ConnectConfig = {
  host: process.env.PGTEST_HOST ?? '127.0.0.1',
  port: Number(process.env.PGTEST_PORT ?? 54329),
  user: process.env.PGTEST_USER ?? 'postgres',
  password: process.env.PGTEST_PASSWORD ?? 'postgres',
  database: process.env.PGTEST_DB ?? 'testdb',
}

export const testConnect = (o: Partial<ConnectConfig> = {}) => connect({ ...TEST_CONFIG, decode: VARIANT, ...o })
export const testPool = (o: Partial<PoolConfig> = {}) => createPool({ ...TEST_CONFIG, decode: VARIANT, ...o })

/** Run `fn` with a fresh connection that is always ended afterwards. */
export async function withConn<T>(fn: (c: Awaited<ReturnType<typeof connect>>) => Promise<T>, o: Partial<ConnectConfig> = {}): Promise<T> {
  const c = await testConnect(o)
  try { return await fn(c) } finally { await c.end() }
}

/** Capture a rejection; throws if the operation unexpectedly resolves. Accepts any thenable/value
 *  (e.g. a lazy PoolQuery, which `await` forces to run), not just a Promise. */
export async function caught(fn: () => unknown): Promise<unknown> {
  try { await fn() } catch (e) { return e }
  throw new Error('expected the operation to reject, but it resolved')
}
