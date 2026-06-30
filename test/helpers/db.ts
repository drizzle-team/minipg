// Shared test harness: connection/pool factories + small assertion helpers.
// Integration tests expect a running cluster from `bun run test:setup`
// (override with PGTEST_* env vars).
// Driver under test: the interpreted driver (src/) by default, or the codegen/JIT driver
// (src/inline/) when MINIPG_VARIANT=jit. This lets the query/decode suite run unchanged against
// BOTH implementations — `bun run test:query` (interpreted) and `test:query:jit` (codegen).
import * as interpreted from '../../src/index.ts'
import * as jit from '../../src/inline/index.ts'
import type { ConnectConfig, PoolConfig } from '../../src/index.ts'

export const VARIANT = process.env.MINIPG_VARIANT === 'jit' ? 'jit' : 'interpreted'
const impl: typeof interpreted = VARIANT === 'jit' ? (jit as unknown as typeof interpreted) : interpreted
const connect = impl.connect
const createPool = impl.createPool
// PgError is used both as a value (instanceof / throw) and a type (`e: PgError`) by tests, so
// export both: the value tracks the active variant; the type comes from the (identical) class.
export type PgError = interpreted.PgError
export const PgError = impl.PgError

export const TEST_CONFIG: ConnectConfig = {
  host: process.env.PGTEST_HOST ?? '127.0.0.1',
  port: Number(process.env.PGTEST_PORT ?? 54329),
  user: process.env.PGTEST_USER ?? 'postgres',
  password: process.env.PGTEST_PASSWORD ?? 'postgres',
  database: process.env.PGTEST_DB ?? 'testdb',
}

export const testConnect = (o: Partial<ConnectConfig> = {}) => connect({ ...TEST_CONFIG, ...o })
export const testPool = (o: Partial<PoolConfig> = {}) => createPool({ ...TEST_CONFIG, ...o })

/** Run `fn` with a fresh connection that is always ended afterwards. */
export async function withConn<T>(fn: (c: Awaited<ReturnType<typeof connect>>) => Promise<T>, o: Partial<ConnectConfig> = {}): Promise<T> {
  const c = await testConnect(o)
  try { return await fn(c) } finally { await c.end() }
}

/** Capture a rejection; throws if the operation unexpectedly resolves. */
export async function caught(fn: () => Promise<unknown>): Promise<unknown> {
  try { await fn() } catch (e) { return e }
  throw new Error('expected the operation to reject, but it resolved')
}
