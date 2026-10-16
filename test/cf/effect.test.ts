// The Effect entry point inside real workerd.
import { it, expect } from 'vitest'
import { Effect, Redacted } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { createPool } from '../../src/cf.ts'
import { MinipgClient } from '../../src/effect/core.ts'
import { MinipgClient as CfMinipgClient } from '../../src/effect/cf.ts'

const CFG = { host: '127.0.0.1', port: 54329, user: 'postgres', password: 'postgres', database: 'testdb' }
const select = Effect.gen(function* () {
  const sql = yield* SqlClient
  return yield* sql<{ n: number; t: string }>`select 1 as n, ${'hi'}::text as t`
})

it('builds the layer from a ./cf pool and runs a sql select', async () => {
  const pool = createPool(CFG)
  try {
    const rows = await Effect.runPromise(select.pipe(Effect.provide(MinipgClient.layerFromPool(pool))))
    expect(rows).toEqual([{ n: 1, t: 'hi' }])
  } finally {
    await pool.end()
  }
})

// `@drizzle-team/minipg/effect` resolves to ./effect/cf under the `workerd` condition, so its own
// layer must dial through cloudflare:sockets. That entry registers no other transport.
it('the ./effect/cf layer runs a sql select with its own pool', async () => {
  const layer = CfMinipgClient.layer({ host: CFG.host, port: CFG.port, database: CFG.database, username: CFG.user, password: Redacted.make(CFG.password) })
  const rows = await Effect.runPromise(select.pipe(Effect.provide(layer)))
  expect(rows).toEqual([{ n: 1, t: 'hi' }])
})

it('./effect/cf exposes the whole ./effect/core namespace, with its own make and layers', () => {
  const missing = Object.keys(MinipgClient).filter((k) => !(k in CfMinipgClient))
  expect(missing).toEqual([])
  expect(CfMinipgClient.make).not.toBe(MinipgClient.make)
  expect(CfMinipgClient.layer).not.toBe(MinipgClient.layer)
  expect(CfMinipgClient.layerConfig).not.toBe(MinipgClient.layerConfig)
  expect(CfMinipgClient.makeCompiler).toBe(MinipgClient.makeCompiler)
})
