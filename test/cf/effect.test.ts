// The Effect entry point inside real workerd: ./effect/core with a pool from the ./cf entry point.
import { it, expect } from 'vitest'
import { Effect } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { createPool } from '../../src/cf.ts'
import { MinipgClient } from '../../src/effect/core.ts'

const CFG = { host: '127.0.0.1', port: 54329, user: 'postgres', password: 'postgres', database: 'testdb' }

it('builds the layer from a ./cf pool and runs a sql select', async () => {
  const pool = createPool(CFG)
  try {
    const rows = await Effect.runPromise(Effect.gen(function* () {
      const sql = yield* SqlClient
      return yield* sql<{ n: number; t: string }>`select 1 as n, ${'hi'}::text as t`
    }).pipe(Effect.provide(MinipgClient.layerFromPool(pool))))
    expect(rows).toEqual([{ n: 1, t: 'hi' }])
  } finally {
    await pool.end()
  }
})
