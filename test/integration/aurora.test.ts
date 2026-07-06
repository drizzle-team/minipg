// minipg/aurora — live end-to-end test over the RDS Data API. Runs UNCHANGED against either:
//   - a real Aurora Serverless v2 cluster (Data API enabled), OR
//   - koxudaxi/local-data-api (a Docker emulator over a local Postgres — no AWS account/cost).
//
// Set in the env (or .env, which Bun auto-loads):
//   AURORA_RESOURCE_ARN, AURORA_SECRET_ARN, AURORA_DATABASE   (from `sst deploy`, or the emulator's dummies)
//   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (+ AWS_REGION)  (real for AWS; any value works for the emulator)
//   AURORA_DATA_API_ENDPOINT=http://localhost:8080           (ONLY when using local-data-api)
// Skips entirely when the ARNs are unset.  Run: `bun run test:aurora:live`
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, bind, PgError, type AuroraClient, type AuroraConfig } from '../../src/aurora.ts'

const READY = !!(process.env.AURORA_RESOURCE_ARN && process.env.AURORA_SECRET_ARN)
const d = describe.skipIf(!READY)
const TBL = 'minipg_aurora_test'

function cfg(): AuroraConfig {
  const hasEnvCreds = !!(process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY)
  return {
    resourceArn: process.env.AURORA_RESOURCE_ARN!,
    secretArn: process.env.AURORA_SECRET_ARN!,
    database: process.env.AURORA_DATABASE || undefined,
    endpoint: process.env.AURORA_DATA_API_ENDPOINT || undefined,
    // real AWS reads AWS_* from env; the emulator ignores signing, so supply dummies when none are set
    credentials: hasEnvCreds ? undefined : { accessKeyId: 'localtest', secretAccessKey: 'localtest' },
  }
}

d('minipg/aurora over the RDS Data API', () => {
  let db: AuroraClient
  beforeAll(async () => {
    db = await connect(cfg())
    await db.query(`drop table if exists ${TBL}`)
    await db.query(`create table ${TBL} (id bigint primary key, name text, amount numeric, doc jsonb, blob bytea, ts timestamptz)`)
  }, 30000) // generous: a cold local-data-api JVM can be slow on the first calls
  afterAll(async () => { await db?.query(`drop table if exists ${TBL}`).catch(() => {}); await db?.end() }, 30000)

  test('scalars + bind params', async () => {
    const r = await db.query('select 1 as n, :p1 as t, (2*1.5)::float8 as f, true as b', [bind.text('hi')], { mode: 'object' })
    expect(r.rows[0]).toEqual({ n: 1, t: 'hi', f: 3, b: true })
  })

  test('type decoders via the shared mapper: int8->BigInt, numeric->string, jsonb->object, ts->Date, bytea->Buffer, null', async () => {
    const r = await db.query(
      'select 1234567890123::int8 as big, :p1::numeric as num, :p2::jsonb as j, :p3::timestamptz as ts, :p4::bytea as blob, null::text as nil',
      [bind.numeric('10.50'), bind.json({ a: 1 }), bind.timestamp(new Date('2024-01-15T10:30:45Z')), bind.bytea(Buffer.from('deadbeef', 'hex'))],
      { mode: 'object' },
    )
    const row = r.rows[0] as Record<string, unknown>
    expect(row.big).toBe(1234567890123n) // int8 -> BigInt (>2^53 exactness is covered in the unit test)
    expect(row.num).toBe('10.50')
    expect(row.j).toEqual({ a: 1 })
    expect(row.ts).toBeInstanceOf(Date)
    expect(Buffer.isBuffer(row.blob) && (row.blob as Buffer).toString('hex')).toBe('deadbeef')
    expect(row.nil).toBeNull()
  })

  test('named bind params (:name)', async () => {
    const r = await db.query('select :id::int as id, :label as label', [bind.int(7, 'id'), bind.text('x', 'label')], { mode: 'object' })
    expect(r.rows[0]).toEqual({ id: 7, label: 'x' })
  })

  test('DML rowCount + insert/select round-trip', async () => {
    const ins = await db.query(`insert into ${TBL} (id, name, amount) values (:id, :nm, :amt)`, [bind.bigint(1n, 'id'), bind.text('alice', 'nm'), bind.numeric('99.99', 'amt')])
    expect(ins.rowCount).toBe(1)
    expect(ins.command).toBe('INSERT')
    const sel = await db.query(`select id, name, amount from ${TBL} where id = :id`, [bind.bigint(1n, 'id')], { mode: 'object' })
    expect(sel.rows[0]).toEqual({ id: 1n, name: 'alice', amount: '99.99' })
  })

  test('interactive transaction: commit then rollback (transactionId flow)', async () => {
    await db.transaction(async (tx) => {
      await tx.query(`insert into ${TBL} (id, name) values (:id, :nm)`, [bind.bigint(2n, 'id'), bind.text('bob', 'nm')])
    })
    expect(((await db.query(`select count(*)::int as n from ${TBL}`, [], { mode: 'object' })).rows[0] as { n: number }).n).toBe(2)

    await expect(db.transaction(async (tx) => {
      await tx.query(`insert into ${TBL} (id, name) values (:id, :nm)`, [bind.bigint(3n, 'id'), bind.text('carol', 'nm')])
      throw new Error('boom')
    })).rejects.toThrow('boom')
    expect(((await db.query(`select count(*)::int as n from ${TBL}`, [], { mode: 'object' })).rows[0] as { n: number }).n).toBe(2) // rolled back
  })

  test('errors surface as PgError', async () => {
    let err: unknown
    try { await db.query('select * from a_table_that_does_not_exist') } catch (e) { err = e }
    expect(err).toBeInstanceOf(PgError)
  })

  test('batch insert (BatchExecuteStatement)', async () => {
    await db.batch(`insert into ${TBL} (id, name) values (:id, :nm)`, [
      [bind.bigint(10n, 'id'), bind.text('x', 'nm')],
      [bind.bigint(11n, 'id'), bind.text('y', 'nm')],
    ])
    expect(((await db.query(`select count(*)::int as n from ${TBL} where id >= 10`, [], { mode: 'object' })).rows[0] as { n: number }).n).toBe(2)
  })
})
