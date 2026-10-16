// minipg/aurora — OFFLINE unit tests (no live cluster). Covers the whole flow with an injected fetch:
//   - SigV4 signer vs AWS's documented signing vector (src/sigv4.ts)
//   - bind() -> Data API SqlParameters
//   - Field -> JS decode with full wire parity (via the shared compiled mapper)
//   - request shaping, transactions (Begin/Commit/Rollback + transactionId), error -> PgError
// Live end-to-end against a real Aurora Data API cluster is deferred.
import { test, expect, describe } from 'bun:test'
import { signV4 } from '../../src/sigv4.ts'
import { connect, bind, toParameters, PgError, type AuroraConfig } from '../../src/aurora.ts'
import { Shape } from '../../src/shape.ts'

describe('SigV4 signer (src/sigv4.ts)', () => {
  test('matches AWS documented vector: GET iam ListUsers 20150830', async () => {
    const r = await signV4({
      method: 'GET', path: '/', query: 'Action=ListUsers&Version=2010-05-08',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8', Host: 'iam.amazonaws.com', 'X-Amz-Date': '20150830T123600Z' },
      body: '', service: 'iam', region: 'us-east-1', amzDate: '20150830T123600Z',
      credentials: { accessKeyId: 'AKIDEXAMPLE', secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' },
    })
    // canonical hash is AWS-documented; signature cross-verified against Node's reference crypto
    expect(r.canonicalHash).toBe('f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59')
    expect(r.signature).toBe('33f5dad2191de0cb4b7ab912f876876c2c4f72e2991a458f9499233c7b992438')
    expect(r.signedHeaders).toBe('content-type;host;x-amz-date')
  })
})

describe('bind() -> Data API parameters', () => {
  test('positional, named, precision-safe, and raw inference', () => {
    const p = toParameters([
      bind.bigint(10), bind(100, 'text'), bind.uuid('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'uid'),
      bind.json({ k: 1 }), bind.bigint(9223372036854775807n), 42, true, Buffer.from('deadbeef', 'hex'), null,
    ])
    expect(p[0]).toEqual({ name: 'p1', value: { longValue: 10 } })
    expect(p[1]).toEqual({ name: 'p2', value: { stringValue: '100' } })
    expect(p[2]).toEqual({ name: 'uid', typeHint: 'UUID', value: { stringValue: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' } })
    expect(p[3]).toEqual({ name: 'p4', typeHint: 'JSON', value: { stringValue: '{"k":1}' } })
    expect(p[4]).toEqual({ name: 'p5', value: { stringValue: '9223372036854775807' } }) // > 2^53 -> string
    expect(p[5]).toEqual({ name: 'p6', value: { longValue: 42 } })
    expect(p[6]).toEqual({ name: 'p7', value: { booleanValue: true } })
    expect(p[7]!.value).toEqual({ blobValue: Buffer.from('deadbeef', 'hex').toString('base64') })
    expect(p[8]).toEqual({ name: 'p9', value: { isNull: true } })
  })
})

// A mock fetch that records the last request and returns a scripted Data API response.
function mockFetch(script: (op: string, body: any) => { status?: number; headers?: Record<string, string>; json: unknown }) {
  const calls: Array<{ op: string; body: any; headers: Record<string, string> }> = []
  const fetchImpl = (async (url: string, init: RequestInit) => {
    const op = url.split('/').pop()!
    const body = JSON.parse(init.body as string)
    calls.push({ op, body, headers: init.headers as Record<string, string> })
    const s = script(op, body)
    return new Response(JSON.stringify(s.json), { status: s.status ?? 200, headers: s.headers })
  }) as unknown as typeof fetch
  return { fetchImpl, calls }
}

const CFG = (fetchImpl: typeof fetch): AuroraConfig => ({
  resourceArn: 'arn:aws:rds:eu-central-1:123456789012:cluster:my-cluster',
  secretArn: 'arn:aws:secretsmanager:eu-central-1:123456789012:secret:sec-abc',
  database: 'db0', credentials: { accessKeyId: 'AKID', secretAccessKey: 'SECRET' }, fetch: fetchImpl,
})

describe('query: request shaping + Field decode parity', () => {
  test('signs, targets /Execute, sends parameters + resultSetOptions, decodes every type like the wire driver', async () => {
    const { fetchImpl, calls } = mockFetch(() => ({
      json: {
        columnMetadata: [
          { name: 'id', typeName: 'int8' }, { name: 'cnt', typeName: 'int4' }, { name: 'nm', typeName: 'text' },
          { name: 'ok', typeName: 'bool' }, { name: 'score', typeName: 'numeric' }, { name: 'ratio', typeName: 'float8' },
          { name: 'doc', typeName: 'jsonb' }, { name: 'ts', typeName: 'timestamptz' }, { name: 'blob', typeName: 'bytea' }, { name: 'nil', typeName: 'text' },
        ],
        records: [[
          { stringValue: '9223372036854775807' }, { longValue: 42 }, { stringValue: 'Jane' }, { booleanValue: true },
          { stringValue: '10.50' }, { doubleValue: 1.5 }, { stringValue: '{"k": 1}' }, { stringValue: '2024-01-15 10:30:45.123456+00' },
          { blobValue: Buffer.from('deadbeef', 'hex').toString('base64') }, { isNull: true },
        ]],
      },
    }))
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select * from t where id = :p1', [bind.bigint(1)], { mode: 'object' })

    // request shape
    const req = calls[0]!
    expect(req.op).toBe('Execute')
    expect(req.headers.Authorization).toContain('AWS4-HMAC-SHA256')
    expect(req.headers.Authorization).toContain('/rds-data/aws4_request')
    expect(req.body.resourceArn).toContain(':cluster:my-cluster')
    expect(req.body.parameters).toEqual([{ name: 'p1', value: { longValue: 1 } }])
    expect(req.body.resultSetOptions).toEqual({ longReturnType: 'STRING', decimalReturnType: 'STRING' })

    // decode parity (identical to minipg/node)
    const row = r.rows[0] as Record<string, unknown>
    expect(row.id).toBe(9223372036854775807n)
    expect(row.cnt).toBe(42)
    expect(row.nm).toBe('Jane')
    expect(row.ok).toBe(true)
    expect(row.score).toBe('10.50')
    expect(row.ratio).toBe(1.5)
    expect(row.doc).toEqual({ k: 1 })
    expect(row.ts).toBeInstanceOf(Date)
    expect(Buffer.isBuffer(row.blob) && (row.blob as Buffer).toString('hex')).toBe('deadbeef')
    expect(row.nil).toBeNull()
    expect(r.columns).toEqual(['id', 'cnt', 'nm', 'ok', 'score', 'ratio', 'doc', 'ts', 'blob', 'nil'])
    expect(r.command).toBe('SELECT')
  })

  test('DML with no result set -> rows:[], rowCount from numberOfRecordsUpdated, command inferred', async () => {
    const { fetchImpl } = mockFetch(() => ({ json: { numberOfRecordsUpdated: 3, generatedFields: [] } }))
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('update t set x = 1 where y > :p1', [bind.int(0)])
    expect(r.rows).toEqual([])
    expect(r.rowCount).toBe(3)
    expect(r.command).toBe('UPDATE')
  })
})

describe('transactions: transactionId flow', () => {
  test('begin -> Execute(with transactionId) -> Commit', async () => {
    const { fetchImpl, calls } = mockFetch((op) => {
      if (op === 'BeginTransaction') return { json: { transactionId: 'TX-123' } }
      if (op === 'CommitTransaction') return { json: { transactionStatus: 'Transaction Committed' } }
      return { json: { columnMetadata: [{ name: 'v', typeName: 'int4' }], records: [[{ longValue: 7 }]] } }
    })
    const db = await connect(CFG(fetchImpl))
    const out = await db.transaction(async (tx) => (await tx.query('select 7 as v', [], { mode: 'object' })).rows[0])
    expect(out).toEqual({ v: 7 })
    expect(calls.map((c) => c.op)).toEqual(['BeginTransaction', 'Execute', 'CommitTransaction'])
    expect(calls[1]!.body.transactionId).toBe('TX-123')
    expect(calls[2]!.body.transactionId).toBe('TX-123')
  })

  test('error inside the callback triggers Rollback and rethrows', async () => {
    const { fetchImpl, calls } = mockFetch((op) => {
      if (op === 'BeginTransaction') return { json: { transactionId: 'TX-9' } }
      return { json: { transactionStatus: 'Rollback Complete' } }
    })
    const db = await connect(CFG(fetchImpl))
    await expect(db.transaction(async () => { throw new Error('boom') })).rejects.toThrow('boom')
    expect(calls.map((c) => c.op)).toEqual(['BeginTransaction', 'RollbackTransaction'])
  })
})

describe('errors', () => {
  test('Aurora DatabaseErrorException maps to PgError', async () => {
    const { fetchImpl } = mockFetch(() => ({ status: 400, headers: { 'x-amzn-errortype': 'DatabaseErrorException' }, json: { message: 'ERROR: relation "nope" does not exist' } }))
    const db = await connect(CFG(fetchImpl))
    let err: unknown
    try { await db.query('select * from nope') } catch (e) { err = e }
    expect(err).toBeInstanceOf(PgError)
    expect((err as PgError).message).toBe('relation "nope" does not exist')
  })

  test('retries a DatabaseResumingException then succeeds', async () => {
    let n = 0
    const { fetchImpl, calls } = mockFetch(() => {
      if (n++ === 0) return { status: 400, headers: { 'x-amzn-errortype': 'DatabaseResumingException' }, json: { message: 'resuming' } }
      return { json: { columnMetadata: [{ name: 'v', typeName: 'int4' }], records: [[{ longValue: 1 }]] } }
    })
    const db = await connect({ ...CFG(fetchImpl), maxRetries: 3 })
    const r = await db.query('select 1 as v', [], { mode: 'object' })
    expect(r.rows[0]).toEqual({ v: 1 })
    expect(calls.length).toBe(2) // one resume + one success
  })
})

describe('missing config', () => {
  test('throws without resourceArn/secretArn', () => {
    expect(() => createSync({ resourceArn: '', secretArn: '' } as AuroraConfig)).toThrow(/resourceArn and secretArn/)
  })
})
function createSync(c: AuroraConfig) { return new (require('../../src/aurora.ts').AuroraClient)(c) }

describe('transaction-control SQL guard', () => {
  const stub = (async () => { throw new Error('reached-fetch') }) as unknown as typeof fetch

  test('query()/batch() reject textual tx-control; savepoint & rollback-to pass (threaded-tx legit)', async () => {
    const db = await connect(CFG(stub))
    for (const sql of ['begin', 'COMMIT', 'rollback', 'end', 'abort', 'start transaction']) {
      const err = await db.query(sql).then(() => null, (e: unknown) => e as Error)
      expect(err?.message).toMatch(/bypasses the Data API's transaction threading/)
    }
    for (const sql of ['savepoint sp1', 'rollback to savepoint sp1', 'select 1', 'select commitfee from t']) {
      const err = await db.query(sql).then(() => null, (e: unknown) => e as Error)
      expect(err?.message).toBe('reached-fetch') // passed the guard
    }
    const berr = await db.batch('commit', [[]]).then(() => null, (e: unknown) => e as Error)
    expect(berr?.message).toMatch(/bypasses the Data API's transaction threading/)
  })
})

// A one-shot result-set response with the given columns/records.
const rs = (columnMetadata: { name: string; typeName: string }[], records: unknown[][]) =>
  mockFetch(() => ({ json: { columnMetadata, records } }))

describe('aurora: output shapes', () => {
  test('a shape picks the decode targets, overriding the Data API typeName', async () => {
    // Reported as text/int8; the shape asks for int4 and a JS number/string instead.
    const { fetchImpl } = rs(
      [{ name: 'n', typeName: 'text' }, { name: 'big', typeName: 'int8' }, { name: 'when', typeName: 'timestamptz' }],
      [[{ stringValue: '42' }, { stringValue: '9007199254740993' }, { stringValue: '2024-01-15 10:30:45+00' }]],
    )
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select …', [], { shape: { n: 'int4', big: 'int8:string', when: 'timestamptz:string' } })
    expect(r.rows[0]).toEqual({ n: 42, big: '9007199254740993', when: '2024-01-15 10:30:45+00' })
    expect(r.columns).toEqual(['n', 'big', 'when']) // shape keys are the OUTPUT keys
  })

  test("a shape implies mode:'object', and mode:'array' still wins when asked", async () => {
    const cols = [{ name: 'a', typeName: 'int4' }, { name: 'b', typeName: 'text' }]
    const recs = [[{ longValue: 1 }, { stringValue: 'x' }]]
    const obj = await (await connect(CFG(rs(cols, recs).fetchImpl))).query('select …', [], { shape: { a: 'int4', b: 'text' } })
    expect(obj.rows[0]).toEqual({ a: 1, b: 'x' })
    const arr = await (await connect(CFG(rs(cols, recs).fetchImpl))).query('select …', [], { shape: { a: 'int4', b: 'text' }, mode: 'array' })
    expect(arr.rows[0]).toEqual([1, 'x'])
  })

  test("'unknown' defers a column to the reported typeName, like the other transports", async () => {
    const { fetchImpl } = rs(
      [{ name: 'count', typeName: 'int8' }, { name: 'doc', typeName: 'jsonb' }],
      [[{ stringValue: '2' }, { stringValue: '{"k":1}' }]],
    )
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select …', [], { shape: { count: 'unknown', doc: 'unknown' } })
    expect(r.rows[0]).toEqual({ count: 2n, doc: { k: 1 } })
  })

  test('a shape can name a type the Data API metadata cannot (enum reported as its own typname)', async () => {
    const { fetchImpl } = rs([{ name: 'mood', typeName: 'mood_enum' }], [[{ stringValue: 'happy' }]])
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select …', [], { shape: { mood: 'text' } })
    expect(r.rows[0]).toEqual({ mood: 'happy' })
  })

  test('a Shape() mapper object is accepted, and its binary format request is downgraded to text', async () => {
    const { fetchImpl } = rs([{ name: 'id', typeName: 'int4' }, { name: 'nm', typeName: 'text' }], [[{ longValue: 7 }, { stringValue: 'a' }]])
    const db = await connect(CFG(fetchImpl))
    const s = Shape({ id: 'int4', nm: 'text' }) // int4 is in BINARY_FAST -> $cols asks for binary
    expect(s.$cols.some((c) => c.format === 'binary')).toBe(true) // …and the Data API only ever sends text
    const r = await db.query('select …', [], { shape: s })
    expect(r.rows[0]).toEqual({ id: 7, nm: 'a' })
  })

  test('a shape with the wrong column count throws instead of decoding the wrong column', async () => {
    const { fetchImpl } = rs([{ name: 'a', typeName: 'int4' }, { name: 'b', typeName: 'text' }], [[{ longValue: 1 }, { stringValue: 'x' }]])
    const db = await connect(CFG(fetchImpl))
    const err = await db.query('select …', [], { shape: { a: 'int4' } }).catch((e) => e) as Error
    expect(err.message).toContain('shape declares 1 column(s)')
    expect(err.message).toContain('returned 2')
  })
})

describe('aurora: array columns decode like every other entry', () => {
  test("pg_type's '_elem' array typenames are recognised and decoded to JS arrays", async () => {
    const { fetchImpl } = rs(
      [{ name: 'ints', typeName: '_int4' }, { name: 'bigs', typeName: '_int8' }, { name: 'tags', typeName: '_text' }, { name: 'stamps', typeName: '_timestamptz' }],
      [[{ stringValue: '{1,2,3}' }, { stringValue: '{1,9007199254740993}' }, { stringValue: '{a,b}' }, { stringValue: '{"2024-01-15 10:30:45+00"}' }]],
    )
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select …', [], { mode: 'object' }) as { rows: Record<string, unknown>[] }
    expect(r.rows[0]!.ints).toEqual([1, 2, 3])
    expect(r.rows[0]!.bigs).toEqual([1n, 9007199254740993n]) // int8[] elements follow the scalar rule
    expect(r.rows[0]!.tags).toEqual(['a', 'b'])
    expect((r.rows[0]!.stamps as Date[])[0]).toBeInstanceOf(Date)
  })

  test('an array type with no known element still arrives as its raw literal (never a wrong guess)', async () => {
    const { fetchImpl } = rs([{ name: 'moods', typeName: '_mood_enum' }], [[{ stringValue: '{happy,sad}' }]])
    const db = await connect(CFG(fetchImpl))
    const r = await db.query('select …', [], { mode: 'object' }) as { rows: Record<string, unknown>[] }
    expect(r.rows[0]!.moods).toBe('{happy,sad}')
    // …and a shape is the way out
    const typed = await (await connect(CFG(rs([{ name: 'moods', typeName: '_mood_enum' }], [[{ stringValue: '{happy,sad}' }]]).fetchImpl)))
      .query('select …', [], { shape: { moods: 'text[]' } })
    expect(typed.rows[0]).toEqual({ moods: ['happy', 'sad'] })
  })

  test("temporal:'string' reaches temporal[] elements too", async () => {
    const { fetchImpl } = rs([{ name: 'stamps', typeName: '_timestamptz' }], [[{ stringValue: '{"2024-01-15 10:30:45+00"}' }]])
    const db = await connect({ ...CFG(fetchImpl), temporal: 'string' })
    const r = await db.query('select …', [], { mode: 'object' }) as { rows: Record<string, unknown>[] }
    expect(r.rows[0]!.stamps).toEqual(['2024-01-15 10:30:45+00'])
  })
})
