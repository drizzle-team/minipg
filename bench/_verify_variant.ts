// Correctness gate for a driver variant: connects the variant (by dir path) over the
// in-process mock and asserts decoded output is identical to baseline across all modes.
//   bun bench/_verify_variant.ts <absolute path to src/variantN>
import assert from 'node:assert/strict'
import { MockPgServer } from '../test/mock/server.ts'

const dir = process.argv[2]
if (!dir) { console.error('usage: bun bench/_verify_variant.ts <variant dir>'); process.exit(1) }
const { connect } = (await import(`${dir}/index.ts`)) as typeof import('../src/index.ts')

const MIXED = {
  fields: [{ name: 'n', oid: 23 }, { name: 'big', oid: 20 }, { name: 's', oid: 25 }, { name: 'ok', oid: 16 }, { name: 'j', oid: 3802 }, { name: 'neg', oid: 23 }],
  rows: [['42', '9007199254740993', 'hi', 't', '{"a":1}', '-7']] as (string | null)[][],
  command: 'SELECT',
}
const mock = await MockPgServer.start({
  onQuery: (sql) => (sql.includes('series')
    ? { fields: [{ name: 'id', oid: 23 }], rows: Array.from({ length: 50 }, (_, i) => [String(i + 1)]), command: 'SELECT' }
    : MIXED),
})

const c = await connect({ host: 'x', port: 0, user: 'u', database: 'd', socket: () => mock.inProcessConnect() })

// array mode — decoded values + sign + bigint-as-string + jsonb parse
const a = (await c.query('select mixed')).rows[0] as unknown[]
assert.deepEqual(a, [42, '9007199254740993', 'hi', true, { a: 1 }, -7])
// object mode
const o = (await c.query('select mixed', [], { mode: 'object' })).rows[0] as Record<string, unknown>
assert.equal(o.n, 42); assert.equal(o.big, '9007199254740993'); assert.equal(o.ok, true)
assert.deepEqual(o.j, { a: 1 }); assert.equal(o.neg, -7)
// buffer mode — raw bytes
const b = (await c.query('select mixed', [], { mode: 'buffer' })).rows[0] as (Buffer | null)[]
assert.ok(Buffer.isBuffer(b[0]) && b[0]!.toString() === '42')
assert.ok(Buffer.isBuffer(b[1]) && b[1]!.toString() === '9007199254740993')
// raw mode — one Buffer per row
assert.ok(Buffer.isBuffer((await c.query('select mixed', [], { mode: 'raw' })).rows[0]))
// param + prepared reuse
assert.equal(((await c.query('select $1::int', [5])).rows[0] as unknown[])[0], 42)
await c.query('select $1::int', [1], { name: 'p' })
assert.equal(((await c.query('select $1::int', [2], { name: 'p' })).rows[0] as unknown[])[0], 42)
// many rows (exercise the decode loop)
const many = await c.query('select generate_series')
assert.equal(many.rows.length, 50)
assert.equal((many.rows[0] as unknown[])[0], 1)
assert.equal((many.rows[49] as unknown[])[0], 50)

await c.end()
await mock.close()
console.log('VARIANT OK')
process.exit(0)
