// How do I pass an explicit DEFAULT directive per cell in a bulk insert?
//
// bulkInsert() compiles `insert into t (…) select * from unnest($1::a[], …)`.
// A SELECT projection has NO way to say "use the column DEFAULT" — a NULL is a
// NULL (and overrides the default; see undefined-defaults.ts). SQL's DEFAULT
// keyword is only legal inside a VALUES row list. So there are exactly two ways:
//
//   A. Per-cell DEFAULT → drop to a VALUES insert and emit the literal `DEFAULT`
//      token for the cells you want defaulted, `$n` for the rest. Fully general
//      (works with sequences, now(), any default expr), but the SQL text is
//      unique to that exact arrangement of DEFAULT tokens, so it's not a stable
//      reusable prepared statement.
//
//   B. Known constant/function default → keep ONE stable unnest statement and
//      wrap the column in `coalesce(col, <default-expr>)`. Reusable + fast like
//      bulkInsert, but it can't tell "I want the default" from "I want NULL", so
//      only use it on columns where NULL is not a legitimate value.
//
//   bun run test:setup                                   # once, starts local cluster
//   bun playground/inserts/explicit-default.ts
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }
const mc = await connect({ path: SOCK, ...DB })

const qIdent = (s: string) => '"' + s.replace(/"/g, '""') + '"'
const line = (s: string) => console.log('\n' + '─'.repeat(72) + '\n' + s + '\n' + '─'.repeat(72))

// The sentinel: a cell === DEFAULT emits the literal SQL `DEFAULT` keyword.
const DEFAULT = Symbol('DEFAULT')

/** Multi-row VALUES insert. Any cell === DEFAULT becomes the literal `DEFAULT`
 *  keyword; every other cell becomes a bound $n param. Postgres infers each
 *  param's type from the target column, so no cast/OID list is needed. */
function valuesInsert(
  table: string,
  columns: readonly string[],
  rows: readonly Record<string, unknown>[],
  returning?: string,
): { sql: string; params: unknown[] } {
  const params: unknown[] = []
  const tuples = rows.map((row) => {
    const cells = columns.map((c) => {
      const v = row[c]
      if (v === DEFAULT) return 'DEFAULT'
      params.push(v)
      return '$' + params.length
    })
    return '(' + cells.join(',') + ')'
  })
  const sql = `insert into ${qIdent(table)} (${columns.map(qIdent).join(',')}) values ${tuples.join(',')}`
    + (returning ? ` returning ${returning}` : '')
  return { sql, params }
}

await mc.query('drop table if exists bulk_defaults')
await mc.query(`create table bulk_defaults(
  id         int8 primary key,
  name       text        not null,
  status     text        default 'pending',
  qty        int4        not null default 0,
  created_at timestamptz default now()
)`)

const cols = ['id', 'name', 'status', 'qty', 'created_at']
const OLD = new Date('2020-01-01T00:00:00Z')

// 100 rows; every 3rd row asks for the DEFAULT on status / qty / created_at.
const makeRows = () =>
  Array.from({ length: 100 }, (_, i) => {
    const wantDefault = i % 3 === 0
    return {
      id: i + 1,
      name: `row_${i}`,
      status: wantDefault ? DEFAULT : 'active',
      qty: wantDefault ? DEFAULT : i,
      created_at: wantDefault ? DEFAULT : OLD,
    }
  })

// ── A · per-cell DEFAULT via VALUES ─────────────────────────────────────────
line('A · VALUES insert with the literal DEFAULT keyword (per-cell, fully general)')
{
  const { sql, params } = valuesInsert('bulk_defaults', cols, makeRows())
  console.log('  sample tuple 0 (wants defaults): ' + sql.slice(sql.indexOf('values') + 7, sql.indexOf('),') + 1))
  console.log(`  ${params.length} bound params (defaulted cells consume none)\n`)
  await mc.query(sql, params)

  const r = await mc.query(
    `select status,
            qty,
            created_at >= date '2021-01-01' as defaulted_ts,
            count(*)::text n
       from bulk_defaults
      group by status, qty >= 0 and qty < 3, created_at >= date '2021-01-01', qty
      order by defaulted_ts desc, status`,
    [], { mode: 'object' },
  )
  console.table(r.rows.slice(0, 6))
  const d = await mc.query(
    `select count(*)::text n from bulk_defaults where status = 'pending' and qty = 0 and created_at >= date '2021-01-01'`,
    [], { mode: 'object' },
  )
  console.log(`→ ${(d.rows[0] as { n: string }).n} rows took ALL three defaults (status='pending', qty=0, created_at=now());`)
  console.log("  the other 66 kept their explicit 'active' / i / 2020 values. No NOT NULL violation on qty.")
}

// ── B · known default via coalesce over unnest (reusable single statement) ──
line('B · coalesce(col, default) over unnest — one stable prepared statement, fast')
{
  await mc.query('truncate bulk_defaults')
  // Here `undefined` (→ NULL on the wire) MEANS "use the default", folded in by coalesce.
  // Note: this conflates NULL with default, so only safe where NULL is not a real value.
  const sql = `insert into bulk_defaults (id, name, status, qty, created_at)
    select id, name, coalesce(status, 'pending'), coalesce(qty, 0), coalesce(created_at, now())
      from unnest($1::int8[], $2::text[], $3::text[], $4::int4[], $5::timestamptz[])
        as u(id, name, status, qty, created_at)`
  const rows = Array.from({ length: 100 }, (_, i) => {
    const wantDefault = i % 3 === 0
    return [i + 1, `row_${i}`,
      wantDefault ? undefined : 'active',
      wantDefault ? undefined : i,
      wantDefault ? undefined : OLD]
  })
  // pivot to column arrays (unnest is column-major), exactly like bulkInsert does internally
  const col = (k: number) => rows.map((r) => r[k])
  await mc.query(sql, [col(0), col(1), col(2), col(3), col(4)],
    { name: 'ins_coalesce', params: ['int8[]', 'text[]', 'text[]', 'int4[]', 'timestamptz[]'] })

  const d = await mc.query(
    `select count(*)::text n from bulk_defaults where status = 'pending' and qty = 0 and created_at >= date '2021-01-01'`,
    [], { mode: 'object' },
  )
  console.log(`→ ${(d.rows[0] as { n: string }).n} rows took the coalesced defaults — same result, but the`)
  console.log("  statement 'ins_coalesce' is prepared ONCE and reused across every batch/row-count.")
}

await mc.query('drop table if exists bulk_defaults')
await mc.end()
console.log('\ndone.')
