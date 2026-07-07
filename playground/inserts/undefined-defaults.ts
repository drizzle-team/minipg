// Does bulkInsert of 100 rows honor column DEFAULTs when some rows leave those
// columns `undefined`?  Short answer: NO — and this playground shows why.
//
// bulkInsert compiles ONE prepared statement of the form
//     insert into t (a,b,c) select * from unnest($1::a[], $2::b[], $3::c[])
// so EVERY declared column is sent for EVERY row. A missing object key (or an
// explicit `undefined`) encodes as SQL NULL (src/codec.ts:224, loose `== null`),
// and an explicit NULL *overrides* the column DEFAULT — it does not fall back to
// it the way an omitted column in a plain `VALUES` insert would.
//
//   bun run test:setup                                   # once, starts local cluster
//   bun playground/inserts/undefined-defaults.ts
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }

const mc = await connect({ path: SOCK, ...DB })

await mc.query('drop table if exists bulk_defaults')
await mc.query(`create table bulk_defaults(
  id         int8 primary key,
  name       text        not null,
  status     text        default 'pending',   -- nullable + default
  qty        int4        not null default 0,   -- NOT NULL + default
  created_at timestamptz default now()         -- default now()
)`)

// 100 rows; every 3rd row leaves status / qty / created_at undefined.
type Row = { id: number; name: string; status?: string; qty?: number; created_at?: Date }
const makeRows = (): Row[] =>
  Array.from({ length: 100 }, (_, i) => {
    const sparse = i % 3 === 0
    return sparse
      ? { id: i + 1, name: `row_${i}` }                                   // status/qty/created_at all undefined
      : { id: i + 1, name: `row_${i}`, status: 'active', qty: i, created_at: new Date() }
  })

const line = (s: string) => console.log('\n' + '─'.repeat(70) + '\n' + s + '\n' + '─'.repeat(70))

// ── Scenario 1 ──────────────────────────────────────────────────────────────
// Declare `status` (nullable + default) in the columns map, leave it undefined
// on the sparse rows. Expectation: those rows get NULL, NOT 'pending'.
line("Scenario 1 · 'status' declared but undefined on 1/3 of rows (nullable + default)")
{
  const rows = makeRows().map(({ id, name, status }) => ({ id, name, status })) // only id/name/status
  await mc.query('truncate bulk_defaults')
  await mc.query(`alter table bulk_defaults alter column qty drop not null`) // let qty be omitted safely here
  await mc.bulkInsert(
    'bulk_defaults',
    { id: 'int8', name: 'text', status: 'text' },
    rows,
  )
  const r = await mc.query(
    `select status, count(*)::text n from bulk_defaults group by status order by status nulls last`,
    [], { mode: 'object' },
  )
  console.table(r.rows)
  const nulls = await mc.query(`select count(*)::text n from bulk_defaults where status is null`, [], { mode: 'object' })
  console.log(`→ ${(nulls.rows[0] as { n: string }).n} rows got NULL status (default 'pending' was NOT applied).`)
  await mc.query(`alter table bulk_defaults alter column qty set not null`)
  await mc.query(`alter table bulk_defaults alter column qty set default 0`)
}

// ── Scenario 2 ──────────────────────────────────────────────────────────────
// Same, but the undefined column is NOT NULL + default. The explicit NULL now
// violates the constraint and the WHOLE atomic batch fails.
line("Scenario 2 · 'qty' declared but undefined on 1/3 of rows (NOT NULL + default)")
{
  const rows = makeRows().map(({ id, name, qty }) => ({ id, name, qty }))
  await mc.query('truncate bulk_defaults')
  try {
    await mc.bulkInsert('bulk_defaults', { id: 'int8', name: 'text', qty: 'int4' }, rows)
    console.log('→ (unexpected) insert succeeded')
  } catch (e) {
    console.log(`→ threw: ${(e as Error).message}`)
    console.log('   The undefined qty became NULL, which the NOT NULL constraint rejected.')
  }
}

// ── Scenario 3 ──────────────────────────────────────────────────────────────
// The fix: DON'T declare a column you want defaulted. Omit it from the columns
// map entirely and Postgres applies its DEFAULT for every row.
line("Scenario 3 · omit status/qty/created_at from the columns map → DEFAULTs apply")
{
  const rows = makeRows().map(({ id, name }) => ({ id, name })) // only the columns we truly have
  await mc.query('truncate bulk_defaults')
  await mc.bulkInsert('bulk_defaults', { id: 'int8', name: 'text' }, rows)
  const r = await mc.query(
    `select status, qty, created_at is not null as has_ts, count(*)::text n
       from bulk_defaults group by status, qty, created_at is not null`,
    [], { mode: 'object' },
  )
  console.table(r.rows)
  console.log("→ All 100 rows got status='pending', qty=0, created_at=now() — defaults applied.")
}

await mc.query('drop table if exists bulk_defaults')
await mc.end()
console.log('\ndone.')
