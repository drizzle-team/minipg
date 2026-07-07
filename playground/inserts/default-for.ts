// "Can I literally do defaultFor(status)?"  Yes — two flavors:
//
//   1) SERVER-RESOLVED, per cell → the SQL `DEFAULT` keyword. Zero lookups: the
//      server substitutes whatever the column's LIVE DDL default is. VALUES-only,
//      but (unlike coalesce) still lets you insert a real NULL in other cells.
//
//   2) CATALOG-FETCHED expression → defaultFor(table, col) reads the column's
//      default expression TEXT from pg_attrdef via pg_get_expr, then splices it
//      into a reusable unnest+coalesce statement. Auto-tracks DDL changes; but it
//      conflates "I want the default" with "I want NULL" for that column.
//
//   bun run test:setup                                   # once, starts local cluster
//   bun playground/inserts/default-for.ts
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }
const mc = await connect({ path: SOCK, ...DB })

const qIdent = (s: string) => '"' + s.replace(/"/g, '""') + '"'
const line = (s: string) => console.log('\n' + '─'.repeat(72) + '\n' + s + '\n' + '─'.repeat(72))
const DEFAULT = Symbol('DEFAULT') // per-cell "use the column default" marker

await mc.query('drop table if exists bulk_defaults')
await mc.query(`create table bulk_defaults(
  id         int8 primary key,
  name       text        not null,
  status     text        default 'pending',
  qty        int4        not null default 0,
  created_at timestamptz default now()
)`)

// ── the "in db" helper: read a column's actual DEFAULT expression from catalog ──
/** Returns the column's DEFAULT expression as SQL text (e.g. `'pending'::text`,
 *  `now()`, `nextval('s'::regclass)`), or null if the column has no default.
 *  This is what "defaultFor(status)" really means: ask the DB what it is. */
async function defaultFor(table: string, col: string): Promise<string | null> {
  const r = await mc.query(
    `select pg_get_expr(d.adbin, d.adrelid) as expr
       from pg_attribute a
       join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
      where a.attrelid = $1::regclass and a.attname = $2 and not a.attisdropped`,
    [table, col], { mode: 'object' },
  )
  return r.rows.length ? (r.rows[0] as { expr: string }).expr : null
}

line('defaultFor(table, col) — reading the live default expression out of the DB')
for (const c of ['status', 'qty', 'created_at', 'name']) {
  console.log(`  defaultFor('bulk_defaults', '${c}') = ${JSON.stringify(await defaultFor('bulk_defaults', c))}`)
}

// ── Flavor 2 · splice the fetched expr into a reusable unnest+coalesce insert ──
// undefined (→ NULL on the wire) MEANS "use the default"; coalesce folds the
// catalog expression in. One stable prepared statement, DDL-accurate defaults.
async function buildCoalesceInsert(table: string, cols: readonly string[], defaultable: readonly string[]) {
  const exprs = new Map<string, string>()
  for (const c of defaultable) {
    const e = await defaultFor(table, c)
    if (e) exprs.set(c, e)
  }
  const proj = cols.map((c) => (exprs.has(c) ? `coalesce(${qIdent(c)}, ${exprs.get(c)})` : qIdent(c)))
  return `insert into ${qIdent(table)} (${cols.map(qIdent).join(',')})\n    select ${proj.join(', ')}\n      from unnest($1::int8[], $2::text[], $3::text[], $4::int4[], $5::timestamptz[]) as u(${cols.map(qIdent).join(',')})`
}

const COLS = ['id', 'name', 'status', 'qty', 'created_at']
const CASTS = ['int8[]', 'text[]', 'text[]', 'int4[]', 'timestamptz[]'] as const
const OLD = new Date('2020-01-01T00:00:00Z')
const makeRows = () =>
  Array.from({ length: 100 }, (_, i) => {
    const wantDefault = i % 3 === 0
    return [i + 1, `row_${i}`,
      wantDefault ? undefined : 'active',
      wantDefault ? undefined : i,
      wantDefault ? undefined : OLD]
  })
const pivot = (rows: unknown[][]) => COLS.map((_, k) => rows.map((r) => r[k]))

line('Flavor 2 · coalesce(col, defaultFor(col)) over unnest — reusable statement')
{
  const sql = await buildCoalesceInsert('bulk_defaults', COLS, ['status', 'qty', 'created_at'])
  console.log('  ' + sql.replace(/\n\s*/g, '\n  ') + '\n')
  await mc.query(sql, pivot(makeRows()), { name: 'ins_dfl', params: CASTS })
  const d = await mc.query(
    `select count(*)::text n from bulk_defaults where status='pending' and qty=0 and created_at >= date '2021-01-01'`,
    [], { mode: 'object' })
  console.log(`→ ${(d.rows[0] as { n: string }).n} rows took the fetched defaults ('pending', 0, now()).`)
}

// Prove it's driven by the DB: change the DDL default, refetch, defaults follow.
line('Flavor 2 · change the DDL default → defaultFor picks it up automatically')
{
  await mc.query('truncate bulk_defaults')
  await mc.query(`alter table bulk_defaults alter column status set default 'archived'`)
  const sql = await buildCoalesceInsert('bulk_defaults', COLS, ['status', 'qty', 'created_at'])
  await mc.query(sql, pivot(makeRows()), { name: 'ins_dfl2', params: CASTS })
  const r = await mc.query(
    `select status, count(*)::text n from bulk_defaults group by status order by n desc`,
    [], { mode: 'object' })
  console.table(r.rows)
  console.log("→ the 34 defaulted rows are now 'archived' — no code change, defaultFor re-read the catalog.")
}

// ── Flavor 1 · the per-cell server-resolved DEFAULT keyword (VALUES) ──────────
// No catalog lookup at all: the token `DEFAULT` tells the server "use this
// column's default here". Per-cell, and other cells can still be a real NULL.
function valuesInsert(table: string, cols: readonly string[], rows: readonly Record<string, unknown>[]) {
  const params: unknown[] = []
  const tuples = rows.map((row) =>
    '(' + cols.map((c) => (row[c] === DEFAULT ? 'DEFAULT' : (params.push(row[c]), '$' + params.length))).join(',') + ')')
  return { sql: `insert into ${qIdent(table)} (${cols.map(qIdent).join(',')}) values ${tuples.join(',')}`, params }
}

line("Flavor 1 · per-cell DEFAULT keyword — status: DEFAULT means 'the DB default'")
{
  await mc.query('truncate bulk_defaults')
  await mc.query(`alter table bulk_defaults alter column status set default 'pending'`) // reset
  const rows = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1, name: `row_${i}`,
    status: i % 3 === 0 ? DEFAULT : 'active', // <- literally "defaultFor(status)" per cell
    qty: i % 3 === 0 ? DEFAULT : i,
    created_at: i % 3 === 0 ? DEFAULT : OLD,
  }))
  const { sql, params } = valuesInsert('bulk_defaults', COLS, rows)
  await mc.query(sql, params)
  const d = await mc.query(
    `select count(*)::text n from bulk_defaults where status='pending' and qty=0 and created_at >= date '2021-01-01'`,
    [], { mode: 'object' })
  console.log(`→ ${(d.rows[0] as { n: string }).n} rows took the server-resolved defaults; no catalog query needed.`)
}

await mc.query('drop table if exists bulk_defaults')
await mc.end()
console.log('\ndone.')
