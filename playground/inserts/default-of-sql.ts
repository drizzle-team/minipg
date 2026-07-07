// "I wanna do coalesce(status, pg_attrdef(status))" — resolve the column DEFAULT
// entirely inside SQL, no client-side fetch/splice.
//
// There is NO built-in pg_attrdef(col) scalar. pg_attrdef is a CATALOG TABLE; a
// default is a compiled node tree, and pg_get_expr(adbin, adrelid) only decompiles
// it to TEXT — text is not evaluated in a plain SELECT. So `coalesce(status,
// DEFAULT)` is invalid, and so is `coalesce(status, pg_attrdef(status))`.
//
// The workaround: a small helper function that reads the catalog and EXECUTEs the
// decompiled expression, returning the correctly-typed value (anyelement, inferred
// from a null-typed witness). Then you get the exact syntax you wanted:
//
//     coalesce(status, default_of('bulk_defaults', 'status', null::text))
//
// Trade-off: it's a per-row EXECUTE (a catalog read + dynamic plan each call) — fine
// for correctness and small batches, but for bulk you want the fetch-once-and-splice
// approach in default-for.ts, which inlines the expr so it runs natively.
//
//   bun run test:setup                                   # once, starts local cluster
//   bun playground/inserts/default-of-sql.ts
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }
const mc = await connect({ path: SOCK, ...DB })

const line = (s: string) => console.log('\n' + '─'.repeat(72) + '\n' + s + '\n' + '─'.repeat(72))

await mc.query('drop table if exists bulk_defaults')
await mc.query(`create table bulk_defaults(
  id         int8 primary key,
  name       text        not null,
  status     text        default 'pending',
  qty        int4        not null default 0,
  created_at timestamptz default now()
)`)

// ── the helper: default_of(table, col, witness) → the column's default, evaluated ──
// `witness` is a null of the column type (null::text, null::int4, …) so the polymorphic
// return type is inferred. Returns the witness itself when the column has no default.
await mc.query(`create or replace function default_of(tbl regclass, col text, witness anyelement)
returns anyelement language plpgsql stable as $fn$
declare
  expr text;
  result witness%type;
begin
  select pg_get_expr(d.adbin, d.adrelid) into expr
    from pg_attrdef d
    join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
   where d.adrelid = tbl and a.attname = col and not a.attisdropped;
  if expr is null then
    return witness;
  end if;
  execute format('select (%s)::%s', expr, pg_typeof(witness)) into result;  -- expr is catalog-trusted DDL text
  return result;
end
$fn$`)

line("default_of(...) called directly — 'what is this column's default, evaluated?'")
{
  const r = await mc.query(
    `select default_of('bulk_defaults','status', null::text)     as status,
            default_of('bulk_defaults','qty', null::int4)        as qty,
            default_of('bulk_defaults','created_at', null::timestamptz) is not null as created_at_set,
            default_of('bulk_defaults','name', null::text)       as name_has_no_default`,
    [], { mode: 'object' })
  console.table(r.rows)
}

// ── the payoff: a 100% STATIC insert whose defaults resolve server-side ──
// Note the SQL never mentions 'pending'/0/now() — it asks the DB, per value.
const INSERT_SQL = `insert into bulk_defaults (id, name, status, qty, created_at)
  select id,
         name,
         coalesce(status,     default_of('bulk_defaults','status',     null::text)),
         coalesce(qty,        default_of('bulk_defaults','qty',        null::int4)),
         coalesce(created_at, default_of('bulk_defaults','created_at', null::timestamptz))
    from unnest($1::int8[], $2::text[], $3::text[], $4::int4[], $5::timestamptz[])
      as u(id, name, status, qty, created_at)`

const OLD = new Date('2020-01-01T00:00:00Z')
const makeRows = () =>
  Array.from({ length: 100 }, (_, i) => {
    const wantDefault = i % 3 === 0 // undefined → NULL → coalesce → default_of(...)
    return [i + 1, `row_${i}`,
      wantDefault ? undefined : 'active',
      wantDefault ? undefined : i,
      wantDefault ? undefined : OLD]
  })
const COLS = 5
const pivot = (rows: unknown[][]) => Array.from({ length: COLS }, (_, k) => rows.map((r) => r[k]))
const CASTS = ['int8[]', 'text[]', 'text[]', 'int4[]', 'timestamptz[]'] as const

line('coalesce(col, default_of(col)) over unnest — reusable, no default literals in SQL')
{
  await mc.query('truncate bulk_defaults')
  await mc.query(INSERT_SQL, pivot(makeRows()), { name: 'ins_defof', params: CASTS })
  const d = await mc.query(
    `select count(*)::text n from bulk_defaults where status='pending' and qty=0 and created_at >= date '2021-01-01'`,
    [], { mode: 'object' })
  console.log(`→ ${(d.rows[0] as { n: string }).n} rows resolved all three defaults server-side (no client fetch).`)
}

// The nice part vs. fetch-and-splice: the SQL is byte-for-byte identical after a
// DDL change — the function re-reads the catalog, so the SAME prepared statement
// now produces the new default.
line('Change the DDL default → the SAME static statement yields the new default')
{
  await mc.query('truncate bulk_defaults')
  await mc.query(`alter table bulk_defaults alter column status set default 'archived'`)
  await mc.query(INSERT_SQL, pivot(makeRows()), { name: 'ins_defof' }) // same name, same SQL text
  const r = await mc.query(
    `select status, count(*)::text n from bulk_defaults group by status order by n desc`,
    [], { mode: 'object' })
  console.table(r.rows)
  console.log("→ defaulted rows are now 'archived' — the SQL text never changed; default_of re-read the catalog.")
}

await mc.query('drop table if exists bulk_defaults')
await mc.query('drop function if exists default_of(regclass, text, anyelement)')
await mc.end()
console.log('\ndone.')
