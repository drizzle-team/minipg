// "Can I put it in the same statement — WITH ... select the defaults ... insert?"
// Yes. Resolve each default ONCE in a CTE, then cross join it into the unnest
// select. One statement, reusable, DDL-tracked, and the default expressions run
// once per statement instead of once per row (the default-of-sql.ts weakness).
//
// BUT: "compute once in a CTE" is only correct for STATEMENT-STABLE defaults
// (constants, now()). For VOLATILE per-row defaults (nextval, gen_random_uuid,
// clock_timestamp) a one-row CTE gives EVERY defaulted row the SAME value — a bug.
// Those you must evaluate per row (omit the column so the table default fires, or
// put the volatile expr in the row-producing select), never in the CTE.
//
//   bun run test:setup                                   # once, starts local cluster
//   bun playground/inserts/default-cte.ts
import { connect } from '../../src/index.ts'

const SOCK = '/tmp/minipg_sock/.s.PGSQL.54329'
const DB = { user: 'postgres', database: 'testdb' }
const mc = await connect({ path: SOCK, ...DB })

const line = (s: string) => console.log('\n' + '─'.repeat(72) + '\n' + s + '\n' + '─'.repeat(72))

await mc.query('drop table if exists bulk_defaults')
await mc.query('drop sequence if exists bd_tok')
await mc.query('create sequence bd_tok')
await mc.query(`create table bulk_defaults(
  id         int8 primary key,
  name       text        not null,
  status     text        default 'pending',
  qty        int4        not null default 0,
  created_at timestamptz default now(),
  tok        int8        default nextval('bd_tok')   -- VOLATILE per-row default
)`)

// same catalog-reading helper as default-of-sql.ts (playgrounds are standalone)
await mc.query(`create or replace function default_of(tbl regclass, col text, witness anyelement)
returns anyelement language plpgsql stable as $fn$
declare expr text; result witness%type;
begin
  select pg_get_expr(d.adbin, d.adrelid) into expr
    from pg_attrdef d join pg_attribute a on a.attrelid=d.adrelid and a.attnum=d.adnum
   where d.adrelid = tbl and a.attname = col and not a.attisdropped;
  if expr is null then return witness; end if;
  execute format('select (%s)::%s', expr, pg_typeof(witness)) into result;
  return result;
end $fn$`)

const OLD = new Date('2020-01-01T00:00:00Z')
const makeRows = () =>
  Array.from({ length: 100 }, (_, i) => {
    const wantDefault = i % 3 === 0
    return [i + 1, `row_${i}`,
      wantDefault ? undefined : 'active',
      wantDefault ? undefined : i,
      wantDefault ? undefined : OLD]
  })
const pivot5 = (rows: unknown[][]) => Array.from({ length: 5 }, (_, k) => rows.map((r) => r[k]))
const CASTS5 = ['int8[]', 'text[]', 'text[]', 'int4[]', 'timestamptz[]'] as const

// ── the CTE pattern: statement-stable defaults resolved ONCE, then cross join ──
// `materialized` guarantees the CTE is evaluated once (PG12+ would otherwise be
// free to inline it and re-evaluate default_of per row). tok is OMITTED here, so
// its volatile table default (nextval) fires per row — the correct place for it.
const CTE_INSERT = `with d as materialized (
    select default_of('bulk_defaults','status',     null::text)        as status,
           default_of('bulk_defaults','qty',        null::int4)        as qty,
           default_of('bulk_defaults','created_at', null::timestamptz) as created_at
  )
  insert into bulk_defaults (id, name, status, qty, created_at)
  select u.id, u.name,
         coalesce(u.status,     d.status),
         coalesce(u.qty,        d.qty),
         coalesce(u.created_at, d.created_at)
    from unnest($1::int8[], $2::text[], $3::text[], $4::int4[], $5::timestamptz[])
           as u(id, name, status, qty, created_at)
    cross join d`

line('WITH d AS (defaults) … INSERT … SELECT coalesce(u.col, d.col) — one statement')
{
  await mc.query(CTE_INSERT, pivot5(makeRows()), { name: 'ins_cte', params: CASTS5 })
  const r = await mc.query(
    `select count(*) filter (where status='pending' and qty=0 and created_at >= date '2021-01-01')::text as defaulted,
            count(distinct tok)::text as distinct_tok,
            count(*)::text as total
       from bulk_defaults`,
    [], { mode: 'object' })
  console.table(r.rows)
  console.log('→ defaults resolved once in the CTE; tok (omitted → table default) is distinct per row.')
}

// ── the volatile caveat: putting nextval in the CTE collides ────────────────
line('CAVEAT · a VOLATILE default computed once in the CTE → every row gets the SAME value')
{
  await mc.query('truncate bulk_defaults')
  // WRONG for volatile: nextval evaluated once in the one-row CTE.
  const rows = makeRows().map((r, i) => [r[0], r[1], i % 3 === 0 ? undefined : (10_000 + i)]) // tok: 34 undefined
  const pivot3 = (rs: unknown[][]) => [rs.map((r) => r[0]), rs.map((r) => r[1]), rs.map((r) => r[2])]
  await mc.query(
    `with d as materialized (select nextval('bd_tok') as tok)
     insert into bulk_defaults (id, name, tok)
     select u.id, u.name, coalesce(u.tok, d.tok)
       from unnest($1::int8[], $2::text[], $3::int8[]) as u(id, name, tok) cross join d`,
    pivot3(rows), { name: 'ins_bad', params: ['int8[]', 'text[]', 'int8[]'] as const })
  const bad = await mc.query(
    `select count(distinct tok)::text as distinct_tok from bulk_defaults where id in (select id from bulk_defaults where (id-1) % 3 = 0)`,
    [], { mode: 'object' })
  console.log(`→ 34 defaulted rows share ${(bad.rows[0] as { n?: string; distinct_tok: string }).distinct_tok} distinct tok value(s) — a collision.`)

  // RIGHT for volatile: nextval in the row-producing select → evaluated per row.
  await mc.query('truncate bulk_defaults')
  await mc.query(
    `insert into bulk_defaults (id, name, tok)
     select u.id, u.name, coalesce(u.tok, nextval('bd_tok'))
       from unnest($1::int8[], $2::text[], $3::int8[]) as u(id, name, tok)`,
    pivot3(rows), { name: 'ins_good', params: ['int8[]', 'text[]', 'int8[]'] as const })
  const good = await mc.query(
    `select count(distinct tok)::text as distinct_tok from bulk_defaults where (id-1) % 3 = 0`,
    [], { mode: 'object' })
  console.log(`→ moving nextval into the SELECT → ${(good.rows[0] as { distinct_tok: string }).distinct_tok} distinct tok values, one per row. Correct.`)
}

await mc.query('drop table if exists bulk_defaults')
await mc.query('drop sequence if exists bd_tok')
await mc.query('drop function if exists default_of(regclass, text, anyelement)')
await mc.end()
console.log('\ndone.')
