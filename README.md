# @drizzle-team/minipg

> [!WARNING]
> Internal Drizzle Team package. Not supported for outside use. Breaking changes can land
> without notice.

A minimal, dependency-free PostgreSQL driver for Node, Bun, Deno, and Cloudflare Workers. It
covers the wire protocol directly and ships as ESM only, with 18 subpath entry points for
each supported runtime and adapter.

## Install

```bash
pnpm add @drizzle-team/minipg
```

```bash
npm install @drizzle-team/minipg
```

```bash
bun add @drizzle-team/minipg
```

## Usage

The example below queries a local cluster. From a checkout of this repo, provision one with:

```bash
bun run test:setup
```

That starts Postgres on `127.0.0.1:54329` with a `testdb` database and a seeded table `t`. It uses
local PostgreSQL binaries when `initdb`/`pg_ctl` are on PATH and a Docker container otherwise (force
either with `MINIPG_PG_MODE=native|docker`; remove the container with `docker rm -f minipg-test-pg`).
Any other Postgres works too — point the config below at it.

```js
import { connect } from '@drizzle-team/minipg'

const db = await connect({
  host: '127.0.0.1',
  port: 54329,
  user: 'postgres',
  password: 'postgres',
  database: 'testdb',
})

const result = await db.query('select id, name, n8, amount from t order by id')
console.log(result.rows)

await db.end()
```

`n8` (bigint) comes back as a native JS `BigInt`, and `amount` (numeric) comes back as a
string, since both types can exceed the range a JS number can represent without losing
precision.

Built-in array columns (`int4[]`, `text[]`, `jsonb[]`, `timestamptz[]`, …) decode to JS
arrays, with each element following the scalar rules above — so `int8[]` yields `BigInt`s
and `numeric[]` yields exact strings. Arrays whose element type has a per-database OID
(`enum[]`, `domain[]`, composite arrays) can't be recognised from the wire OID alone and
still arrive as the raw `{…}` literal; declare a `shape` for those. To opt back out for a
given type, override its array OID in `types` (e.g. `types: { 1007: (b) => b.toString() }`).

## Runtimes

The root import picks its TCP transport from the runtime, so the same line works everywhere:

| Runtime | `@drizzle-team/minipg` resolves to | Transport |
|---|---|---|
| Node, Bun | the Node entry | `node:net` / `node:tls` |
| Deno | the Deno entry | `Deno.connect` / `Deno.startTls` |
| Cloudflare Workers | the Workers entry | `cloudflare:sockets` (needs `nodejs_compat`) |

The choice is made once, by the `exports` conditions in `package.json`, when the module is
resolved or bundled. There is no fallback on purpose: a target with no TCP (a browser bundle, the
Vercel Edge runtime) fails to resolve the root import at build time. Use
`@drizzle-team/minipg/http`, `/neon-http` or `/neon-ws` there.

To force a transport, or when a bundler does not apply runtime conditions, import the explicit
entry: `@drizzle-team/minipg/node`, `/cf` or `/deno`. Each exposes the same API as the root.
`/core` has no transport at all; pass `config.socket`.

## Effect

`@drizzle-team/minipg/effect` implements Effect's `SqlClient` on top of the minipg pool. Effect
core then gives you statement spans, transactions, savepoints and typed `SqlError`s. It needs
`effect` 4.0.x as a peer dependency:

```bash
bun add effect
```

```ts
import { Effect, Redacted } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { MinipgClient } from '@drizzle-team/minipg/effect'

const Database = MinipgClient.layer({ url: Redacted.make('postgres://postgres:postgres@127.0.0.1:54329/testdb') })

const program = Effect.gen(function* () {
  const sql = yield* SqlClient
  return yield* sql<{ id: number; name: string }>`select id, name from t where id = ${1}`
})

console.log(await Effect.runPromise(program.pipe(Effect.provide(Database))))
```

The layer provides both `MinipgClient` and `SqlClient`. It opens the pool when it is built and
ends the pool when its scope closes. `MinipgClient.layerConfig` reads the same settings from
Effect `Config`.

### Moving from @effect/sql-pg

Code written against `SqlClient` moves by swapping one layer:

```diff
- PgClient.layer({ url, maxConnections: 10 })
+ MinipgClient.layer({ url, maxConnections: 10 })
```

The config uses the same names and types: `url` and `password` as `Redacted`, `username`,
`maxConnections`, `Duration` timeouts, `transformResultNames`, `transformQueryNames`,
`transformJson` and `spanAttributes`. `MinipgClient.json` has the signature of
`PgClient.json`.

Some things do not move:

- minipg has no `LISTEN` and `NOTIFY`, so code that uses `PgClient.listen` or `notify` stays on
  `@effect/sql-pg`.
- `interval` decodes to its Postgres text, such as `'1 day 02:00:00'`, and not to an `Interval`
  object.
- `bytea` decodes to a Node `Buffer`, which is a `Uint8Array` subclass.
- For reactive queries, add `Reactivity.layer` to your app layer, as with `@effect/sql-pg`.

### Runtimes

`@drizzle-team/minipg/effect` picks its transport from the runtime with the same `exports`
conditions as the root import, and it has no fallback either:

| Runtime | `@drizzle-team/minipg/effect` resolves to | Transport |
|---|---|---|
| Node, Bun | the Node entry | `node:net` / `node:tls` |
| Deno | the Deno entry | `Deno.connect` / `Deno.startTls` |
| Cloudflare Workers | the Workers entry | `cloudflare:sockets` (needs `nodejs_compat`) |

To force a transport, import `@drizzle-team/minipg/effect/node`, `/effect/cf` or
`/effect/deno`. `/effect/core` has no transport:
pass `socket` in the config, or build a pool with another entry point, for example `createPool`
from `@drizzle-team/minipg/neon-ws`, and pass it to `MinipgClient.layerFromPool`.

`layerFromPool` also lets the async and the Effect entry points share one pool. The pool stays
open when the layer's scope closes:

```ts
import { Effect } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { createPool } from '@drizzle-team/minipg'
import { MinipgClient } from '@drizzle-team/minipg/effect/core'

const pool = createPool('postgres://postgres:postgres@127.0.0.1:54329/testdb')

const rows = await Effect.runPromise(Effect.gen(function* () {
  const sql = yield* SqlClient
  return yield* sql`select count(*)::int as n from t`
}).pipe(Effect.provide(MinipgClient.layerFromPool(pool))))

console.log(rows, (await pool.query('select 1')).rows)
await pool.end()
```

### Transactions, Batch, Pipeline and Parallel

```ts
import { Effect, Redacted } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { MinipgClient } from '@drizzle-team/minipg/effect'

const Database = MinipgClient.layer({ url: Redacted.make('postgres://postgres:postgres@127.0.0.1:54329/testdb') })

const program = Effect.gen(function* () {
  const sql = yield* SqlClient
  const client = yield* MinipgClient.MinipgClient
  const count = sql`select count(*)::int as n from t`

  // One transaction on one connection. The options apply to the outermost transaction only.
  yield* client.withTransaction(count, { isolation: 'repeatable read', readOnly: true })

  // Batch: one transaction, every statement pipelined, all or nothing.
  yield* client.batch([count, count])

  // Pipeline: one connection, no transaction, each statement commits on its own.
  yield* client.pipeline([count, count])

  // Parallel: separate pool connections, so the server runs the statements at the same time.
  yield* Effect.all([count, count], { concurrency: 'unbounded' })
})

await Effect.runPromise(program.pipe(Effect.provide(Database)))
```

Interrupting a statement cancels it on the server. In a Pipeline, only the statement at the head
gets the cancel request. A statement that waits behind it still runs on the server and can
commit, so do not rely on an interrupt to undo writes in a Pipeline. Inside a transaction, the
interrupt rolls the whole transaction back.

A cancel request can arrive at the server after its statement has already ended. The request then
stops the next statement in the Pipeline. This late cancel applies to every Pipeline: an Effect
interrupt, a `timeout` or `signal` option, and concurrent queries on one `Connection`. The stopped
statement fails with SQLSTATE 57014, and the server rolls it back. minipg sets `isLateCancel: true`
on that `PgError`, and you can run the statement again. In the Effect entry point, the error is a
`StatementTimeoutError` whose `cause` has the flag. No PostgreSQL driver can prevent this, because a
cancel request cannot target one statement.

`MinipgClient` also has `query` (minipg's query options, such as `mode`, `shape` and `name`),
`bulkInsert`, `bulkUpdate`, `copyMany`, `copyFrom` (takes a `Stream`), `copyTo` and `cursor`
(return a `Stream`). Each one runs on the transaction's connection inside `withTransaction`.

### Spans

Effect core creates one span per statement with `db.system.name`, `db.namespace`,
`server.address`, `server.port` and `driver: "minipg"`. With `metrics: true`, each statement
span also gets minipg's timings and sizes: queue wait, time to first byte, download, decode,
total, bytes sent and received, and the row count. With the option off, the driver collects no
timings.

```ts
import { Effect, Redacted } from 'effect'
import { SqlClient } from 'effect/sql/SqlClient'
import { MinipgClient } from '@drizzle-team/minipg/effect'

const Database = MinipgClient.layer({
  url: Redacted.make('postgres://postgres:postgres@127.0.0.1:54329/testdb'),
  metrics: true,
})

await Effect.runPromise(Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`select 1`
}).pipe(Effect.provide(Database)))
```

> [!WARNING]
> A pool that you create with the `otel()` or `sentry()` plugins and pass to `layerFromPool`
> produces two spans per query: one from the plugin and one from Effect. Remove one of them.
