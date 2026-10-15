# Effect support implements `effect/sql` over the minipg pool

Effect 4 ships its own Postgres client, `@effect/sql-pg`. minipg still offers Effect support, so that Effect applications that use the `sql` template, and Drizzle users of `effect-postgres`, can run on minipg. The support implements Effect's `SqlClient` contract (`SqlConnection.Connection`) on top of the existing `Pool` and `Connection`. Spans, transactions, savepoints and typed errors then come from Effect core. minipg does not get its own Effect-only query API, and its protocol layer is not rewritten in Effect.

The entry point follows the pattern of Effect's own drivers. It has its own `MinipgClient` service, which extends `SqlClient` with minipg's extras, and its layers provide both `MinipgClient` and `SqlClient`. The layer config uses Effect's names and types (`username`, `Redacted` passwords, `Duration` timeouts), and `MinipgClient.json` matches `PgClient.json`. Code written against `SqlClient` then moves from `@effect/sql-pg` by swapping one layer. Code written against `PgClient` does not move, because minipg has no `listen` or `notify`.

It ships as two subpaths that mirror the async entry point. `@drizzle-team/minipg/effect` registers the Node transport, for Node and Bun. `@drizzle-team/minipg/effect/core` has no transport: other runtimes pass it a pool from `./cf`, `./deno` or `./neon-ws`, or a `socket` in the config. `effect` is an optional peer dependency with the range `~4.0.5`, because Effect marks its SQL modules as unstable, which allows breaking changes in minor releases. The core package keeps zero runtime dependencies.

Drizzle reaches it through its own module, `drizzle-orm/effect-minipg`, in the same way as its other Effect driver modules. minipg does not imitate the `PgClient` service of `@effect/sql-pg`, so Drizzle's `effect-postgres` module stays tied to the official client.

## Consequences

- Interrupting a query that waits behind another query in an explicit pipeline does not stop it. The server still runs it, and it commits. Only the query at the head of the pipeline gets a cancel request. This is documented on `pipeline`.
- Inside a transaction, an interrupt rolls the whole transaction back, so a query that still ran leaves no writes.
- The pool keeps its own reconnect. All fibers share one recovery after a lost connection, and a user's `Effect.retry` is an outer layer on top of it.
- The layer config has no `plugins` field, because Effect core already creates a span per statement. A pool passed in from outside keeps its own plugins, which gives two spans per query.
