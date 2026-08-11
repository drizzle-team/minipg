# @drizzle-team/minipg

> [!WARNING]
> Internal Drizzle Team package. Not supported for outside use. Breaking changes can land
> without notice.

A minimal, dependency-free PostgreSQL driver for Node, Bun, Deno, and Cloudflare Workers. It
covers the wire protocol directly and ships as ESM only, with 13 subpath entry points for
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

The example below queries a local cluster. Provision one first:

```bash
bun run test:setup
```

That starts Postgres on `127.0.0.1:54329` with a `testdb` database and a seeded table `t`.

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
