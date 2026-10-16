// Effect's SqlClient contract implemented over the minipg pool and connections. This part is the same
// on every runtime; each runtime's client module adds make, layer and layerConfig with its own pool.
import * as Context from 'effect/Context'
import type * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import type * as Redacted from 'effect/Redacted'
import * as Reactivity from 'effect/reactivity/Reactivity'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Client from 'effect/sql/SqlClient'
import type { Connection as SqlConnection } from 'effect/sql/SqlConnection'
import {
  AuthenticationError, AuthorizationError, ConnectionError, ConstraintError, DeadlockError, LockTimeoutError,
  SerializationError, SqlError, SqlSyntaxError, StatementTimeoutError, UniqueViolation, UnknownError,
  type SqlErrorReason,
} from 'effect/sql/SqlError'
import type { Custom, Fragment } from 'effect/sql/Statement'
import * as Statement from 'effect/sql/Statement'
import type { ConnectionOptions } from 'node:tls'
import type { Connection } from '../connection.ts'
import { Cursor, type CursorOptions } from '../cursor.ts'
import { PgError } from '../errors.ts'
import type { QueryMetrics } from '../plugin.ts'
import type { Pool } from '../pool.ts'
import type { RawParams } from '../encode.ts'
import type { ConnectConfig, QueryOptions as MinipgQueryOptions, QueryResult } from '../types.ts'

export const TypeId: TypeId = '~@drizzle-team/minipg/MinipgClient'
export type TypeId = '~@drizzle-team/minipg/MinipgClient'

/** A SqlClient that runs on minipg. Its layers provide it under both this service and SqlClient. */
export interface MinipgClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: MinipgClientConfig
  /** The minipg pool that runs every statement. */
  readonly pool: Pool
  /** Binds a value as jsonb, with the same signature as PgClient.json. */
  readonly json: (value: unknown) => Fragment
  /** SqlClient.withTransaction with transaction settings. A nested call becomes a savepoint and
   *  ignores the settings, because they apply to the whole transaction. */
  readonly withTransaction: <R, E, A>(self: Effect.Effect<A, E, R>, options?: TransactionOptions) => Effect.Effect<A, E | SqlError, R>
  /** Runs one minipg query with minipg's options. The result type follows `mode`, as in the async entry
   *  point. Inside withTransaction it runs on the transaction's connection. */
  readonly query: MinipgQuery
  /** Runs the Effects as a Batch: one transaction, every statement pipelined on its connection, all or nothing. */
  readonly batch: <A, E, R>(effects: Iterable<Effect.Effect<A, E, R>>) => Effect.Effect<Array<A>, E | SqlError, R>
  /** Runs the Effects as a Pipeline: concurrently on one connection, without a transaction, so each
   *  statement commits on its own. An interrupted statement that waits behind another one still runs
   *  on the server. Inside withTransaction it uses the transaction's connection. */
  readonly pipeline: <A, E, R>(effects: Iterable<Effect.Effect<A, E, R>>) => Effect.Effect<Array<A>, E | SqlError, R>
  /** Connection.bulkInsert as an Effect. Inside withTransaction or a Pipeline it uses that connection. */
  readonly bulkInsert: (...args: Parameters<Connection['bulkInsert']>) => Effect.Effect<QueryResult<never>, SqlError>
  /** Connection.bulkUpdate as an Effect. Inside withTransaction or a Pipeline it uses that connection. */
  readonly bulkUpdate: (...args: Parameters<Connection['bulkUpdate']>) => Effect.Effect<QueryResult<never>, SqlError>
  /** Connection.copyMany as an Effect. Inside withTransaction or a Pipeline it uses that connection. */
  readonly copyMany: (...args: Parameters<Connection['copyMany']>) => Effect.Effect<QueryResult<never>, SqlError>
  /** COPY … FROM STDIN with the COPY payload read from a Stream. An interrupt or a failed source sends
   *  CopyFail, so no rows from the COPY remain. */
  readonly copyFrom: <E, R>(sql: string, source: Stream.Stream<Uint8Array | string, E, R>) => Effect.Effect<QueryResult<never>, SqlError | E, R>
  /** COPY … TO STDOUT as a Stream of raw bytes. Closing the Stream early cancels the COPY on the server. */
  readonly copyTo: (sql: string, options?: { readonly highWaterMark?: number }) => Stream.Stream<Uint8Array, SqlError>
  /** A server-side cursor on a dedicated connection, as a Stream of row batches of up to `fetchSize` rows. */
  readonly cursor: <Row = Record<string, unknown>>(options: CursorOptions) => Stream.Stream<Row[], SqlError>
}

/** minipg query options without the ones that Effect replaces: interrupt the Effect to cancel it. */
export type QueryOptions = Omit<MinipgQueryOptions, 'signal' | 'timeout' | 'trace'>
type Params = unknown[] | RawParams

export interface MinipgQuery {
  (sql: string, params: Params, options: QueryOptions & { mode: 'wire' }): Effect.Effect<Uint8Array[], SqlError>
  (sql: string, params: Params, options: QueryOptions & { mode: 'buffer' }): Effect.Effect<QueryResult<(Buffer | null)[]>, SqlError>
  (sql: string, params: Params, options: QueryOptions & { mode: 'raw' }): Effect.Effect<QueryResult<Buffer>, SqlError>
  (sql: string, params: Params, options: (QueryOptions & { mode: 'object' }) | (QueryOptions & { mode?: 'object'; shape: NonNullable<QueryOptions['shape']> })): Effect.Effect<QueryResult<Record<string, unknown>>, SqlError>
  (sql: string, params?: Params, options?: QueryOptions & { mode?: 'array' }): Effect.Effect<QueryResult<unknown[]>, SqlError>
}

export interface TransactionOptions {
  readonly isolation?: 'serializable' | 'repeatable read' | 'read committed' | 'read uncommitted' | undefined
  readonly readOnly?: boolean | undefined
  readonly deferrable?: boolean | undefined
}

const ISOLATION_LEVELS = new Set(['serializable', 'repeatable read', 'read committed', 'read uncommitted'])

const setTransaction = (options: TransactionOptions): string | undefined => {
  const parts: string[] = []
  if (options.isolation !== undefined) {
    if (!ISOLATION_LEVELS.has(options.isolation)) {
      throw new Error(`MinipgClient: the isolation level "${options.isolation}" is unknown. Use serializable, repeatable read, read committed or read uncommitted.`)
    }
    parts.push(`isolation level ${options.isolation}`)
  }
  if (options.readOnly !== undefined) parts.push(options.readOnly ? 'read only' : 'read write')
  if (options.deferrable !== undefined) parts.push(options.deferrable ? 'deferrable' : 'not deferrable')
  return parts.length ? 'set transaction ' + parts.join(' ') : undefined
}

export const MinipgClient = Context.Service<MinipgClient>('@drizzle-team/minipg/MinipgClient')

/** Effect-style names and types, as in @effect/sql-pg, plus the minipg settings that have no Effect equivalent. */
export interface MinipgClientConfig {
  readonly url?: Redacted.Redacted | undefined
  readonly host?: string | undefined
  readonly port?: number | undefined
  /** A unix-domain socket path, used instead of host and port. */
  readonly path?: string | undefined
  readonly ssl?: boolean | ConnectionOptions | undefined
  readonly database?: string | undefined
  readonly username?: string | undefined
  readonly password?: Redacted.Redacted | undefined
  readonly connectTimeout?: Duration.Input | undefined
  readonly idleTimeout?: Duration.Input | undefined
  readonly maxConnections?: number | undefined
  readonly applicationName?: string | undefined
  readonly spanAttributes?: Record<string, unknown> | undefined
  readonly transformResultNames?: ((str: string) => string) | undefined
  readonly transformQueryNames?: ((str: string) => string) | undefined
  readonly transformJson?: boolean | undefined
  /** Adds minipg's per-query timings and sizes to each statement span, and turns on Effect's
   *  SpanPropagationEnabled for the layer. Off by default, and then the driver collects no timings. */
  readonly metrics?: boolean | undefined

  readonly socket?: ConnectConfig['socket']
  readonly decode?: ConnectConfig['decode']
  readonly temporal?: ConnectConfig['temporal']
  readonly prepare?: ConnectConfig['prepare']
  readonly pipeline?: ConnectConfig['pipeline']
  readonly types?: ConnectConfig['types']
}

/** The client settings that apply on top of an existing pool. */
export type FromPoolOptions = Pick<MinipgClientConfig, 'spanAttributes' | 'transformResultNames' | 'transformQueryNames' | 'transformJson' | 'metrics'>

/** Creates a client over a pool that the caller owns: the async and Effect entry points then share its
 *  connections, and the pool stays open when the scope closes. */
export const makeFromPool = (pool: Pool, options: FromPoolOptions = {}): Effect.Effect<MinipgClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  makeWithPool(pool, options)

let clientIdCounter = 0

const makeWithPool = Effect.fnUntraced(function* (pool: Pool, config: MinipgClientConfig) {
  // Connect once, so a wrong config fails when the layer is built and not at the first statement.
  yield* Effect.tryPromise({ try: () => pool.acquire().then((c) => pool.release(c)), catch: (e) => toSqlError(e, 'connect') })
  const target = pool.target
  const compiler = makeCompiler(config.transformQueryNames, config.transformJson)
  const transformRows = config.transformResultNames
    ? Statement.defaultTransforms(config.transformResultNames, config.transformJson).array
    : undefined

  const poolConnection = Effect.uninterruptibleMask((restore) => {
    let acquired: Connection | undefined
    return restore(acquire(pool, (conn) => { acquired = conn })).pipe(
      Effect.onInterrupt(() => Effect.sync(() => { if (acquired) pool.release(acquired) })),
      Effect.tap((conn) => Effect.addFinalizer(() => Effect.sync(() => pool.release(conn)))),
    )
  })
  // Inside `pipeline`, every statement borrows the pipeline's connection instead of the pool.
  const pipelineConnection = Context.Service<Connection>(`@drizzle-team/minipg/MinipgClient/Pipeline/${clientIdCounter++}`)
  const acquirer = Effect.flatMap(Effect.serviceOption(pipelineConnection), (pinned) =>
    Effect.map(Option.isSome(pinned) ? Effect.succeed(pinned.value) : poolConnection, (conn): SqlConnection => new ConnectionImpl(conn, config.metrics === true)))

  const spanAttributes: ReadonlyArray<readonly [string, unknown]> = [
    ...(config.spanAttributes ? Object.entries(config.spanAttributes) : []),
    ['db.system.name', 'postgresql'],
    ['db.namespace', target.database],
    ['server.address', target.host],
    ['server.port', target.port],
    ['driver', 'minipg'],
  ]
  const client = yield* Client.make({
    acquirer,
    compiler,
    prepareTransactionControls: true,
    commit,
    releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
    spanAttributes,
    transformRows,
  })

  const scopedConnection: Effect.Effect<Connection, SqlError, Scope.Scope> =
    Effect.flatMap(Effect.serviceOption(client.transactionService), (tx) =>
      Effect.map(Option.isSome(tx) ? Effect.succeed(tx.value[0]) : acquirer, (sqlConnection) => (sqlConnection as ConnectionImpl).conn))
  const withConnection = <A, E>(f: (conn: Connection) => Effect.Effect<A, E>): Effect.Effect<A, E | SqlError> =>
    Effect.scoped(Effect.flatMap(scopedConnection, f))

  // A span per operation, named and attributed as Effect names the spans of sql statements.
  const spanName = Statement.spanName(spanAttributes)
  const spanOptions = (operation: string, text: string) => ({
    kind: 'client' as const,
    attributes: { ...Object.fromEntries(spanAttributes), 'db.operation.name': operation, 'db.query.text': text },
  })
  const traced = <A, E, R>(operation: string, text: string, effect: Effect.Effect<A, E, R>) =>
    Effect.withSpan(effect, spanName, spanOptions(operation, text))

  const query = ((sql: string, params: Params = [], options: QueryOptions = {}) =>
    traced('query', sql, withConnection((conn) =>
      Effect.tryPromise({
        try: (signal) => conn.query(sql, params as unknown[], { ...options, signal } as never) as Promise<unknown>,
        catch: (e) => toSqlError(e, 'query', conn),
      })))) as MinipgQuery

  const plainWithTransaction = client.withTransaction
  const withTransaction = <R, E, A>(self: Effect.Effect<A, E, R>, options?: TransactionOptions): Effect.Effect<A, E | SqlError, R> => {
    const set = options && setTransaction(options)
    if (!set) return plainWithTransaction(self)
    return Effect.flatMap(Effect.serviceOption(client.transactionService), (outer) =>
      Option.isSome(outer)
        ? plainWithTransaction(self)
        : plainWithTransaction(Effect.andThen(client.unsafe(set), self)))
  }

  const bulk = <O extends object>(
    operation: 'bulkInsert' | 'bulkUpdate' | 'copyMany',
    table: string,
    f: (conn: Connection, options: O & { signal: AbortSignal }) => Promise<QueryResult<never>>,
    options: O,
  ) => traced(operation, table, withConnection((conn) =>
    Effect.tryPromise({ try: (signal) => f(conn, { ...options, signal }), catch: (e) => toSqlError(e, operation, conn) })))
  const bulkInsert = (...[table, columns, rows, options]: Parameters<Connection['bulkInsert']>) =>
    bulk('bulkInsert', table, (conn, withSignal) => conn.bulkInsert(table, columns, rows, withSignal), options ?? {})
  const bulkUpdate = (...[table, columns, rows, options]: Parameters<Connection['bulkUpdate']>) =>
    bulk('bulkUpdate', table, (conn, withSignal) => conn.bulkUpdate(table, columns, rows, withSignal), options)
  const copyMany = (...[table, columns, rows, options]: Parameters<Connection['copyMany']>) =>
    bulk('copyMany', table, (conn, withSignal) => conn.copyMany(table, columns, rows, withSignal), options ?? {})

  const copyFrom = <E, R>(sql: string, source: Stream.Stream<Uint8Array | string, E, R>): Effect.Effect<QueryResult<never>, SqlError | E, R> =>
    traced('copyFrom', sql, Effect.flatMap(Effect.context<R>(), (context) => {
      // minipg turns a failed source into CopyFail and reports the server's error, so the source's own
      // failure is kept here to fail with it instead.
      let sourceFailure: { error: unknown } | undefined
      const inner = Stream.toAsyncIterableWith(Stream.mapError(source, (e) => new SourceFailure(e)), context)
      const payload: AsyncIterable<Uint8Array | string> = {
        [Symbol.asyncIterator]: () => {
          const it = inner[Symbol.asyncIterator]()
          return {
            next: () => it.next().catch((e: unknown) => { sourceFailure = { error: e }; throw e }),
            return: () => it.return!(),
          }
        },
      }
      return withConnection((conn) => Effect.tryPromise({
        try: (signal) => conn.copyFrom(sql, payload, { signal }),
        catch: (e) => sourceFailure ? sourceFailure.error : toSqlError(e, 'copyFrom', conn),
      }).pipe(Effect.catch((e): Effect.Effect<never, SqlError | E> =>
        e instanceof SourceFailure ? Effect.fail(e.error as E) : e instanceof SqlError ? Effect.fail(e) : Effect.die(e))))
    }))

  const copyTo = (sql: string, options: { readonly highWaterMark?: number } = {}): Stream.Stream<Uint8Array, SqlError> =>
    Stream.unwrap(Effect.map(scopedConnection, (conn) => {
      const abort = new AbortController()
      const it = conn.copyTo(sql, { ...options, signal: abort.signal })
      // An early close cancels the COPY: copyTo's own return() would let the server send the rest.
      const bytes: AsyncIterable<Uint8Array> = { [Symbol.asyncIterator]: () => ({ next: () => it.next(), return: () => { abort.abort(); return it.return!() } }) }
      return Stream.fromAsyncIterable(bytes, (e) => toSqlError(e, 'copyTo', conn))
    })).pipe(Stream.withSpan(spanName, spanOptions('copyTo', sql)))

  const cursor = <Row = Record<string, unknown>>(options: CursorOptions): Stream.Stream<Row[], SqlError> =>
    Stream.unwrap(Effect.map(poolConnection, (conn) => {
      const serverCursor = new Cursor<Row>(() => Promise.resolve({ conn, release: () => {} }), options)
      const batches: AsyncIterable<Row[]> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => { const rows = await serverCursor.next(); return rows === null ? { done: true, value: undefined } : { done: false, value: rows } },
          return: async () => { await serverCursor.close(); return { done: true, value: undefined } },
        }),
      }
      return Stream.fromAsyncIterable(batches, (e) => toSqlError(e, 'cursor', conn))
    })).pipe(Stream.withSpan(spanName, spanOptions('cursor', options.sql)))

  const all = <A, E, R>(effects: Iterable<Effect.Effect<A, E, R>>) => Effect.all(effects, { concurrency: 'unbounded' })
  const batch = <A, E, R>(effects: Iterable<Effect.Effect<A, E, R>>) => withTransaction(all(effects))
  const pipeline = <A, E, R>(effects: Iterable<Effect.Effect<A, E, R>>): Effect.Effect<Array<A>, E | SqlError, R> =>
    Effect.flatMap(Effect.serviceOption(client.transactionService), (tx) =>
      Option.isSome(tx)
        ? all(effects)
        : Effect.scoped(Effect.flatMap(poolConnection, (conn) => Effect.provideService(all(effects), pipelineConnection, conn))))

  return Object.assign(client, {
    [TypeId]: TypeId,
    config,
    pool,
    json: (value: unknown) => Statement.fragment([MinipgJson(value)]),
    withTransaction,
    query,
    batch,
    pipeline,
    bulkInsert,
    bulkUpdate,
    copyMany,
    copyFrom,
    copyTo,
    cursor,
  }) as MinipgClient
})

// Waits for a pool connection. If the fiber is interrupted while it waits, the connection that
// arrives later goes straight back to the pool.
const acquire = (pool: Pool, onAcquired: (conn: Connection) => void): Effect.Effect<Connection, SqlError> =>
  Effect.callback<Connection, SqlError>((resume, signal) => {
    pool.acquire().then(
      (conn) => {
        if (signal.aborted) return pool.release(conn)
        onAcquired(conn)
        resume(Effect.succeed(conn))
      },
      (e: unknown) => { if (!signal.aborted) resume(Effect.fail(toSqlError(e, 'acquire'))) },
    )
  })

class ConnectionImpl implements SqlConnection {
  constructor(readonly conn: Connection, readonly metrics = false) {}

  private run<A>(operation: string, f: (signal: AbortSignal) => Promise<A>): Effect.Effect<A, SqlError> {
    return Effect.tryPromise({ try: f, catch: (e) => toSqlError(e, operation, this.conn) })
  }

  private query(operation: string, sql: string, params: ReadonlyArray<unknown>, mode: 'object' | 'array') {
    const result = this.run(operation, (signal) => this.conn.query(sql, params as unknown[], { mode: mode as 'object', signal, metrics: this.metrics }))
    return this.metrics ? Effect.tap(result, (r) => r.metrics ? Effect.annotateCurrentSpan(metricAttributes(r.metrics)) : Effect.void) : result
  }

  private rows(sql: string, params: ReadonlyArray<unknown>, mode: 'object' | 'array') {
    return Effect.map(this.query('execute', sql, params, mode), (r): ReadonlyArray<any> => r.rows)
  }

  execute(sql: string, params: ReadonlyArray<unknown>, transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined) {
    const rows = this.rows(sql, params, 'object')
    return transformRows ? Effect.map(rows, transformRows) : rows
  }
  executeUnprepared(sql: string, params: ReadonlyArray<unknown>, transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined) {
    return this.execute(sql, params, transformRows)
  }
  executeValues(sql: string, params: ReadonlyArray<unknown>) {
    return this.rows(sql, params, 'array') as Effect.Effect<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>
  }
  executeValuesUnprepared(sql: string, params: ReadonlyArray<unknown>) {
    return this.executeValues(sql, params)
  }
  executeRaw(sql: string, params: ReadonlyArray<unknown>): Effect.Effect<QueryResult<Record<string, unknown>>, SqlError> {
    return this.query('executeRaw', sql, params, 'object')
  }
  executeStream(sql: string, params: ReadonlyArray<unknown>, transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined) {
    const rows = Stream.fromAsyncIterable(
      { [Symbol.asyncIterator]: () => this.conn.stream<Record<string, unknown>>(sql, params as unknown[], { mode: 'object' }) },
      (e) => toSqlError(e, 'executeStream', this.conn),
    )
    return transformRows ? Stream.mapArray(rows, (chunk) => transformRows(chunk) as typeof chunk) : rows
  }
}

// The same names as minipg's otel() plugin.
const metricAttributes = (m: QueryMetrics): Record<string, unknown> => ({
  'db.response.returned_rows': m.rowCount ?? 0,
  'db.client.response.body.size': m.bytesReceived,
  'minipg.bytes_sent': m.bytesSent,
  'minipg.queue_wait_ms': m.queueWait,
  'minipg.ttfb_ms': m.ttfb,
  'minipg.download_ms': m.download,
  'minipg.decode_ms': m.decode,
  'minipg.total_ms': m.total,
})

class SourceFailure {
  constructor(readonly error: unknown) {}
}

/** Postgres answers COMMIT with the ROLLBACK tag when the transaction was already aborted. */
const commit = (conn: SqlConnection): Effect.Effect<void, SqlError> =>
  Effect.flatMap(conn.executeRaw('COMMIT', []) as Effect.Effect<QueryResult<never>, SqlError>, (result) =>
    result.command === 'ROLLBACK'
      ? Effect.fail(new SqlError({
        reason: new UnknownError({
          cause: new Error('COMMIT rolled back an aborted transaction'),
          message: 'MinipgClient: COMMIT rolled back the transaction, because a statement in it failed earlier. Find that statement, fix it, and run the transaction again.',
          operation: 'commit',
        }),
      }))
      : Effect.void)

/** Maps a minipg failure to a SqlError. A failure that leaves the connection closed is a ConnectionError. */
export const toSqlError = (cause: unknown, operation: string, conn?: Connection): SqlError => {
  const isConnectionLost = conn !== undefined && conn.state !== 'ready'
  const message = cause instanceof Error ? cause.message : String(cause)
  if (cause instanceof PgError && !isConnectionLost) {
    if (cause.code === '42601' && /multiple commands/.test(message)) {
      return new SqlError({
        reason: new SqlSyntaxError({
          cause,
          message: 'SQL with several statements is not supported: minipg sends each query as one statement. Run the statements one by one.',
          operation,
        }),
      })
    }
    return new SqlError({ reason: classifySqlState(cause.code, cause.constraint, { cause, message, operation }) })
  }
  const props = { cause, message, operation }
  if (isConnectionLost || conn === undefined) return new SqlError({ reason: new ConnectionError(props) })
  return new SqlError({ reason: new UnknownError(props) })
}

// The same SQLSTATE table as @effect/sql-pg.
const classifySqlState = (code: string | undefined, constraint: unknown, props: { cause: unknown; message: string; operation: string }): SqlErrorReason => {
  if (code !== undefined) {
    if (code.startsWith('08')) return new ConnectionError(props)
    if (code.startsWith('28')) return new AuthenticationError(props)
    if (code === '42501') return new AuthorizationError(props)
    if (code.startsWith('42')) return new SqlSyntaxError(props)
    if (code === '23505') {
      const name = typeof constraint === 'string' && constraint.trim() !== '' ? constraint.trim() : 'unknown'
      return new UniqueViolation({ ...props, constraint: name })
    }
    if (code.startsWith('23')) return new ConstraintError(props)
    if (code === '40P01') return new DeadlockError(props)
    if (code === '40001') return new SerializationError(props)
    if (code === '55P03') return new LockTimeoutError(props)
    if (code === '57014') return new StatementTimeoutError(props)
  }
  return new UnknownError(props)
}

export const layerFrom = <E, R>(
  build: Effect.Effect<MinipgClient, E, R>,
): Layer.Layer<MinipgClient | Client.SqlClient, E, Exclude<R, Scope.Scope | Reactivity.Reactivity>> =>
  Layer.effectContext(
    Effect.map(build, (client) => {
      const context = Context.make(MinipgClient, client).pipe(Context.add(Client.SqlClient, client))
      // The driver can reach the statement span only when Effect propagates it.
      return client.config.metrics ? Context.add(context, Statement.SpanPropagationEnabled, true) : context
    }),
  ).pipe(Layer.provide(Reactivity.layer)) as never

/** A layer over a pool that the caller owns. Closing the layer's scope leaves the pool open. */
export const layerFromPool = (pool: Pool, options?: FromPoolOptions): Layer.Layer<MinipgClient | Client.SqlClient, SqlError> =>
  layerFrom(makeFromPool(pool, options))

/** The Postgres statement compiler: $n placeholders, double-quoted identifiers, and json fragments as jsonb. */
export const makeCompiler = (transform?: (_: string) => string, transformJson = true): Statement.Compiler => {
  const transformValue = transformJson && transform ? Statement.defaultTransforms(transform).value : undefined
  return Statement.makeCompiler<MinipgCustom>({
    dialect: 'pg',
    placeholder: (i) => `$${i}`,
    onIdentifier: transform
      ? (value, withoutTransform) => escape(withoutTransform ? value : transform(value))
      : escape,
    onRecordUpdate(placeholders, valueAlias, valueColumns, values, returning) {
      return [
        `(values ${placeholders}) AS ${valueAlias}${valueColumns}${returning ? ` RETURNING ${returning[0]}` : ''}`,
        returning ? values.flat().concat(returning[1]) : values.flat(),
      ]
    },
    onCustom(type, placeholder, withoutTransform) {
      const value = withoutTransform || transformValue === undefined ? type.paramA : transformValue(type.paramA)
      return [`${placeholder(undefined)}::jsonb`, [JSON.stringify(value)]]
    },
  })
}

const escape = Statement.defaultEscape('"')

type MinipgCustom = MinipgJson
interface MinipgJson extends Custom<'MinipgJson', unknown> {}
const MinipgJson = Statement.custom<MinipgJson>('MinipgJson')
