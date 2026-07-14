// minipg for AWS Aurora Serverless — the RDS Data API — `import { connect, createPool, bind } from 'minipg/aurora'`.
//
// The Data API is a stateless HTTPS+JSON service (NOT the Postgres wire protocol): one SigV4-signed POST
// per statement, DB credentials held server-side in Secrets Manager. This driver signs requests with a
// zero-dep WebCrypto SigV4 (src/sigv4.ts; override via config.sign) and reuses minipg's REAL compiled row
// mapper for decode — it converts each typed `Field` back to its PG text and feeds a synthetic wire
// DataRow to `buildMapperFactory`, so results decode identically to the socket driver (int8→BigInt,
// numeric→string, temporal→Date, bytea→Buffer, …). We force resultSetOptions longReturnType/decimalReturnType
// = STRING so int8/numeric arrive as exact text.
//
// Params: the CALLER owns the SQL and its `:name`/`:pN` placeholders — we never rewrite it. Use `bind()`:
//   db.query('insert into t (a,b) values (:p1, :uid)', [ bind.bigint(10), bind.uuid(id, 'uid') ])
// Interactive transactions work (begin/transaction) via a threaded transactionId. No streaming/cursors
// (1 MiB response cap), no COPY/LISTEN, no array bind params (pass as text/JSON) — Data API limitations.
import { buildMapperFactory, type RowMapperFactory } from './mapper.ts'
import { buildDecoders } from './decode.ts'
import { INSTANT_OIDS, type CodegenCol } from './decode.ts'
import { parseDataRow } from './protocol.ts'
import { PgError } from './errors.ts'
import { signRequest, type AwsCredentials } from './sigv4.ts'
import type { Decoder, ResultMode, QueryResult, TxOptions } from './types.ts'

const SERVICE = 'rds-data'
const RESULT_SET_OPTIONS = { longReturnType: 'STRING', decimalReturnType: 'STRING' } as const

// ---- bind(): explicit typed params -> Data API SqlParameter. name present => :name, else positional :pN ----
export type BindType = 'int' | 'bigint' | 'float' | 'numeric' | 'text' | 'bool' | 'uuid' | 'json' | 'date' | 'time' | 'timestamp' | 'bytea'
export interface Bound { readonly __minipgBind: true; type: BindType; value: unknown; name?: string }
const isBound = (v: unknown): v is Bound => !!v && typeof v === 'object' && (v as Bound).__minipgBind === true
const mk = (type: BindType) => (value: unknown, name?: string): Bound => ({ __minipgBind: true, type, value, name })

/** Declare a typed bind parameter for the Data API. `bind(value, type, name?)` or `bind.<type>(value, name?)`;
 *  pass a `name` to target a `:name` placeholder, otherwise it binds positionally as `:p1`, `:p2`, … */
export const bind = Object.assign(
  (value: unknown, type: BindType, name?: string): Bound => ({ __minipgBind: true, type, value, name }),
  { int: mk('int'), bigint: mk('bigint'), float: mk('float'), numeric: mk('numeric'), text: mk('text'), bool: mk('bool'), uuid: mk('uuid'), json: mk('json'), date: mk('date'), time: mk('time'), timestamp: mk('timestamp'), bytea: mk('bytea') },
)

type FieldValue = { longValue: number } | { stringValue: string } | { doubleValue: number } | { booleanValue: boolean } | { blobValue: string } | { isNull: true }
interface SqlParameter { name: string; typeHint?: string; value: FieldValue }

const toB64 = (v: unknown): string => (Buffer.isBuffer(v) ? v : Buffer.from(v as Uint8Array)).toString('base64')
const tsText = (v: unknown): string => (v instanceof Date ? v.toISOString().replace('T', ' ').replace('Z', '') : String(v))
const longOrString = (v: bigint): FieldValue => (Number.isSafeInteger(Number(v)) ? { longValue: Number(v) } : { stringValue: v.toString() })

function encodeBound(b: Bound, name: string): SqlParameter {
  switch (b.type) {
    case 'int': return { name, value: { longValue: Number(b.value) } }
    case 'bigint': return { name, value: longOrString(typeof b.value === 'bigint' ? b.value : BigInt(b.value as string | number)) }
    case 'float': return { name, value: { doubleValue: Number(b.value) } }
    case 'numeric': return { name, typeHint: 'DECIMAL', value: { stringValue: String(b.value) } }
    case 'text': return { name, value: { stringValue: String(b.value) } }
    case 'bool': return { name, value: { booleanValue: !!b.value } }
    case 'uuid': return { name, typeHint: 'UUID', value: { stringValue: String(b.value) } }
    case 'json': return { name, typeHint: 'JSON', value: { stringValue: typeof b.value === 'string' ? b.value : JSON.stringify(b.value) } }
    case 'date': return { name, typeHint: 'DATE', value: { stringValue: String(b.value) } }
    case 'time': return { name, typeHint: 'TIME', value: { stringValue: String(b.value) } }
    case 'timestamp': return { name, typeHint: 'TIMESTAMP', value: { stringValue: tsText(b.value) } }
    case 'bytea': return { name, value: { blobValue: toB64(b.value) } }
  }
}

// raw JS value -> Field (the convenience fallback; bind() is the explicit path)
function encodeRaw(v: unknown, name: string): SqlParameter {
  if (v == null) return { name, value: { isNull: true } }
  if (typeof v === 'boolean') return { name, value: { booleanValue: v } }
  if (typeof v === 'bigint') return { name, value: longOrString(v) }
  if (typeof v === 'number') return { name, value: Number.isInteger(v) ? { longValue: v } : { doubleValue: v } }
  if (Buffer.isBuffer(v)) return { name, value: { blobValue: v.toString('base64') } }
  if (v instanceof Date) return { name, typeHint: 'TIMESTAMP', value: { stringValue: tsText(v) } }
  if (typeof v === 'object') return { name, typeHint: 'JSON', value: { stringValue: JSON.stringify(v) } }
  return { name, value: { stringValue: String(v) } }
}

/** Map a positional params array (raw values and/or `bind()` markers) to Data API SqlParameters. A `bind`
 *  with an explicit name binds `:name`; everything else binds positionally as `:p{index+1}`. */
export function toParameters(params: unknown[]): SqlParameter[] {
  return params.map((p, i) => (isBound(p) ? encodeBound(p, p.name ?? 'p' + (i + 1)) : encodeRaw(p, 'p' + (i + 1))))
}

// ---- typeName (pg_type.typname, exactly what columnMetadata.typeName carries) -> OID -> minipg decoders ----
const TYPENAME_OID: Record<string, number> = {
  bool: 16, bytea: 17, char: 18, name: 19, int8: 20, int2: 21, int4: 23, text: 25, oid: 26, json: 114, xml: 142,
  point: 600, lseg: 601, path: 602, box: 603, polygon: 604, line: 628, cidr: 650, float4: 700, float8: 701,
  circle: 718, macaddr8: 774, money: 790, macaddr: 829, inet: 869, bpchar: 1042, varchar: 1043, date: 1082,
  time: 1083, timestamp: 1114, timestamptz: 1184, interval: 1186, timetz: 1266, bit: 1560, varbit: 1562,
  numeric: 1700, uuid: 2950, jsonb: 3802, int4range: 3904, numrange: 3906, tsrange: 3908, tstzrange: 3910,
  daterange: 3912, int8range: 3926,
}

interface ColumnMetadata { name?: string; label?: string; typeName: string }
interface Field { isNull?: boolean; stringValue?: string; longValue?: number; doubleValue?: number; booleanValue?: boolean; blobValue?: string }

// A Field -> its PG text bytes (so the SAME compiled mapper the wire driver uses can decode it).
function fieldToText(f: Field): Buffer | null {
  if (f.isNull) return null
  if (f.stringValue !== undefined) return Buffer.from(f.stringValue, 'utf8')
  if (f.longValue !== undefined) return Buffer.from(String(f.longValue), 'utf8')
  if (f.doubleValue !== undefined) return Buffer.from(String(f.doubleValue), 'utf8')
  if (f.booleanValue !== undefined) return Buffer.from(f.booleanValue ? 't' : 'f', 'utf8')
  if (f.blobValue !== undefined) return Buffer.from('\\x' + Buffer.from(f.blobValue, 'base64').toString('hex'), 'utf8')
  return null
}

const firstKeyword = (sql: string): string | null => { const m = sql.match(/^[\s(]*([a-zA-Z]+)/); return m ? m[1]!.toUpperCase() : null }

export interface AuroraConfig {
  /** Aurora cluster ARN. Required. */
  resourceArn: string
  /** Secrets Manager secret ARN holding the DB credentials. Required. */
  secretArn: string
  database?: string
  /** AWS region. Defaults to the region parsed from resourceArn. */
  region?: string
  /** AWS credentials. Defaults to AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN env vars. */
  credentials?: AwsCredentials
  /** Override the endpoint URL. Defaults to https://rds-data.<region>.amazonaws.com. */
  endpoint?: string
  /** Bring-your-own signer (e.g. an aws4fetch client or the AWS SDK) — returns the headers to send.
   *  When set, the built-in WebCrypto SigV4 is bypassed. */
  sign?: (req: { method: string; url: string; body: string; headers: Record<string, string> }) => Record<string, string> | Promise<Record<string, string>>
  /** Custom fetch (defaults to the global fetch). */
  fetch?: typeof fetch
  /** How many times to retry a resuming (Aurora v2 wake-up) or throttled call. Default 5. */
  maxRetries?: number
  // decode config (same as the wire driver)
  types?: Record<number, Decoder>
  jsonBigints?: 'number' | 'string' | 'bigint'
  temporal?: 'date' | 'string'
  decode?: 'auto' | 'jit' | 'interpreted'
}

export interface AuroraQueryOptions { mode?: ResultMode; timeout?: number; signal?: AbortSignal }
/** Callback run inside begin()/transaction() with the transaction-scoped client. */
export type AuroraTxFn<T> = (tx: AuroraClient) => T | Promise<T>

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

export class AuroraClient {
  private readonly resourceArn: string
  private readonly secretArn: string
  private readonly database?: string
  private readonly region: string
  private readonly endpoint: string
  private readonly credentials?: AwsCredentials
  private readonly signOverride?: AuroraConfig['sign']
  private readonly fetchImpl: typeof fetch
  private readonly maxRetries: number
  private readonly temporal: 'date' | 'string'
  private readonly decoders: Map<number, Decoder>
  private readonly mapperFactory: RowMapperFactory
  private txId?: string // set on a transaction-scoped clone
  private spCounter = 0

  get state(): 'ready' { return 'ready' }
  get inTransaction(): boolean { return this.txId != null }

  constructor(config: AuroraConfig) {
    if (!config.resourceArn || !config.secretArn) throw new Error('minipg/aurora: resourceArn and secretArn are required')
    this.resourceArn = config.resourceArn
    this.secretArn = config.secretArn
    this.database = config.database
    this.region = config.region ?? config.resourceArn.split(':')[3] ?? 'us-east-1'
    this.endpoint = config.endpoint ?? `https://rds-data.${this.region}.amazonaws.com`
    this.credentials = config.credentials ?? envCredentials()
    this.signOverride = config.sign
    if (!this.credentials && !this.signOverride) throw new Error('minipg/aurora: no AWS credentials — pass { credentials } or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or provide { sign }')
    const f = config.fetch ?? globalThis.fetch
    if (!f) throw new Error('minipg/aurora: no fetch available — pass { fetch }')
    this.fetchImpl = f
    this.maxRetries = config.maxRetries ?? 5
    this.temporal = config.temporal ?? 'date'
    this.decoders = buildDecoders(config.types, config.jsonBigints)
    this.mapperFactory = buildMapperFactory(config.decode)
  }

  // ---- transport: sign + POST one Data API operation, with resume/throttle retry ----
  private async call<T = unknown>(op: string, body: object, signal?: AbortSignal): Promise<T> {
    const url = `${this.endpoint}/${op}`
    const payload = JSON.stringify(body)
    for (let attempt = 0; ; attempt++) {
      const headers = this.signOverride
        ? await this.signOverride({ method: 'POST', url, body: payload, headers: { 'Content-Type': 'application/json' } })
        : await signRequest({ method: 'POST', url, body: payload, service: SERVICE, region: this.region, credentials: this.credentials!, headers: { 'Content-Type': 'application/json' } })
      const res = await this.fetchImpl(url, { method: 'POST', headers, body: payload, signal })
      if (res.ok) return (await res.json()) as T
      const errType = (res.headers.get('x-amzn-errortype') ?? '').split(':')[0] ?? ''
      let parsed: { message?: string } = {}
      try { parsed = (await res.json()) as { message?: string } } catch { /* non-JSON */ }
      if (attempt < this.maxRetries && (errType.includes('DatabaseResuming') || errType.includes('Throttling') || res.status === 429)) {
        await sleep(Math.min(5000, 200 * 2 ** attempt) * (0.5 + Math.random() * 0.5))
        continue
      }
      const message = parsed.message ?? `${res.status} ${res.statusText}`
      // A SQL/database error: real Aurora tags it via x-amzn-errortype (Database/BadRequest/Statement*Exception),
      // but some proxies/emulators leave that header empty and put the PG text in the body. Detect either and
      // map to PgError with the bare PG message (the Data API carries no separate SQLSTATE field).
      const looksPg = /\bERROR:|Database error code|SQLSTATE/i.test(message)
      if (looksPg || errType.includes('Database') || errType.includes('BadRequest') || errType.includes('Statement')) {
        const clean = message.replace(/^Database error code:\s*-?\d+\.\s*Message:\s*/i, '').replace(/^ERROR:\s*/, '')
        throw Object.assign(new PgError({ message: clean }), { awsErrorType: errType })
      }
      throw Object.assign(new Error(`minipg/aurora: ${errType || 'HTTP ' + res.status} — ${message}`), { code: errType })
    }
  }

  private resolveCols(cols: CodegenCol[]): CodegenCol[] {
    if (this.temporal !== 'string') return cols
    return cols.map((c) => (!c.js && !c.json && INSTANT_OIDS.has(c.oid) ? { ...c, js: 'string' as const, format: 'text' as const } : c))
  }

  private decodeResult(r: { columnMetadata?: ColumnMetadata[]; records?: Field[][]; numberOfRecordsUpdated?: number }, sql: string, mode: ResultMode): QueryResult<never> {
    const meta = r.columnMetadata
    if (!meta || !r.records) { // DML with no result set
      return { rows: [], columns: [], rowCount: r.numberOfRecordsUpdated ?? null, command: firstKeyword(sql) }
    }
    const cols = this.resolveCols(meta.map((c) => ({ name: c.label ?? c.name ?? '?', oid: TYPENAME_OID[c.typeName] ?? 25 })))
    const ncols = cols.length
    const mapper = mode === 'array' || mode === 'object' ? this.mapperFactory(cols, mode, this.decoders) : null
    const rows = r.records.map((rec) => {
      const cells = rec.map(fieldToText)
      let size = 2; for (const c of cells) size += 4 + (c ? c.length : 0)
      const b = Buffer.allocUnsafe(size); b.writeInt16BE(ncols, 0); let pos = 2
      for (const c of cells) { if (c == null) { b.writeInt32BE(-1, pos); pos += 4 } else { b.writeInt32BE(c.length, pos); pos += 4; c.copy(b, pos); pos += c.length } }
      if (mapper) return mapper(b)
      if (mode === 'buffer') return parseDataRow(b).map((cell) => (cell == null ? null : Buffer.from(cell)))
      return Buffer.from(b)
    })
    return { rows: rows as never[], columns: cols.map((c) => c.name), rowCount: r.records.length, command: firstKeyword(sql) }
  }

  private base(): { resourceArn: string; secretArn: string; database?: string } {
    return { resourceArn: this.resourceArn, secretArn: this.secretArn, ...(this.database ? { database: this.database } : {}) }
  }

  /** Run one SQL statement via the Data API. `params` may mix raw values and `bind()` markers. */
  query(sql: string, params: unknown[], opts: { mode: 'object'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Record<string, unknown>>>
  query(sql: string, params?: unknown[], opts?: { mode?: 'array'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<unknown[]>>
  query(sql: string, params: unknown[], opts: { mode: 'buffer'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<(Buffer | null)[]>>
  query(sql: string, params: unknown[], opts: { mode: 'raw'; timeout?: number; signal?: AbortSignal }): Promise<QueryResult<Buffer>>
  async query(sql: string, params: unknown[] = [], opts: AuroraQueryOptions = {}): Promise<QueryResult<never>> {
    const body = {
      ...this.base(), sql,
      ...(params.length ? { parameters: toParameters(params) } : {}),
      includeResultMetadata: true, resultSetOptions: RESULT_SET_OPTIONS,
      ...(this.txId ? { transactionId: this.txId } : {}),
    }
    const r = await this.call<{ columnMetadata?: ColumnMetadata[]; records?: Field[][]; numberOfRecordsUpdated?: number }>('Execute', body, opts.signal)
    return this.decodeResult(r, sql, opts.mode ?? 'array')
  }

  /** Bulk-execute ONE statement over many parameter sets (BatchExecuteStatement). For INSERT/UPDATE/DELETE —
   *  it returns no result rows. Each set is a params array (raw values and/or `bind()`), like `query`. */
  async batch(sql: string, paramSets: unknown[][], opts: { signal?: AbortSignal } = {}): Promise<{ updateResults: Array<{ generatedFields: unknown[] }> }> {
    const body = { ...this.base(), sql, parameterSets: paramSets.map(toParameters), ...(this.txId ? { transactionId: this.txId } : {}) }
    return this.call('BatchExecute', body, opts.signal)
  }

  /** Run `fn` inside a Data API transaction (BeginTransaction → threaded transactionId → Commit, or
   *  Rollback + rethrow on error). A nested begin() uses a SAVEPOINT. `transaction()` is an alias. */
  begin<T>(fn: AuroraTxFn<T>): Promise<T>
  begin<T>(options: TxOptions, fn: AuroraTxFn<T>): Promise<T>
  begin<T>(a: TxOptions | AuroraTxFn<T>, b?: AuroraTxFn<T>): Promise<T> { return typeof a === 'function' ? this.runTx(undefined, a) : this.runTx(a, b!) }
  transaction<T>(fn: AuroraTxFn<T>): Promise<T>
  transaction<T>(options: TxOptions, fn: AuroraTxFn<T>): Promise<T>
  transaction<T>(a: TxOptions | AuroraTxFn<T>, b?: AuroraTxFn<T>): Promise<T> { return typeof a === 'function' ? this.runTx(undefined, a) : this.runTx(a, b!) }

  private async runTx<T>(options: TxOptions | undefined, fn: AuroraTxFn<T>): Promise<T> {
    if (this.txId) { // nested -> SAVEPOINT on the same transactionId
      const sp = `minipg_sp_${++this.spCounter}`
      await this.query(`savepoint ${sp}`)
      try { const r = await fn(this); await this.query(`release savepoint ${sp}`); return r }
      catch (e) { try { await this.query(`rollback to savepoint ${sp}`); await this.query(`release savepoint ${sp}`) } catch { /* */ } throw e }
    }
    const { transactionId } = await this.call<{ transactionId: string }>('BeginTransaction', this.base())
    const tx: AuroraClient = Object.create(this)
    ;(tx as unknown as { txId?: string; spCounter: number }).txId = transactionId
    ;(tx as unknown as { txId?: string; spCounter: number }).spCounter = 0
    if (options) await tx.query('set transaction ' + beginOptions(options))
    try { const r = await fn(tx); await this.call('CommitTransaction', { resourceArn: this.resourceArn, secretArn: this.secretArn, transactionId }); return r }
    catch (e) { try { await this.call('RollbackTransaction', { resourceArn: this.resourceArn, secretArn: this.secretArn, transactionId }) } catch { /* */ } throw e }
  }

  /** No-op — the Data API holds no connection. Present for API symmetry. */
  async end(): Promise<void> { /* nothing to close */ }
}

function beginOptions(o: TxOptions): string {
  if (typeof o === 'string') { const s = o.replace(/[^a-zA-Z ]/g, '').trim(); return s }
  const parts: string[] = []
  if (o.isolation) parts.push('isolation level ' + o.isolation)
  if (o.readOnly != null) parts.push(o.readOnly ? 'read only' : 'read write')
  if (o.deferrable != null) parts.push(o.deferrable ? 'deferrable' : 'not deferrable')
  return parts.join(' ')
}

function envCredentials(): AwsCredentials | undefined {
  const p = typeof process !== 'undefined' ? process.env : undefined
  if (p && p.AWS_ACCESS_KEY_ID && p.AWS_SECRET_ACCESS_KEY) return { accessKeyId: p.AWS_ACCESS_KEY_ID, secretAccessKey: p.AWS_SECRET_ACCESS_KEY, sessionToken: p.AWS_SESSION_TOKEN }
  return undefined
}

/** Create an Aurora Data API client. */
export async function connect(config: AuroraConfig): Promise<AuroraClient> { return new AuroraClient(config) }
/** Alias of `connect` — the Data API is connectionless, so there is no pool to manage. */
export function createPool(config: AuroraConfig): AuroraClient { return new AuroraClient(config) }

export { PgError }
export type { AwsCredentials }
