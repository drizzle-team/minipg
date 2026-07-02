// Explicit plugin system for telemetry/observability. Plugins are passed to the driver
// (`connect({ plugins: [otel(), sentry()] })`) and subscribe to lifecycle hooks — NO monkey-patching,
// no implicit global setup. Per query the driver calls onQueryStart (whose return value is threaded
// back as `state` to onQueryEnd/onQueryError, so a plugin can carry a span), then onQueryEnd or
// onQueryError with the collected metrics. All hooks are optional; when no plugin (and no `metrics`
// option) is present, the driver skips timing entirely (zero hot-path cost).

/** Static facts about a query, known when it starts. */
export interface QueryInfo {
  sql: string
  statementName?: string | undefined
  paramCount: number
  database: string
  host?: string | undefined
  backendPid?: number | undefined
  prepared: boolean // true if this reused a cached prepared statement (Parse skipped)
}

/** Per-query measurements (monotonic ms). ttfb ≈ network RTT + server exec; download is first byte →
 *  ReadyForQuery (decode is interleaved in the streaming model, so `decodeMs` is the CPU subset of it). */
export interface QueryMetrics {
  queueWaitMs: number // enqueue → execution start (pool/serialization contention)
  writeMs: number     // encode + serialize + socket.write
  ttfbMs: number      // write → first response byte
  downloadMs: number  // first byte → ReadyForQuery (wall; includes interleaved decode)
  decodeMs: number    // CPU spent mapping DataRow bodies → JS
  totalMs: number     // enqueue → resolve
  bytesSent: number
  bytesReceived: number
  rowCount: number | null
  columnCount: number
  command: string | null
  error?: Error
}

export interface Plugin {
  readonly name: string
  /** Query about to execute. Return any per-query state (e.g. a span) — threaded to end/error. */
  onQueryStart?(info: QueryInfo): unknown
  onQueryEnd?(state: unknown, info: QueryInfo, metrics: QueryMetrics): void
  onQueryError?(state: unknown, info: QueryInfo, error: Error, metrics: QueryMetrics): void
  /** Connection reached ReadyForQuery (initial connect). */
  onConnect?(info: { database: string; host?: string | undefined; backendPid?: number | undefined }): void
  onReconnect?(info: { attempt: number; error: Error }): void
  onDisconnect?(info: { error?: Error | undefined }): void
}
