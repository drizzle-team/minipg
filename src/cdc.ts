import { replication, batchTransactions, PublicationMissing, PublicationEmpty, InvalidSlotName, InvalidReplicationShape, type ReplicationConnection, type ReplicationConfig, type ReplicationWarning, type TransactionBatch, type TableShape } from './replication.ts'
import { PgError } from './errors.ts'
import { randomBytes } from 'node:crypto'

// replicate() is a consumer of the raw replication() API: it owns the reconnect loop, slot
// lifecycle, backfill window, and ack-on-handler-resolve.

/** One session's lifecycle stage, tracked on the handle's own closure so two concurrent
 *  replicate() calls never share state. */
type SessionState = 'idle' | 'connecting' | 'preparing' | 'backfilling' | 'streaming' | 'handling' | 'backoff' | 'stopped' | 'dead'

export interface ReplicateOptions {
  /** Connection target, forwarded verbatim to replication() on every (re)connect — a string, or
   *  the same config object replication() accepts. */
  url: string | ReplicationConfig
  /** 'temporary': fresh random-suffixed slot each session, exported-snapshot backfill —
   *  disconnects LOSE queued changes (PG 10-17). { name }: durable slot, health-checked via
   *  command() each connect (needs PG13+, else UnsupportedServerVersionError); malformed names
   *  throw InvalidSlotName synchronously, and a later absence raises SlotInvalidatedError instead
   *  of recreating it. Retains WAL so a disconnected consumer resumes where it left off. */
  slot: 'temporary' | { name: string }
  /** Publications to subscribe — forwarded to start(), whose PublicationMissing probe applies
   *  unchanged. An empty array throws PublicationEmpty synchronously, before any session starts. */
  publications: string[]
  /** Per-table decode shapes, forwarded to start() unchanged. */
  shapes?: TableShape[]
  /** Aborting stops the session the same way stop() does: settle in-flight work, ack what
   *  completed, close, resolve — never fires onFatalError. */
  signal?: AbortSignal
  /** Runs once per session that just created a slot, strictly between CREATE_REPLICATION_SLOT
   *  and START_REPLICATION — the snapshot dies on the connection's next command (SQLSTATE
   *  22023). Adopt it on your OWN connection inside REPEATABLE READ or SERIALIZABLE (else 0A000).
   *  A throw retries IN PLACE on the same slot/snapshot; a backfillTimeoutMs timeout instead goes
   *  straight to onFatalError, no retry. */
  backfill?: (info: { snapshot: string; streamStartLsn: string; isReconnect: boolean; signal: AbortSignal }) => void | Promise<void>
  /** Abandon the session if backfill hasn't resolved within this many ms (omitted = unbounded).
   *  Fires BackfillTimeoutError straight to onFatalError, skipping the retry budget — retrying
   *  would rebuild the slot and let the next session's health check silently resume past an
   *  unread baseline. Must be positive when set; replicate() throws RangeError otherwise. */
  backfillTimeoutMs?: number
  /** Forwarded to batchTransactions() as maxEvents — omitted or non-positive means unbounded,
   *  matching that helper's own default. */
  maxTransactionEvents?: number
  /** Called with each assembled transaction; acked only once this resolves, and only for the
   *  done:true chunk (a done:false chunk carries no commitLsn/endLsn, so acking it is a compile
   *  error). A throw retries the SAME batch in place, bounded by retryDelayMs. */
  onTransaction: (batch: TransactionBatch) => void | Promise<void>
  /** Governs every retry: connection failures, backfill/handler throws, timeouts. Returning null
   *  gives up (fires onFatalError). attempt resets to 0 only on ACKED progress, never on connect
   *  or delivery. Omitted default gives up after ~10 attempts (~5 min): 1000ms base, ×2 per
   *  attempt, 0-1000ms jitter, capped at 30s. */
  retryDelayMs?: (attempt: number, err: Error) => number | null
  /** The managed layer's only diagnostic channel — see CdcWarning. Raw warnings forward through
   *  untouched; a throw from this callback is caught and reported to stderr. */
  onWarning?: (w: CdcWarning) => void
  /** Called exactly once, after the layer has fully stopped. Never called from stop() or from
   *  the consumer's own signal aborting — those resolve cleanly instead. */
  onFatalError?: (err: Error) => void
  /** What to do when a durable slot is already held by another backend. 'error' (default) raises
   *  SlotBusyError. 'evict' terminates the holder with pg_terminate_backend and polls until it
   *  clears; needs the pg_signal_backend grant and fails permanently with 42501 without it. */
  onSlotBusy?: 'error' | 'evict'
  /** Fires instead of backfill when a durable slot already existed and passed its health check —
   *  nothing to backfill. A throw here routes through retryDelayMs like a backfill throw. */
  onResume?: (info: { confirmedFlush: string; restartLsn: string }) => void | Promise<void>
  /** Forwarded to start() unchanged; see StartOptions.messages. */
  messages?: boolean
  /** Forwarded to start() when set. Omitted defaults to max(10_000, wal_sender_timeout * 0.75),
   *  read once per connect and kept above half the server's ping interval so its keepalive clock
   *  actually expires. A value set here always wins over the derived one. */
  statusIntervalMs?: number
  /** Forwarded to start() when set. Omitted defaults to max(60_000, wal_sender_timeout * 2), read
   *  once per connect — a fixed 60s alone false-fires against statusIntervalMs's own cadence.
   *  When wal_sender_timeout is 0 (disabled), this is left off and keepAlive is turned on
   *  instead, with a 'wal-sender-timeout-disabled' warning. A value set here always wins. */
  receiveTimeoutMs?: number
  /** Forwarded to start() unchanged; see StartOptions.maxQueueBytes. */
  maxQueueBytes?: number
  /** Forwarded to start() unchanged; see StartOptions.binary. */
  binary?: boolean | 'auto'
  /** Forwarded to start() unchanged; see StartOptions.hydrateToast. */
  hydrateToast?: boolean
  /** Forwarded to start() unchanged; see StartOptions.idleAck. */
  idleAck?: boolean
}

export interface ReplicateHandle {
  /** Idempotent: the second call returns the first call's promise. Settles in-flight work, acks
   *  what completed, closes the session, and resolves — never fires onFatalError. */
  stop(): Promise<void>
}

/** A durable-slot health check failed on connect — see ReplicateOptions.slot's { name } form.
 *  Fires straight to onFatalError, skipping retries: the slot row went missing, its wal_status
 *  read 'lost', confirmed_flush_lsn was null, or the server's systemId/timeline changed — none
 *  of these heal with time. Drop the slot and call replicate() again, or investigate. */
export class SlotInvalidatedError extends Error {
  readonly reason = 'slot-invalidated' as const
  constructor(readonly slot: string, override readonly cause: 'absent' | 'wal-lost' | 'no-confirmed-flush' | 'system-changed') { super(`minipg: replication slot ${JSON.stringify(slot)} is invalidated (${cause}) — drop it and call replicate() again to start over`) }
}

/** backfillTimeoutMs elapsed before backfill resolved — no partial backfill is usable, so the
 *  session is abandoned straight to onFatalError, skipping the retry budget: retrying would
 *  rebuild the slot and let the next session's health check silently resume past an unread
 *  baseline. */
export class BackfillTimeoutError extends Error {
  readonly reason = 'backfill-timeout' as const
  constructor(readonly ms: number) { super(`minipg: backfill did not resolve within ${ms}ms (backfillTimeoutMs) — the session is abandoned`) }
}

/** A durable slot is already held by another connection and onSlotBusy is 'error' (default), or
 *  eviction was denied. Names the slot and holding PID, never the URL. */
export class SlotBusyError extends Error {
  readonly reason = 'slot-busy' as const
  constructor(readonly slot: string, readonly pid: number) { super(`minipg: replication slot ${JSON.stringify(slot)} is active for PID ${pid} — pass onSlotBusy: 'evict' to terminate it, or wait for the other consumer to release it`) }
}

/** A durable slot's health check reads `pg_replication_slots.wal_status`, added in PostgreSQL
 *  13 — this server lacks it (42703). Never heals with time, so it skips the retry loop rather
 *  than burning the budget on a bare column-does-not-exist error. `slot: 'temporary'` has no
 *  such floor. */
export class UnsupportedServerVersionError extends Error {
  readonly reason = 'unsupported-server-version' as const
  constructor() { super("minipg: durable slots need PostgreSQL 13+ (pg_replication_slots.wal_status is not available on this server) — use slot: 'temporary' instead, or upgrade the server") }
}

/** Lifecycle warnings the managed layer itself emits, on the same onWarning channel raw
 *  ReplicationWarning uses — raw warnings forward through untouched. `reconnect-attempt`: a
 *  session failure or an onTransaction throw consumed a retry attempt, sleeping `delayMs` before
 *  attempt `attempt` (fired for both a full reconnect and an in-place handler retry).
 *  `backfill-timeout`: backfillTimeoutMs elapsed, session abandoned. `slot-evicted`: onSlotBusy:
 *  'evict' terminated another backend. `wal-sender-timeout-disabled`: wal_sender_timeout reads
 *  0, so receiveTimeoutMs is left off and keepAlive is turned on instead. A throw from onWarning
 *  is caught and reported to stderr. */
export type CdcWarning =
  | ReplicationWarning
  | { kind: 'reconnect-attempt'; attempt: number; delayMs: number; message: string }
  | { kind: 'backfill-timeout'; ms: number; message: string }
  | { kind: 'slot-evicted'; slot: string; pid: number; message: string }
  | { kind: 'wal-sender-timeout-disabled'; message: string }

// Mirrors the raw layer's own onWarning contract: a throw from the consumer's callback is caught
// and reported to stderr instead of taking down the session it's reporting on.
const deliverWarning = (cb: ((w: CdcWarning) => void) | undefined, w: CdcWarning): void => {
  if (!cb) return
  try { cb(w) } catch (e) { console.error('minipg: onWarning callback threw', e) }
}

// Always inside [a-z0-9_]{1,63} by construction; createSlot() runs checkSlot() regardless.
const tempSlotName = (): string => `minipg_cdc_${randomBytes(4).toString('hex')}`

// Mirrors src/replication.ts's own SLOT_NAME — a durable name is interpolated into new SQL sites
// (health check, eviction) here, so it's validated before any of them sees it.
const DURABLE_SLOT_NAME = /^[a-z0-9_]{1,63}$/
const validateDurableSlotName = (name: string): void => { if (!DURABLE_SLOT_NAME.test(name)) throw new InvalidSlotName(name) }

// drizzle-pulse's production defaults, not this driver's own backoff elsewhere (multiplicative
// jitter there, additive here — deliberate). attempt is 1-based; null at the ceiling keeps "gave up" a single code path to onFatalError.
function defaultRetryDelayMs(attempt: number, _err: Error): number | null {
  return attempt >= 10 ? null : Math.min(1000 * 2 ** (attempt - 1) + Math.random() * 1000, 30_000)
}

// Resolves early on abort instead of running to completion, so stop() mid-backoff wakes it
// instead of racing a timer against a flag.
const sleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) { resolve(); return }
    const t = setTimeout(done, ms)
    function done(): void { clearTimeout(t); signal.removeEventListener('abort', done); resolve() }
    signal.addEventListener('abort', done, { once: true })
  })

// None of these heal with time (mirrors isFatalAuth, connection.ts). InvalidReplicationShape
// joins because start() validates shapes AFTER createSlot/backfill, so retrying burns a fresh slot and a full backfill before naming the real problem.
const PERMANENT_SQLSTATES = new Set(['28P01', '28000', '3D000', '42501'])
const PERMANENT_ERRORS = [PublicationMissing, PublicationEmpty, InvalidSlotName, InvalidReplicationShape,
  SlotInvalidatedError, SlotBusyError, UnsupportedServerVersionError, BackfillTimeoutError]

function isPermanentFailure(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code
  return (code !== undefined && PERMANENT_SQLSTATES.has(code)) || PERMANENT_ERRORS.some((E) => err instanceof E)
}

interface SlotHealthRow { active: string; active_pid: string | null; wal_status: string; confirmed_flush_lsn: string | null; restart_lsn: string | null }

// One round trip over command() — no second connection. Columns arrive as TEXT ('t'/'f' never
// true/false); slot_type = 'logical' keeps a same-named physical slot from returning null LSNs.
async function readSlotHealth(conn: ReplicationConnection, name: string): Promise<SlotHealthRow | null> {
  let r
  try {
    r = await conn.command(`select active, active_pid, wal_status, confirmed_flush_lsn, restart_lsn from pg_replication_slots where slot_name = '${name}' and slot_type = 'logical'`)
  } catch (e) {
    // wal_status is a PG13+ column — an older server raises 42703 here forever; name the real cause instead of retrying it to the ceiling.
    if (e instanceof PgError && e.code === '42703') throw new UnsupportedServerVersionError()
    throw e
  }
  if (r.rows.length === 0) return null
  return Object.fromEntries(r.columns.map((c, i) => [c, r.rows[0]![i]])) as unknown as SlotHealthRow
}

// Runs on the replication connection itself, never a second one: terminate the holder, then
// poll until the slot reads inactive or a ~3s deadline passes. 't'/'f' both mean "keep polling" ('f' is already-gone; 42704 belongs to pg_drop_replication_slot, not this). 42501 propagates untouched for isPermanentFailure() to classify.
async function evictAndAwaitClear(conn: ReplicationConnection, name: string, pid: number, opts: ReplicateOptions, signal: AbortSignal): Promise<void> {
  if (!Number.isInteger(pid)) throw new SlotBusyError(name, pid) // the trust boundary: a server-sourced value about to ride into SQL
  await conn.command(`select pg_terminate_backend(${pid})`)
  const deadline = Date.now() + 3000
  while (true) {
    const poll = await conn.command(`select active from pg_replication_slots where slot_name = '${name}'`)
    if (poll.rows.length === 0) throw new SlotInvalidatedError(name, 'absent') // dropped mid-poll — never the same as still busy
    if (poll.rows[0]![0] === 'f') {
      deliverWarning(opts.onWarning, { kind: 'slot-evicted', slot: name, pid, message: `minipg: evicted PID ${pid} holding replication slot ${JSON.stringify(name)}` })
      return
    }
    if (Date.now() >= deadline) throw new SlotBusyError(name, pid) // the active flag lags the backend's actual death by a beat — this is what 55006 guards against
    await sleep(100, signal) // the handle's own signal, so stop() interrupts a stuck eviction cleanly
  }
}

// Merges keepAlive: true onto the connect that measured wst = 0 (the one reconnect in run()
// below), carried onto every later session through derivedKeepAlive once it's known.
function withDerivedKeepAlive(url: string | ReplicationConfig, forceKeepAlive: boolean): string | ReplicationConfig {
  if (!forceKeepAlive) return url
  return typeof url === 'string' ? { url, keepAlive: true } : { ...url, keepAlive: true }
}

/** Start a managed CDC session: connect, administer the slot, backfill inside the exported-
 *  snapshot window, then stream with ack tied to onTransaction's resolution. Returns the handle
 *  SYNCHRONOUSLY — a stop() called immediately after replicate() wins before the first connect. */
export function replicate(opts: ReplicateOptions): ReplicateHandle {
  // Consumer misuse (malformed slot name, empty publications) is knowable synchronously — validate
  // before the handle exists, so a consumer who omitted onFatalError still sees it.
  if (typeof opts.slot !== 'string') validateDurableSlotName(opts.slot.name)
  if (opts.publications.length === 0) throw new PublicationEmpty()
  // !(x > 0) catches negative, zero, and NaN in one comparison — a negative value would otherwise fire immediately, aborting every backfill on entry.
  if (opts.backfillTimeoutMs !== undefined && !(opts.backfillTimeoutMs > 0)) {
    throw new RangeError(`minipg: backfillTimeoutMs must be a positive number when set (omit it entirely for unbounded) — got ${opts.backfillTimeoutMs}`)
  }

  let state: SessionState = 'idle'
  let stopping = false
  let fatalFired = false // onFatalError's own guard — S8 must never fire it twice
  let firstBackfill = true
  let attempt = 0 // shared by S6 (reconnect) and the in-place handler retry — one shared budget for both
  let repl: ReplicationConnection | null = null
  let current: Promise<unknown> | null = null // the in-flight backfill or handler promise, awaited by stop()
  let pendingAckLsn: string | null = null // a done:true batch's endLsn once its handler resolves, until acked
  let walSenderTimeoutMs: number | null = null // read once per connect; derives statusIntervalMs/receiveTimeoutMs and the wst=0 keepAlive fallback below
  let stopPromise: Promise<void> | null = null
  let sawSlot = false // absent on the FIRST observation creates the durable slot; absent on any LATER one invalidates it
  let lastSystemId: string | null = null // persisted across connects; a durable slot's systemId/timeline must never move under it
  let lastTimeline: number | null = null
  let derivedKeepAlive = false // true once a connect measures wal_sender_timeout = 0 and the consumer didn't set their own keepAlive
  let currentSlotName: string | null = null // durable-session bookkeeping for the catch block's 42704 -> SlotInvalidatedError mapping below
  let currentIsDurable = false
  const consumerSetKeepAlive = typeof opts.url !== 'string' && opts.url.keepAlive !== undefined
  const consumerKeepAliveValue = typeof opts.url !== 'string' ? opts.url.keepAlive : undefined
  const controller = new AbortController() // internal signal: reaches start()'s own signal, so stop() wakes a parked next()
  const effectiveRetryDelayMs = opts.retryDelayMs ?? defaultRetryDelayMs
  // The consumer's own signal behaves exactly like calling stop() — same teardown, same
  // idempotency, never onFatalError. Removed in doStop() to avoid leaking this listener.
  const onConsumerAbort = (): void => { stop() }
  opts.signal?.addEventListener('abort', onConsumerAbort, { once: true })
  if (opts.signal?.aborted) stop() // already aborted before replicate() was even called

  // S8: tear down and fire onFatalError exactly once. Every give-up path (permanent error,
  // retryDelayMs returning null) funnels through here so there is one way to reach onFatalError.
  function fireFatal(err: Error): void {
    if (fatalFired) return
    fatalFired = true
    repl?.end()
    state = 'dead'
    // Also removed here — doStop() is the only other place this runs, so a handle that dies fatally never leaks the listener on a long-lived shared signal.
    opts.signal?.removeEventListener('abort', onConsumerAbort)
    if (!opts.onFatalError) { console.error('minipg: replicate() session ended fatally with no onFatalError callback wired up —', err); return }
    // Nothing downstream catches a throw here, so one is caught and reported, like deliverWarning does for onWarning.
    try { opts.onFatalError(err) } catch (e) { console.error('minipg: onFatalError callback threw', e) }
  }

  async function run(): Promise<void> {
    if (stopping) { state = 'stopped'; return }
    while (true) { // one iteration = one session: S1 connecting through S4/S5, or a failure into S6
      currentIsDurable = false // reset every iteration — stale from a prior session must never drive the 42704 mapping below
      currentSlotName = null
      try {
        state = 'connecting'
        repl = await replication(withDerivedKeepAlive(opts.url, derivedKeepAlive))
        if (stopping) { repl.end(); state = 'stopped'; return }

        state = 'preparing'
        // command() during an active stream throws ReplicationBusy, so every catalog query and all slot administration must precede start().
        const wst = await repl.command("select setting::int from pg_settings where name = 'wal_sender_timeout'")
        const raw = wst.rows[0]?.[0]
        walSenderTimeoutMs = raw == null ? null : Number(raw)
        if (stopping) { repl.end(); state = 'stopped'; return }

        // wst = 0 means this connection needs keepAlive, but the setting is only readable after
        // the socket exists — reconnect once now (nothing administered yet to unwind); derivedKeepAlive flips below so this can't fire twice.
        if (walSenderTimeoutMs === 0 && !consumerSetKeepAlive && !derivedKeepAlive) {
          repl.end()
          state = 'connecting'
          repl = await replication(withDerivedKeepAlive(opts.url, true))
          if (stopping) { repl.end(); state = 'stopped'; return }
          state = 'preparing'
        }

        // Both timers derive from the server's own wal_sender_timeout rather than fixed constants:
        // a fixed receiveTimeoutMs false-fires because the server only pings once the client's been silent for half that interval, and a fixed statusIntervalMs keeps resetting that clock.
        const derivedStatusIntervalMs = walSenderTimeoutMs == null ? undefined : Math.max(10_000, walSenderTimeoutMs * 0.75)
        const derivedReceiveTimeoutMs = walSenderTimeoutMs == null || walSenderTimeoutMs === 0 ? undefined : Math.max(60_000, walSenderTimeoutMs * 2)
        derivedKeepAlive = walSenderTimeoutMs === 0 && !consumerSetKeepAlive
        if (walSenderTimeoutMs === 0) {
          // Only true when the layer itself turns keepAlive on (derivedKeepAlive above) — a consumer who pinned keepAlive: false gets nothing enabled, and the warning says that plainly.
          const message = !consumerSetKeepAlive
            ? "minipg: the server's wal_sender_timeout is 0 (disabled) — it will never ping this connection, so receiveTimeoutMs is left off and keepAlive is enabled instead"
            : consumerKeepAliveValue
              ? "minipg: the server's wal_sender_timeout is 0 (disabled) — it will never ping this connection, so receiveTimeoutMs is left off; keepAlive is already enabled on this connection"
              : "minipg: the server's wal_sender_timeout is 0 (disabled) — it will never ping this connection, receiveTimeoutMs is left off, and keepAlive is explicitly disabled on this connection: there is no liveness detection at all"
          deliverWarning(opts.onWarning, { kind: 'wal-sender-timeout-disabled', message })
        }

        const isDurable = typeof opts.slot !== 'string'
        const name = typeof opts.slot === 'string' ? tempSlotName() : opts.slot.name
        if (isDurable) validateDurableSlotName(name) // a consumer bug, not slot invalidation — checked before ANY interpolation site below sees it
        currentIsDurable = isDurable
        currentSlotName = name

        let resumed: { confirmedFlush: string; restartLsn: string } | null = null

        if (isDurable) {
          // Fixed order on every durable connect: the wst probe above, then identify(), then the health check.
          const identity = await repl.identify()
          if (lastSystemId !== null && (identity.systemId !== lastSystemId || identity.timeline !== lastTimeline)) {
            throw new SlotInvalidatedError(name, 'system-changed') // a promoted standby voids every prior LSN assumption
          }
          lastSystemId = identity.systemId
          lastTimeline = identity.timeline
          if (stopping) { repl.end(); state = 'stopped'; return }

          let evictions = 0 // caps S2 -> S2 rounds: two 'evict'-configured consumers pointed at the same slot must not terminate each other forever
          while (true) { // S2 -> S2 on eviction: re-reads the row over the SAME connection, no reconnect, bounded only by evictAndAwaitClear's own ~3s deadline
            const row = await readSlotHealth(repl, name)
            if (stopping) { repl.end(); state = 'stopped'; return }
            if (!row) {
              if (!sawSlot) break // first observation ever: fall through to createSlot below
              throw new SlotInvalidatedError(name, 'absent') // was there, now gone — never silently recreated
            }
            sawSlot = true
            if (row.wal_status === 'lost') throw new SlotInvalidatedError(name, 'wal-lost')
            if (row.confirmed_flush_lsn == null) throw new SlotInvalidatedError(name, 'no-confirmed-flush') // guards every LSN use below — this check must run first
            if (row.active === 't' && row.active_pid != null) {
              // A holder who keeps re-acquiring after eviction never heals by retrying — cap the rounds instead of terminating backends in an unbounded ping-pong.
              if (opts.onSlotBusy !== 'evict' || ++evictions > 3) throw new SlotBusyError(name, Number(row.active_pid))
              await evictAndAwaitClear(repl, name, Number(row.active_pid), opts, controller.signal)
              if (stopping) { repl.end(); state = 'stopped'; return }
              continue
            }
            resumed = { confirmedFlush: row.confirmed_flush_lsn, restartLsn: row.restart_lsn! }
            break
          }
        }
        if (stopping) { repl.end(); state = 'stopped'; return }

        let slotForStream: string
        if (resumed) {
          slotForStream = name
          if (opts.onResume) {
            // A throw here routes through retryDelayMs exactly like a backfill throw — it shares this try block's outer catch.
            const p = Promise.resolve(opts.onResume(resumed))
            current = p
            try { await p } finally { current = null }
          }
        } else {
          const created = await repl.createSlot(name, { temporary: !isDurable, snapshot: 'export' })
          if (isDurable) sawSlot = true
          slotForStream = created.slot
          // stop() landing here still leaves the temporary slot dying with the connection, and a
          // durable name as-is — never a compensating dropSlot, the silent data-loss recreate this layer refuses.
          if (stopping) { repl.end(); state = 'stopped'; return }

          state = 'backfilling'
          const window = new AbortController()
          const onWindowAbort = (): void => window.abort()
          controller.signal.addEventListener('abort', onWindowAbort, { once: true })
          const deadline = opts.backfillTimeoutMs
            ? setTimeout(() => {
                window.abort(new BackfillTimeoutError(opts.backfillTimeoutMs!))
                deliverWarning(opts.onWarning, { kind: 'backfill-timeout', ms: opts.backfillTimeoutMs!, message: `minipg: backfill did not resolve within ${opts.backfillTimeoutMs}ms (backfillTimeoutMs) — the session is abandoned` })
              }, opts.backfillTimeoutMs)
            : undefined
          const isReconnect = !firstBackfill
          firstBackfill = false
          try {
            // A non-timeout throw retries IN PLACE on the same slot/snapshot/window — the same
            // shape the handler retry below uses. The deadline bounds the whole window including retries: armed once, outside this loop.
            while (true) {
              try {
                // ZERO commands run on this connection inside the window — the next thing sent on success is START_REPLICATION.
                const p = Promise.resolve(opts.backfill?.({ snapshot: created.snapshot, streamStartLsn: created.consistentPoint, isReconnect, signal: window.signal }))
                current = p
                await p
                current = null
                window.signal.throwIfAborted() // a backfill that ignored the signal still fails the window
                break
              } catch (backfillErr) {
                current = null
                // Identified by the window's own abort reason, never message text — the one throw
                // that skips retry-in-place and goes straight to onFatalError, since a partial backfill is never usable.
                if (window.signal.reason instanceof BackfillTimeoutError) throw window.signal.reason
                if (stopping) { repl.end(); state = 'stopped'; return }
                attempt++
                const delay = effectiveRetryDelayMs(attempt, backfillErr as Error)
                if (delay == null) { fireFatal(backfillErr as Error); return }
                deliverWarning(opts.onWarning, { kind: 'reconnect-attempt', attempt, delayMs: delay, message: `minipg: backfill threw (${(backfillErr as Error).message}) — retrying the same snapshot in place in ${delay}ms (attempt ${attempt})` })
                await sleep(delay, controller.signal)
                if (stopping) { repl.end(); state = 'stopped'; return }
              }
            }
          } finally {
            clearTimeout(deadline)
            controller.signal.removeEventListener('abort', onWindowAbort)
          }
        }

        state = 'streaming'
        const stream = repl.start({
          slot: slotForStream,
          publications: opts.publications,
          shapes: opts.shapes,
          signal: controller.signal,
          messages: opts.messages,
          statusIntervalMs: opts.statusIntervalMs ?? derivedStatusIntervalMs,
          receiveTimeoutMs: opts.receiveTimeoutMs ?? derivedReceiveTimeoutMs,
          maxQueueBytes: opts.maxQueueBytes,
          binary: opts.binary,
          hydrateToast: opts.hydrateToast,
          idleAck: opts.idleAck,
          onWarning: (w) => deliverWarning(opts.onWarning, w),
        })

        for await (const batch of batchTransactions(stream, { maxEvents: opts.maxTransactionEvents })) {
          // next() drains its queue before honoring `ended` (replication.ts), so a fast producer
          // can leave several transactions buffered when stop() resolves — this keeps them from reaching onTransaction.
          if (stopping) { repl.end(); state = 'stopped'; return }
          state = 'handling'
          pendingAckLsn = batch.done ? batch.endLsn : null // done:false carries no commit fields — nothing to ack

          // A handler throw retries the SAME batch in place, zero commands, zero reconnects
          // (PostgreSQL BUG #18754: a second START_REPLICATION here delivers nothing, forever). Escalates only via budget exhaustion (fireFatal) or the handler resolving.
          while (true) {
            try {
              const p = Promise.resolve(opts.onTransaction(batch))
              current = p
              await p
              current = null
              break
            } catch (handlerErr) {
              current = null
              if (stopping) { repl.end(); state = 'stopped'; return }
              attempt++
              const delay = effectiveRetryDelayMs(attempt, handlerErr as Error)
              if (delay == null) { fireFatal(handlerErr as Error); return }
              deliverWarning(opts.onWarning, { kind: 'reconnect-attempt', attempt, delayMs: delay, message: `minipg: onTransaction threw (${(handlerErr as Error).message}) — retrying the same batch in place in ${delay}ms (attempt ${attempt})` })
              await sleep(delay, controller.signal)
              if (stopping) { repl.end(); state = 'stopped'; return }
            }
          }

          if (pendingAckLsn) {
            repl.ack(pendingAckLsn)
            pendingAckLsn = null
            attempt = 0 // the ONLY reset site: the budget resets on ACKED progress, never on delivery
          }
          state = 'streaming'
        }

        state = 'stopped'
        repl.end()
        return
      } catch (rawErr) {
        // A server CopyDone lands here exactly like a dead socket — the walsender accepts exactly
        // one START_REPLICATION per connection (BUG #18754), so both trigger the same full reconnect.
        repl?.end()
        if (stopping) { state = 'stopped'; return }
        // 42704 belongs to slot ACQUISITION (e.g. start() racing a manual drop), never to
        // pg_terminate_backend — 'already gone' from that path is a plain 'f' row inside evictAndAwaitClear.
        const err: Error = currentIsDurable && rawErr instanceof PgError && rawErr.code === '42704'
          ? new SlotInvalidatedError(currentSlotName!, 'absent')
          : (rawErr as Error)
        if (isPermanentFailure(err)) { fireFatal(err); return }
        attempt++
        const delay = effectiveRetryDelayMs(attempt, err)
        if (delay == null) { fireFatal(err); return }
        state = 'backoff'
        deliverWarning(opts.onWarning, { kind: 'reconnect-attempt', attempt, delayMs: delay, message: `minipg: session failed (${err.message}) — reconnecting in ${delay}ms (attempt ${attempt})` })
        await sleep(delay, controller.signal)
        if (stopping) { state = 'stopped'; return }
        // loop back to S1 for a fresh session
      }
    }
  }

  function stop(): Promise<void> {
    if (!stopPromise) stopPromise = doStop()
    return stopPromise
  }

  async function doStop(): Promise<void> {
    stopping = true
    opts.signal?.removeEventListener('abort', onConsumerAbort)
    if (state === 'handling') {
      // Await the handler's promise FIRST and ack what it completed before aborting — aborting
      // first reaches start()'s onAbort, which end()s the socket immediately and a socket being torn down can't carry the ack out.
      if (current) {
        try {
          await current
          if (pendingAckLsn) { repl?.ack(pendingAckLsn); pendingAckLsn = null }
        } catch { /* run()'s own retry loop or its final failure owns a rejected handler promise */ }
      }
      controller.abort()
    } else {
      // Backfilling: the in-flight promise only resolves once its own signal fires, so abort MUST
      // precede the await here. Idle/parked: a generator suspended at next() only wakes via abort -> onAbort -> end().
      controller.abort() // wakes S6's sleep, aborts S3's window, reaches start()'s own signal
      if (current) { try { await current } catch { /* run()'s own catch handles a rejected backfill */ } }
    }
    repl?.end()
    state = 'stopped'
  }

  // retryDelayMs runs inside run()'s own catch blocks — a throw there has nothing left to catch
  // it, and would otherwise reject this floating promise and kill the process outside fireFatal's teardown.
  Promise.resolve().then(run).catch((e) => fireFatal(e as Error))

  return { stop }
}
