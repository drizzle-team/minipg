// Managed CDC session (`replicate()`): a CONSUMER of the raw replication() API, not a rewrite of
// it — owns the reconnect loop, slot lifecycle, the exported-snapshot backfill window, and
// ack-on-handler-resolve, so a caller never issues walsender grammar directly.
//
//   const handle = replicate({
//     url, slot: 'temporary', publications: ['pub'],
//     backfill: async ({ snapshot }) => { … adopt the snapshot on your own connection … },
//     onTransaction: async (batch) => { … persist batch.events, then it acks automatically … },
//   })
//   // later: await handle.stop()
//
// The session loop is an explicit state machine (S0 idle -> S1 connecting -> S2 preparing -> S3
// backfilling -> S4 streaming -> S5 handling -> S6 backoff -> S7 stopped / S8 dead). A transient
// failure anywhere in S1-S5 (including a server CopyDone, which cannot resume on the same
// session — see the CDC-04 note below) drives S6: back off, then loop to S1 for a fresh session.
// A handler throw (S5) retries in place instead, with zero reconnects. A durable slot is
// health-checked over command() on every connect, still inside S2 — see ReplicateOptions.slot.
import { replication, batchTransactions, PublicationMissing, PublicationEmpty, InvalidSlotName, type ReplicationConnection, type ReplicationConfig, type ReplicationWarning, type TransactionBatch, type TableShape } from './replication.ts'
import { PgError } from './errors.ts'
import { randomBytes } from 'node:crypto'

/** One session's lifecycle stage, tracked on the handle's own closure (never module-level) so two
 *  concurrent replicate() calls never share state. Mirrors RESEARCH Architecture Pattern 1's S0-S8
 *  enumeration. */
type SessionState = 'idle' | 'connecting' | 'preparing' | 'backfilling' | 'streaming' | 'handling' | 'backoff' | 'stopped' | 'dead'

export interface ReplicateOptions {
  /** Connection target, forwarded verbatim to replication() on every (re)connect — a connection
   *  string, or the same config object replication() accepts (including a socket factory, which
   *  is how tests inject a fake transport). */
  url: string | ReplicationConfig
  /** 'temporary': a random-suffixed temporary slot is created fresh every session, with an
   *  exported snapshot for backfill — changes made while disconnected are LOST, because the slot
   *  and the WAL it retained die with the connection (CDC-06). { name }: a durable slot,
   *  health-checked over command() on every connect. Absent on the FIRST observation creates it
   *  (exported snapshot, backfill runs); absent on any LATER connect raises
   *  SlotInvalidatedError instead of silently recreating it — the driver never performs that
   *  data-loss decision on the consumer's behalf. It retains WAL until dropped, so a
   *  disconnected consumer resumes exactly where it left off. */
  slot: 'temporary' | { name: string }
  /** Publications to subscribe — forwarded to start(), whose PublicationMissing/PublicationEmpty
   *  probes apply unchanged. */
  publications: string[]
  /** Per-table decode shapes, forwarded to start() unchanged. */
  shapes?: TableShape[]
  /** Aborting stops the session the same way stop() does: settle in-flight work, ack what
   *  completed, close, resolve — never fires onFatalError. */
  signal?: AbortSignal
  /** Runs once per session that just created a slot with an exported snapshot, awaited strictly
   *  between CREATE_REPLICATION_SLOT and START_REPLICATION — the layer issues zero commands on
   *  the replication connection during this window (the snapshot dies on the connection's next
   *  command, measured as SQLSTATE 22023). Absent under a durable slot that already existed and
   *  passed its health check — onResume fires instead, with nothing to backfill. The callback
   *  adopts the snapshot on its OWN normal connection — a raw minipg connection is useless to
   *  drizzle/kysely/prisma, so the layer hands over names, never connections: `begin isolation
   *  level repeatable read`, `set transaction snapshot '<snapshot>'` (outside REPEATABLE READ or
   *  SERIALIZABLE the server raises 0A000), read the baseline, then commit. isReconnect is false
   *  on the handle's first invocation and true when a dropped connection forced a new session
   *  (CDC-13). A throw here retries like a connection failure, bounded by retryDelayMs —
   *  backfill has no cheap path (it only ever runs on a session that just created a slot), so
   *  the retry rebuilds the whole session: new slot, new snapshot, backfill again with
   *  isReconnect true (D-03). */
  backfill?: (info: { snapshot: string; streamStartLsn: string; isReconnect: boolean; signal: AbortSignal }) => void | Promise<void>
  /** Abandon the session if backfill has not resolved within this many ms (omitted = unbounded).
   *  Fires BackfillTimeoutError and routes through retryDelayMs like a connection failure
   *  (CDC-12). */
  backfillTimeoutMs?: number
  /** Forwarded to batchTransactions() as maxEvents — omitted or non-positive means unbounded,
   *  matching that helper's own default; the managed layer does not invent a ceiling the helper
   *  it wraps does not have. */
  maxTransactionEvents?: number
  /** Called with each assembled transaction; the batch is ACKED only once this resolves, and only
   *  for the done:true chunk carrying commitLsn/endLsn — a done:false chunk carries no commit
   *  fields, so acking it is a compile error, not a runtime mistake (CDC-14). A throw here
   *  retries the SAME batch object in place — no reconnect, no command issued — bounded by
   *  retryDelayMs (D-06). */
  onTransaction: (batch: TransactionBatch) => void | Promise<void>
  /** Governs every retry: connection failures, backfill/handler throws, timeouts. Returning null
   *  gives up (fires onFatalError). attempt resets to 0 on ACKED progress only, never on connect
   *  or on delivery — a genuinely idle database that suffers ten transient failures over a day
   *  still reaches onFatalError under the default policy, which is the intended trade, not an
   *  oversight; idleAck advances never reset the budget either. Omitted = a default that gives up
   *  after ~10 attempts, roughly five minutes: 1000ms base, ×2 per attempt, 0-1000ms additive
   *  jitter, capped at 30s (D-01). */
  retryDelayMs?: (attempt: number, err: Error) => number | null
  /** The managed layer's only diagnostic channel — see CdcWarning. Raw warnings forward through
   *  untouched; a throw from this callback is caught and reported to stderr, and the session
   *  keeps going. */
  onWarning?: (w: CdcWarning) => void
  /** Called exactly once, after the layer has fully stopped — no further events and no further
   *  reconnects follow it (CDC-05). Never called from stop() or from the consumer's own signal
   *  aborting; those resolve cleanly instead. */
  onFatalError?: (err: Error) => void
  /** What to do when a durable slot is already held by another backend. 'error' (default) raises
   *  SlotBusyError — retrying a slot someone else legitimately holds never heals on its own.
   *  'evict' terminates the holding backend with pg_terminate_backend and polls until it clears;
   *  it needs the pg_signal_backend grant and fails permanently (42501) without it. Opt-in
   *  because the driver cannot distinguish a stale zombie from a live consumer. */
  onSlotBusy?: 'error' | 'evict'
  /** Fires instead of backfill when a durable slot already existed and passed its health check —
   *  there is nothing to backfill, the slot already carries a resumable position. A throw here
   *  routes through retryDelayMs exactly like a backfill throw. */
  onResume?: (info: { confirmedFlush: string; restartLsn: string }) => void | Promise<void>
  /** Forwarded to start() unchanged; see StartOptions.messages. */
  messages?: boolean
  /** Forwarded to start() when set. Omitted defaults to a value DERIVED from the server's own
   *  wal_sender_timeout, read once per connect: max(10_000, wal_sender_timeout * 0.75) — kept
   *  comfortably above half the server's own ping interval so the server's keepalive clock
   *  actually expires instead of resetting on every status update (D-07). A value set here
   *  always wins over the derived one. */
  statusIntervalMs?: number
  /** Forwarded to start() when set. Omitted defaults to a value DERIVED from the server's own
   *  wal_sender_timeout, read once per connect: max(60_000, wal_sender_timeout * 2) — a fixed
   *  60s figure alone false-fires on a healthy idle stream once statusIntervalMs's own cadence
   *  is factored in (D-07). When wal_sender_timeout reads 0 (disabled — the server never pings),
   *  this is left off entirely and keepAlive is turned on instead, with a
   *  'wal-sender-timeout-disabled' warning naming the setting. A value set here always wins over
   *  the derived one. */
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
  /** Idempotent: the second call returns the first call's promise. Settles in-flight work (the
   *  backfill or handler currently running), acks what completed, closes the session, and
   *  resolves — never fires onFatalError. */
  stop(): Promise<void>
}

/** A durable-slot health check failed on connect — see ReplicateOptions.slot's { name } form.
 *  Fires straight to onFatalError, skipping the retry loop entirely: the slot row was absent
 *  after previously being observed, its wal_status read 'lost', its confirmed_flush_lsn was
 *  null, or the server's systemId/timeline changed since the last connect — none of these heal
 *  with time. Recovery is a decision only the consumer can make: drop the slot and call
 *  replicate() again to start over, or investigate what invalidated it. */
export class SlotInvalidatedError extends Error {
  readonly reason = 'slot-invalidated' as const
  constructor(readonly slot: string, override readonly cause: 'absent' | 'wal-lost' | 'no-confirmed-flush' | 'system-changed') { super(`minipg: replication slot ${JSON.stringify(slot)} is invalidated (${cause}) — drop it and call replicate() again to start over`) }
}

/** backfillTimeoutMs elapsed before the backfill callback resolved — the session is abandoned (no
 *  partial backfill is usable) and the failure routes through retryDelayMs like any other session
 *  failure. */
export class BackfillTimeoutError extends Error {
  readonly reason = 'backfill-timeout' as const
  constructor(readonly ms: number) { super(`minipg: backfill did not resolve within ${ms}ms (backfillTimeoutMs) — the session is abandoned and will retry`) }
}

/** A durable slot is already held by another connection and onSlotBusy is 'error' (the default),
 *  or eviction was attempted and denied — retrying a slot someone else legitimately holds never
 *  heals on its own. Names the slot and the holding PID, never the URL. */
export class SlotBusyError extends Error {
  readonly reason = 'slot-busy' as const
  constructor(readonly slot: string, readonly pid: number) { super(`minipg: replication slot ${JSON.stringify(slot)} is active for PID ${pid} — pass onSlotBusy: 'evict' to terminate it, or wait for the other consumer to release it`) }
}

/** Lifecycle warnings the managed layer itself emits, through the same channel raw
 *  ReplicationWarning values already use — replicate()'s onWarning takes this wider union and
 *  forwards raw warnings through untouched, so a consumer has exactly one warning channel
 *  regardless of which layer noticed something. `reconnect-attempt`: a session failure or an
 *  onTransaction throw consumed a retry attempt, and the layer is about to sleep for `delayMs`
 *  before attempt number `attempt` — fired both for a full reconnect (S6) and for the in-place
 *  handler retry (D-06), since both draw on the same D-01 budget. `backfill-timeout`:
 *  backfillTimeoutMs elapsed and the session is being abandoned. `slot-evicted`: onSlotBusy:
 *  'evict' terminated another backend holding the slot. `wal-sender-timeout-disabled`: the
 *  server's wal_sender_timeout reads 0 (no server-side liveness pings), so receiveTimeoutMs is
 *  left off and keepAlive is turned on instead. A throw from onWarning is caught and reported to
 *  stderr, exactly like the raw layer's own onWarning. */
export type CdcWarning =
  | ReplicationWarning
  | { kind: 'reconnect-attempt'; attempt: number; delayMs: number; message: string }
  | { kind: 'backfill-timeout'; ms: number; message: string }
  | { kind: 'slot-evicted'; slot: string; pid: number; message: string }
  | { kind: 'wal-sender-timeout-disabled'; message: string }

// Mirrors the raw layer's own onWarning contract: a throw from the consumer's callback must not
// take down the session it is reporting on, so it is caught and reported to stderr instead — the
// one process-stream write this module makes.
const deliverWarning = (cb: ((w: CdcWarning) => void) | undefined, w: CdcWarning): void => {
  if (!cb) return
  try { cb(w) } catch (e) { console.error('minipg: onWarning callback threw', e) }
}

// Always inside [a-z0-9_]{1,63} by construction; createSlot() runs checkSlot() regardless.
const tempSlotName = (): string => `minipg_cdc_${randomBytes(4).toString('hex')}`

// Mirrors src/replication.ts's own SLOT_NAME (checkSlot is private to that module) — a durable
// name is interpolated into new SQL sites (health check, eviction) the raw layer does not cover,
// so it gets validated here before ANY of them ever sees it.
const DURABLE_SLOT_NAME = /^[a-z0-9_]{1,63}$/
const validateDurableSlotName = (name: string): void => { if (!DURABLE_SLOT_NAME.test(name)) throw new InvalidSlotName(name) }

// D-01's numbers are the reference consumer's (drizzle-pulse) production defaults, not this
// driver's own reconnect backoff (src/connection.ts, src/pool.ts, src/aurora.ts all use
// multiplicative jitter) — the additive form here is a deliberate divergence, not an oversight.
// attempt is 1-based: the first failure passes attempt 1. Returning null at the ceiling keeps
// "gave up" a single code path; the layer must never have two ways to reach onFatalError.
function defaultRetryDelayMs(attempt: number, _err: Error): number | null {
  return attempt >= 10 ? null : Math.min(1000 * 2 ** (attempt - 1) + Math.random() * 1000, 30_000)
}

// Fourth module-local sleep in the codebase (src/connection.ts, src/pool.ts, src/aurora.ts each
// define their own) — this one resolves early on abort instead of running to completion, so a
// stop() mid-backoff wakes it instead of racing a timer against a flag.
const sleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) { resolve(); return }
    const t = setTimeout(done, ms)
    function done(): void { clearTimeout(t); signal.removeEventListener('abort', done); resolve() }
    signal.addEventListener('abort', done, { once: true })
  })

// Mirrors isFatalAuth's shape (src/connection.ts:167-171): these never heal with time, so retrying
// them identically ten times just delays onFatalError. SlotInvalidatedError, SlotBusyError, and
// 42501 (eviction denied — missing pg_signal_backend) join the auth/publication/slot-name set:
// none of them heal by waiting.
function isPermanentFailure(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code
  if (code === '28P01' || code === '28000' || code === '3D000' || code === '42501') return true
  return err instanceof PublicationMissing || err instanceof PublicationEmpty || err instanceof InvalidSlotName ||
    err instanceof SlotInvalidatedError || err instanceof SlotBusyError
}

interface SlotHealthRow { active: string; active_pid: string | null; wal_status: string; confirmed_flush_lsn: string | null; restart_lsn: string | null }

// CDC-07's health check, one round trip over command() — no second connection (CDC-15). All five
// columns come back as TEXT (Pitfall 3: 't'/'f', never true/false). slot_type = 'logical' makes a
// physical slot squatting this name degenerate to zero rows rather than a row with null LSNs.
async function readSlotHealth(conn: ReplicationConnection, name: string): Promise<SlotHealthRow | null> {
  const r = await conn.command(`select active, active_pid, wal_status, confirmed_flush_lsn, restart_lsn from pg_replication_slots where slot_name = '${name}' and slot_type = 'logical'`)
  if (r.rows.length === 0) return null
  return Object.fromEntries(r.columns.map((c, i) => [c, r.rows[0]![i]])) as unknown as SlotHealthRow
}

// CDC-09's eviction procedure, entirely over command() (CDC-15): terminate the holding backend,
// then poll until the slot reads inactive or a ~3s deadline passes. 't' and 'f' from
// pg_terminate_backend both mean "keep polling" — 'f' is the already-gone case (per research,
// pg_terminate_backend never raises 42704; that code belongs to pg_drop_replication_slot). A
// 42501 PgError propagates out of command() untouched; the caller's isPermanentFailure()
// classifies it, so eviction itself never has to know about retry budgets.
async function evictAndAwaitClear(conn: ReplicationConnection, name: string, pid: number, opts: ReplicateOptions, signal: AbortSignal): Promise<void> {
  if (!Number.isInteger(pid)) throw new SlotBusyError(name, pid) // the trust boundary: a server-sourced value about to ride into SQL
  await conn.command(`select pg_terminate_backend(${pid})`)
  const deadline = Date.now() + 3000
  for (;;) {
    const poll = await conn.command(`select active from pg_replication_slots where slot_name = '${name}'`)
    if (poll.rows[0]?.[0] === 'f') {
      deliverWarning(opts.onWarning, { kind: 'slot-evicted', slot: name, pid, message: `minipg: evicted PID ${pid} holding replication slot ${JSON.stringify(name)}` })
      return
    }
    if (Date.now() >= deadline) throw new SlotBusyError(name, pid) // the active flag lags the backend's actual death by a beat — this is what 55006 guards against
    await sleep(100, signal) // the handle's own signal, so stop() interrupts a stuck eviction cleanly
  }
}

// Merges keepAlive: true into the connect config whenever the caller has decided this connect
// needs it — D-07's wst = 0 case, forced onto the very connect that measured it via the one
// reconnect in run() below, and carried onto every later session through derivedKeepAlive once
// it's known.
function withDerivedKeepAlive(url: string | ReplicationConfig, forceKeepAlive: boolean): string | ReplicationConfig {
  if (!forceKeepAlive) return url
  return typeof url === 'string' ? { url, keepAlive: true } : { ...url, keepAlive: true }
}

/** Start a managed CDC session: connect, administer the slot, backfill inside the exported-
 *  snapshot window, then stream with ack tied to onTransaction's resolution. Returns the handle
 *  SYNCHRONOUSLY — the loop itself starts on a microtask, so a stop() called immediately after
 *  replicate() wins before the first connect. */
export function replicate(opts: ReplicateOptions): ReplicateHandle {
  let state: SessionState = 'idle'
  let stopping = false
  let fatalFired = false // onFatalError's own guard — S8 must never fire it twice (CDC-05)
  let firstBackfill = true
  let attempt = 0 // shared by S6 (reconnect) and the in-place handler retry — one D-01 budget for both
  let repl: ReplicationConnection | null = null
  let current: Promise<unknown> | null = null // the in-flight backfill or handler promise, awaited by stop()
  let pendingAckLsn: string | null = null      // a done:true batch's endLsn once its handler resolves, until acked
  let walSenderTimeoutMs: number | null = null // read once per connect; derives statusIntervalMs/receiveTimeoutMs and the wst=0 keepAlive fallback below
  let stopPromise: Promise<void> | null = null
  let sawSlot = false                    // D-08: absent on the FIRST observation creates the durable slot; absent on any LATER one invalidates it
  let lastSystemId: string | null = null // persisted across connects; a durable slot's systemId/timeline must never move under it
  let lastTimeline: number | null = null
  let derivedKeepAlive = false // D-07: true once a connect measures wal_sender_timeout = 0 and the consumer didn't set their own keepAlive
  let currentSlotName: string | null = null // durable-session bookkeeping for the catch block's 42704 -> SlotInvalidatedError mapping below
  let currentIsDurable = false
  const consumerSetKeepAlive = typeof opts.url !== 'string' && opts.url.keepAlive !== undefined
  const controller = new AbortController() // internal signal: reaches start()'s own signal, so stop() wakes a parked next()
  const effectiveRetryDelayMs = opts.retryDelayMs ?? defaultRetryDelayMs
  // CDC-02: the consumer's own signal behaves exactly like calling stop() — same teardown, same
  // idempotency, never onFatalError. Removed in doStop() so a handle that stopped via stop()
  // itself doesn't leak this listener on opts.signal for the rest of its lifetime.
  const onConsumerAbort = (): void => { stop() }
  opts.signal?.addEventListener('abort', onConsumerAbort, { once: true })
  if (opts.signal?.aborted) stop() // already aborted before replicate() was even called

  // S8: tear down (if a session is open) and fire onFatalError exactly once. Every path that
  // gives up — a permanent error, retryDelayMs returning null on a session failure, or on a
  // handler throw — funnels through here so there is exactly one way to reach onFatalError.
  function fireFatal(err: Error): void {
    if (fatalFired) return
    fatalFired = true
    repl?.end()
    state = 'dead'
    opts.onFatalError?.(err)
  }

  async function run(): Promise<void> {
    if (stopping) { state = 'stopped'; return }
    for (;;) { // one iteration = one session: S1 connecting through S4/S5, or a failure into S6
      currentIsDurable = false // reset every iteration — stale from a prior session must never drive the 42704 mapping below
      currentSlotName = null
      try {
        state = 'connecting'
        repl = await replication(withDerivedKeepAlive(opts.url, derivedKeepAlive))
        if (stopping) { repl.end(); state = 'stopped'; return }

        state = 'preparing'
        // RAW-04 forces every catalog query here: command() during an active stream throws
        // ReplicationBusy, so all slot administration must precede start().
        const wst = await repl.command("select setting::int from pg_settings where name = 'wal_sender_timeout'")
        const raw = wst.rows[0]?.[0]
        walSenderTimeoutMs = raw == null ? null : Number(raw)
        if (stopping) { repl.end(); state = 'stopped'; return }

        // D-07: wst = 0 means the server never pings at all, so THIS connection needs keepAlive —
        // but the setting can only be read after the socket exists, and this is that socket. If it
        // didn't already carry keepAlive (derivedKeepAlive still reflects whatever the PREVIOUS
        // session decided), reconnect once, now with it on, before anything else happens: no slot
        // administration has run yet, so there is nothing to unwind. derivedKeepAlive flips to true
        // right below, so this condition cannot hold twice for the same session.
        if (walSenderTimeoutMs === 0 && !consumerSetKeepAlive && !derivedKeepAlive) {
          repl.end()
          state = 'connecting'
          repl = await replication(withDerivedKeepAlive(opts.url, true))
          if (stopping) { repl.end(); state = 'stopped'; return }
          state = 'preparing'
        }

        // Both liveness timers derive from the server's own wal_sender_timeout rather than
        // shipping as fixed constants — a fixed 60s receiveTimeoutMs false-fires at ~70s on a
        // healthy idle stream (Pitfall 2), because the server only pings once the client has been
        // silent for wal_sender_timeout/2, and a fixed 10s statusIntervalMs keeps resetting that
        // clock. wst = 0 means the server never pings at all: leave receiveTimeoutMs off and rely
        // on the keepAlive turned on above instead.
        const derivedStatusIntervalMs = walSenderTimeoutMs == null ? undefined : Math.max(10_000, walSenderTimeoutMs * 0.75)
        const derivedReceiveTimeoutMs = walSenderTimeoutMs == null || walSenderTimeoutMs === 0 ? undefined : Math.max(60_000, walSenderTimeoutMs * 2)
        derivedKeepAlive = walSenderTimeoutMs === 0 && !consumerSetKeepAlive
        if (walSenderTimeoutMs === 0) {
          deliverWarning(opts.onWarning, { kind: 'wal-sender-timeout-disabled', message: "minipg: the server's wal_sender_timeout is 0 (disabled) — it will never ping this connection, so receiveTimeoutMs is left off and keepAlive is enabled instead" })
        }

        const isDurable = typeof opts.slot !== 'string'
        const name = typeof opts.slot === 'string' ? tempSlotName() : opts.slot.name
        if (isDurable) validateDurableSlotName(name) // a consumer bug, not slot invalidation — checked before ANY interpolation site below sees it
        currentIsDurable = isDurable
        currentSlotName = name

        let resumed: { confirmedFlush: string; restartLsn: string } | null = null

        if (isDurable) {
          // Fixed order on every durable connect: the wst probe above, then identify(), then the
          // health check — mirrors the state-machine table in RESEARCH.md.
          const identity = await repl.identify()
          if (lastSystemId !== null && (identity.systemId !== lastSystemId || identity.timeline !== lastTimeline)) {
            throw new SlotInvalidatedError(name, 'system-changed') // a promoted standby voids every prior LSN assumption
          }
          lastSystemId = identity.systemId
          lastTimeline = identity.timeline
          if (stopping) { repl.end(); state = 'stopped'; return }

          for (;;) { // S2 -> S2 on eviction: re-reads the row over the SAME connection, no reconnect, bounded only by evictAndAwaitClear's own ~3s deadline
            const row = await readSlotHealth(repl, name)
            if (stopping) { repl.end(); state = 'stopped'; return }
            if (!row) {
              if (!sawSlot) break // first observation ever: fall through to createSlot below (D-08)
              throw new SlotInvalidatedError(name, 'absent') // was there, now gone — D-02: never silently recreated
            }
            sawSlot = true
            if (row.wal_status === 'lost') throw new SlotInvalidatedError(name, 'wal-lost')
            if (row.confirmed_flush_lsn == null) throw new SlotInvalidatedError(name, 'no-confirmed-flush') // guards every LSN use below — this check must run first
            if (row.active === 't' && row.active_pid != null) {
              if (opts.onSlotBusy !== 'evict') throw new SlotBusyError(name, Number(row.active_pid))
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
            // A throw here routes through retryDelayMs exactly like a backfill throw (D-03) — it
            // shares this try block's outer catch, so nothing special is needed to wire that up.
            const p = Promise.resolve(opts.onResume(resumed))
            current = p
            try { await p } finally { current = null }
          }
        } else {
          const created = await repl.createSlot(name, { temporary: !isDurable, snapshot: 'export' })
          if (isDurable) sawSlot = true
          slotForStream = created.slot
          // Pitfall 4: stop() landing exactly here still leaves the temporary slot dying with the
          // connection, and a durable name as the slot the consumer asked for — never a compensating
          // dropSlot, which would be the silent-data-loss recreate D-02 refuses to perform.
          if (stopping) { repl.end(); state = 'stopped'; return }

          state = 'backfilling'
          const window = new AbortController()
          const onWindowAbort = (): void => window.abort()
          controller.signal.addEventListener('abort', onWindowAbort, { once: true })
          const deadline = opts.backfillTimeoutMs
            ? setTimeout(() => {
                window.abort(new BackfillTimeoutError(opts.backfillTimeoutMs!))
                deliverWarning(opts.onWarning, { kind: 'backfill-timeout', ms: opts.backfillTimeoutMs!, message: `minipg: backfill did not resolve within ${opts.backfillTimeoutMs}ms (backfillTimeoutMs) — the session is abandoned and will retry` })
              }, opts.backfillTimeoutMs)
            : undefined
          const isReconnect = !firstBackfill
          firstBackfill = false
          try {
            // ZERO commands run on this connection inside the window: the very next thing sent
            // below is START_REPLICATION.
            const p = Promise.resolve(opts.backfill?.({ snapshot: created.snapshot, streamStartLsn: created.consistentPoint, isReconnect, signal: window.signal }))
            current = p
            await p
            window.signal.throwIfAborted() // a backfill that ignored the signal still fails the window
          } finally {
            clearTimeout(deadline)
            controller.signal.removeEventListener('abort', onWindowAbort)
            current = null
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
          // next() drains its queue before honoring `ended` (src/replication.ts), so a fast
          // producer can leave several transactions already parsed and buffered when stop()
          // resolves — this check keeps every one of them from still reaching onTransaction.
          if (stopping) { repl.end(); state = 'stopped'; return }
          state = 'handling'
          pendingAckLsn = batch.done ? batch.endLsn : null // done:false carries no commit fields — nothing to ack

          // D-06: a handler throw retries the SAME batch in place on this same session — the
          // stream generator stays parked at its yield the whole time, zero commands issued,
          // zero reconnects (PostgreSQL BUG #18754: a second START_REPLICATION on this session
          // would deliver nothing, forever). Only escalates out of this loop by returning
          // (budget exhausted -> fireFatal) or by falling through once the handler resolves.
          for (;;) {
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
        // A server CopyDone (ReplicationStreamEnded) lands here exactly like a dead socket: the
        // walsender accepts exactly one START_REPLICATION per connection (BUG #18754), so the
        // spent session cannot stream again either way — there is no separate CopyDone branch to
        // write. CDC-04 is satisfied by equivalence (D-06): both trigger the same full reconnect.
        repl?.end()
        if (stopping) { state = 'stopped'; return }
        // CDC-09's correction: 42704 belongs to slot ACQUISITION (e.g. start() racing a manual
        // drop on a durable slot), never to pg_terminate_backend — 'already gone' from THAT path
        // is a plain 'f' row, handled inside evictAndAwaitClear above.
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
      // A handler's own promise resolves on ITS OWN schedule, not the abort's — so await it
      // FIRST and ack what it completed BEFORE aborting. Aborting first would reach start()'s
      // onAbort, which end()s the raw connection immediately (Pitfall 5) — a socket already
      // being torn down can't carry the ack out, so "acks what completed" (CDC-02) would
      // silently lose the write. The backfilling/idle branch below has the opposite requirement.
      if (current) {
        try {
          await current
          if (pendingAckLsn) { repl?.ack(pendingAckLsn); pendingAckLsn = null }
        } catch { /* run()'s own retry loop or its final failure owns a rejected handler promise */ }
      }
      controller.abort()
    } else {
      // Backfilling: the in-flight promise only resolves once ITS OWN signal (derived from this
      // controller) fires, so abort MUST precede the await here or it never settles. Idle/parked:
      // a generator suspended at next() only wakes via abort -> onAbort -> end() (Pitfall 5).
      controller.abort() // wakes S6's sleep, aborts S3's window, reaches start()'s own signal
      if (current) { try { await current } catch { /* run()'s own catch handles a rejected backfill */ } }
    }
    repl?.end()
    state = 'stopped'
  }

  Promise.resolve().then(run)

  return { stop }
}
