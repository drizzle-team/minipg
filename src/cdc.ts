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
// backfilling -> S4 streaming -> S5 handling -> S6 backoff -> S7 stopped / S8 dead). This module
// wires the happy path (S0 through S7) end to end; S6 (retry/backoff) and the durable-slot health
// check land in later plans — a failure anywhere in this slice tears the session down and treats
// it as terminal for now.
import { replication, batchTransactions, type ReplicationConnection, type ReplicationConfig, type ReplicationWarning, type TransactionBatch, type TableShape } from './replication.ts'
import { randomBytes } from 'node:crypto'

/** One session's lifecycle stage, tracked on the handle's own closure (never module-level) so two
 *  concurrent replicate() calls never share state. Mirrors RESEARCH Architecture Pattern 1's S0-S8
 *  enumeration; S6/S8's transitions are wired starting plan 04-02. */
type SessionState = 'idle' | 'connecting' | 'preparing' | 'backfilling' | 'streaming' | 'handling' | 'backoff' | 'stopped' | 'dead'

export interface ReplicateOptions {
  /** Connection target, forwarded verbatim to replication() on every (re)connect — a connection
   *  string, or the same config object replication() accepts (including a socket factory, which
   *  is how tests inject a fake transport). */
  url: string | ReplicationConfig
  /** 'temporary': a random-suffixed temporary slot is created fresh every session, with an
   *  exported snapshot for backfill — changes made while disconnected are LOST, because the slot
   *  and the WAL it retained die with the connection (CDC-06). { name }: a durable slot, created
   *  once and health-checked on every connect (lands plan 04-03); it retains WAL until dropped,
   *  so a disconnected consumer resumes exactly where it left off. */
  slot: 'temporary' | { name: string }
  /** Publications to subscribe — forwarded to start(), whose PublicationMissing/PublicationEmpty
   *  probes apply unchanged. */
  publications: string[]
  /** Per-table decode shapes, forwarded to start() unchanged. */
  shapes?: TableShape[]
  /** Aborting stops the session the same way stop() does: settle in-flight work, ack what
   *  completed, close, resolve — never fires onFatalError. Wired starting plan 04-02. */
  signal?: AbortSignal
  /** Runs once per session that just created a slot with an exported snapshot, awaited strictly
   *  between CREATE_REPLICATION_SLOT and START_REPLICATION — the layer issues zero commands on
   *  the replication connection during this window (the snapshot dies on the connection's next
   *  command, measured as SQLSTATE 22023). Absent under a durable slot that already existed
   *  (onResume fires instead, lands plan 04-03). The callback adopts the snapshot on its OWN
   *  normal connection — a raw minipg connection is useless to drizzle/kysely/prisma, so the
   *  layer hands over names, never connections: `begin isolation level repeatable read`, `set
   *  transaction snapshot '<snapshot>'` (outside REPEATABLE READ or SERIALIZABLE the server
   *  raises 0A000), read the baseline, then commit. isReconnect is false on the handle's first
   *  invocation and true when a dropped connection forced a new session (CDC-13). A throw here
   *  retries like a connection failure, bounded by retryDelayMs (lands plan 04-02). */
  backfill?: (info: { snapshot: string; streamStartLsn: string; isReconnect: boolean; signal: AbortSignal }) => void | Promise<void>
  /** Abandon the session if backfill has not resolved within this many ms (omitted = unbounded).
   *  Fires BackfillTimeoutError and routes through retryDelayMs like a connection failure (lands
   *  plan 04-02). */
  backfillTimeoutMs?: number
  /** Forwarded to batchTransactions() as maxEvents — omitted or non-positive means unbounded,
   *  matching that helper's own default; the managed layer does not invent a ceiling the helper
   *  it wraps does not have. */
  maxTransactionEvents?: number
  /** Called with each assembled transaction; the batch is ACKED only once this resolves, and only
   *  for the done:true chunk carrying commitLsn/endLsn — a done:false chunk carries no commit
   *  fields, so acking it is a compile error, not a runtime mistake (CDC-14). A throw here
   *  retries the SAME batch in place, bounded by retryDelayMs (lands plan 04-02). */
  onTransaction: (batch: TransactionBatch) => void | Promise<void>
  /** Governs every retry: connection failures, backfill/handler throws, timeouts. Returning null
   *  gives up (fires onFatalError). attempt resets to 0 on ACKED progress only, never on connect
   *  or on delivery — a genuinely idle database that suffers ten transient failures over a day
   *  still reaches onFatalError under the default policy, which is the intended trade, not an
   *  oversight; idleAck advances never reset the budget either. Omitted = a default that gives up
   *  after ~10 attempts, roughly five minutes (lands plan 04-02). */
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
   *  because the driver cannot distinguish a stale zombie from a live consumer (lands plan
   *  04-03). */
  onSlotBusy?: 'error' | 'evict'
  /** Fires instead of backfill when a durable slot already existed and passed its health check —
   *  there is nothing to backfill, the slot already carries a resumable position. A throw here
   *  routes through retryDelayMs exactly like a backfill throw (lands plan 04-03). */
  onResume?: (info: { confirmedFlush: string; restartLsn: string }) => void | Promise<void>
  /** Forwarded to start() unchanged; see StartOptions.messages. */
  messages?: boolean
  /** Forwarded to start() unchanged; see StartOptions.statusIntervalMs. Overriding this defeats
   *  the value plan 04-03 derives from the server's own wal_sender_timeout — leave it unset
   *  unless you have a specific reason. */
  statusIntervalMs?: number
  /** Forwarded to start() unchanged; see StartOptions.receiveTimeoutMs. Overriding this defeats
   *  the value plan 04-03 derives from the server's own wal_sender_timeout — leave it unset
   *  unless you have a specific reason. */
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
 *  replicate() again to start over, or investigate what invalidated it. Thrown starting plan
 *  04-03. */
export class SlotInvalidatedError extends Error {
  readonly reason = 'slot-invalidated' as const
  constructor(readonly slot: string, override readonly cause: 'absent' | 'wal-lost' | 'no-confirmed-flush' | 'system-changed') { super(`minipg: replication slot ${JSON.stringify(slot)} is invalidated (${cause}) — drop it and call replicate() again to start over`) }
}

/** backfillTimeoutMs elapsed before the backfill callback resolved — the session is abandoned (no
 *  partial backfill is usable) and the failure routes through retryDelayMs like any other session
 *  failure. Thrown starting plan 04-02. */
export class BackfillTimeoutError extends Error {
  readonly reason = 'backfill-timeout' as const
  constructor(readonly ms: number) { super(`minipg: backfill did not resolve within ${ms}ms (backfillTimeoutMs) — the session is abandoned and will retry`) }
}

/** A durable slot is already held by another connection and onSlotBusy is 'error' (the default),
 *  or eviction was attempted and denied — retrying a slot someone else legitimately holds never
 *  heals on its own. Names the slot and the holding PID, never the URL. Thrown starting plan
 *  04-03. */
export class SlotBusyError extends Error {
  readonly reason = 'slot-busy' as const
  constructor(readonly slot: string, readonly pid: number) { super(`minipg: replication slot ${JSON.stringify(slot)} is active for PID ${pid} — pass onSlotBusy: 'evict' to terminate it, or wait for the other consumer to release it`) }
}

/** Lifecycle warnings the managed layer itself emits, through the same channel raw
 *  ReplicationWarning values already use — replicate()'s onWarning takes this wider union and
 *  forwards raw warnings through untouched, so a consumer has exactly one warning channel
 *  regardless of which layer noticed something. `reconnect-attempt`: the session died and a
 *  retry is about to sleep for delayMs before attempt number `attempt` (lands plan 04-02).
 *  `backfill-timeout`: backfillTimeoutMs elapsed and the session is being abandoned (lands plan
 *  04-02). `slot-evicted`: onSlotBusy: 'evict' terminated another backend holding the slot
 *  (lands plan 04-03). `wal-sender-timeout-disabled`: the server's wal_sender_timeout reads 0 (no
 *  server-side liveness pings), so receiveTimeoutMs is left off and keepAlive is turned on
 *  instead (lands plan 04-03). A throw from onWarning is caught and reported to stderr, exactly
 *  like the raw layer's own onWarning. */
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

/** Start a managed CDC session: connect, administer the slot, backfill inside the exported-
 *  snapshot window, then stream with ack tied to onTransaction's resolution. Returns the handle
 *  SYNCHRONOUSLY — the loop itself starts on a microtask, so a stop() called immediately after
 *  replicate() wins before the first connect. */
export function replicate(opts: ReplicateOptions): ReplicateHandle {
  let state: SessionState = 'idle'
  let stopping = false
  let firstBackfill = true
  let repl: ReplicationConnection | null = null
  let current: Promise<unknown> | null = null // the in-flight backfill or handler promise, awaited by stop()
  let pendingAckLsn: string | null = null      // a done:true batch's endLsn once its handler resolves, until acked
  let walSenderTimeoutMs: number | null = null // stashed for plan 04-03's timer derivation; unused here
  let stopPromise: Promise<void> | null = null
  const controller = new AbortController() // internal signal: reaches start()'s own signal, so stop() wakes a parked next()

  async function run(): Promise<void> {
    if (stopping) { state = 'stopped'; return }
    try {
      state = 'connecting'
      repl = await replication(opts.url)
      if (stopping) { repl.end(); state = 'stopped'; return }

      state = 'preparing'
      // RAW-04 forces every catalog query here: command() during an active stream throws
      // ReplicationBusy, so all slot administration must precede start().
      const wst = await repl.command("select setting::int from pg_settings where name = 'wal_sender_timeout'")
      const raw = wst.rows[0]?.[0]
      walSenderTimeoutMs = raw == null ? null : Number(raw)
      if (stopping) { repl.end(); state = 'stopped'; return }

      const name = typeof opts.slot === 'string' ? tempSlotName() : opts.slot.name
      const created = await repl.createSlot(name, { temporary: typeof opts.slot === 'string', snapshot: 'export' })
      // Pitfall 4: stop() landing exactly here still leaves the temporary slot dying with the
      // connection, and a durable name as the slot the consumer asked for — never a compensating
      // dropSlot, which would be the silent-data-loss recreate D-02 refuses to perform.
      if (stopping) { repl.end(); state = 'stopped'; return }

      state = 'backfilling'
      const window = new AbortController()
      const onWindowAbort = (): void => window.abort()
      controller.signal.addEventListener('abort', onWindowAbort, { once: true })
      const deadline = opts.backfillTimeoutMs
        ? setTimeout(() => window.abort(new BackfillTimeoutError(opts.backfillTimeoutMs!)), opts.backfillTimeoutMs)
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

      state = 'streaming'
      const stream = repl.start({
        slot: created.slot,
        publications: opts.publications,
        shapes: opts.shapes,
        signal: controller.signal,
        messages: opts.messages,
        statusIntervalMs: opts.statusIntervalMs,
        receiveTimeoutMs: opts.receiveTimeoutMs,
        maxQueueBytes: opts.maxQueueBytes,
        binary: opts.binary,
        hydrateToast: opts.hydrateToast,
        idleAck: opts.idleAck,
        onWarning: (w) => deliverWarning(opts.onWarning, w),
      })

      for await (const batch of batchTransactions(stream, { maxEvents: opts.maxTransactionEvents })) {
        state = 'handling'
        pendingAckLsn = batch.done ? batch.endLsn : null // done:false carries no commit fields — nothing to ack
        const p = Promise.resolve(opts.onTransaction(batch))
        current = p
        await p
        current = null
        if (pendingAckLsn) { repl.ack(pendingAckLsn); pendingAckLsn = null }
        state = 'streaming'
      }

      state = 'stopped'
      repl.end()
    } catch (err) {
      repl?.end()
      state = stopping ? 'stopped' : 'dead'
      if (!stopping) opts.onFatalError?.(err as Error) // stop()'s own abort never reaches here as a fatal error
    }
  }

  function stop(): Promise<void> {
    if (!stopPromise) stopPromise = doStop()
    return stopPromise
  }

  async function doStop(): Promise<void> {
    stopping = true
    controller.abort() // wakes S6's sleep (later plans), aborts S3's window, reaches start()'s own signal
    if (current) { try { await current } catch { /* run()'s own catch reports this */ } }
    if (pendingAckLsn) { repl?.ack(pendingAckLsn); pendingAckLsn = null } // acks what completed, if run() hasn't already
    repl?.end()
    state = 'stopped'
  }

  Promise.resolve().then(run)

  return { stop }
}
