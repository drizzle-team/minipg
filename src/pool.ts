// Connection pool over Connection with single-flight reconnect (circuit breaker).
// When establishing a connection fails (server down/restarting), the pool trips
// "down": ONE background probe retries with backoff+jitter while all acquirers wait,
// and only once the probe reconnects is the herd released — no reconnect storm into a
// recovering database. Dead idle connections are evicted on checkout; open transactions
// are rolled back before reuse. In-flight queries are never silently replayed.
import { Connection } from './connection.ts'
import { resolveUrl } from './url.ts'
import type { PoolConfig, QueryOptions, QueryResult } from './types.ts'

interface Waiter { resolve: (c: Connection) => void; reject: (e: Error) => void }

// Errors that mean "this database will never come back on its own" — don't probe.
function classify(err: unknown): 'fatal' | 'unavailable' {
  const code = (err as { code?: string } | null)?.code
  if (code === '28P01' || code === '28000' || code === '3D000') return 'fatal' // bad auth / missing db
  return 'unavailable' // ECONNREFUSED/RESET/timeout, 57P0x shutdown, connection terminated, ...
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

export class Pool {
  private cfg: PoolConfig
  private max: number
  private idle: Connection[] = []
  private all = new Set<Connection>()
  private out = new Set<Connection>() // checked-out (double-release guard)
  private waiters: Waiter[] = [] // waiting because the pool is at max
  private closed = false

  // Idle-connection eviction + a minimal EventEmitter surface, so `pool.options.idleTimeoutMillis` and
  // the `'release'` event let Vercel's attachDatabasePool() drive Fluid-compute connection draining.
  private idleMs: number
  private idleTimers = new Map<Connection, ReturnType<typeof setTimeout>>()
  private listeners = new Map<string, Array<(...args: unknown[]) => void>>()
  readonly options: { idleTimeoutMillis: number }

  // reconnect / circuit breaker
  private rcEnabled: boolean
  private base: number
  private maxBackoff: number
  private acquireTimeout: number
  private down = false
  private broken: Error | null = null // fatal, unrecoverable (e.g. auth)
  private recovered: (() => void)[] = [] // wake-ups for acquirers waiting on recovery

  constructor(config: string | PoolConfig = {}) {
    const cfg = resolveUrl(typeof config === 'string' ? { url: config } : config) // string / url -> config
    this.cfg = cfg
    this.max = cfg.max ?? 10
    const rc = cfg.reconnect
    this.rcEnabled = rc !== false
    const o = rc && typeof rc === 'object' ? rc : {}
    this.base = o.baseMs ?? 50
    this.maxBackoff = o.maxMs ?? 2000
    this.acquireTimeout = o.acquireTimeoutMs ?? 30000
    this.idleMs = cfg.idleTimeoutMillis ?? 0
    this.options = { idleTimeoutMillis: this.idleMs } // detection surface for attachDatabasePool()
  }

  get size(): number { return this.all.size }
  get idleCount(): number { return this.idle.length }
  get waiting(): number { return this.waiters.length }
  get isDown(): boolean { return this.down }

  /** EventEmitter-style subscription (only `'release'` is emitted). Present so attachDatabasePool()
   *  recognizes the pool and can extend the instance lifetime on release. */
  on(event: string, listener: (...args: unknown[]) => void): this {
    const arr = this.listeners.get(event) ?? []; arr.push(listener); this.listeners.set(event, arr); return this
  }
  private emit(event: string): void { const arr = this.listeners.get(event); if (arr) for (const l of arr) try { l() } catch { /* listener errors never break the pool */ } }

  // idle eviction: a connection sitting in `idle` past idleMs is closed and dropped
  private armIdleTimer(conn: Connection): void {
    if (this.idleMs <= 0) return
    this.idleTimers.set(conn, setTimeout(() => {
      this.idleTimers.delete(conn)
      const i = this.idle.indexOf(conn); if (i < 0) return // checked out again meanwhile
      this.idle.splice(i, 1); this.all.delete(conn); void conn.end()
    }, this.idleMs))
  }
  private clearIdleTimer(conn: Connection): void { const t = this.idleTimers.get(conn); if (t) { clearTimeout(t); this.idleTimers.delete(conn) } }

  async acquire(): Promise<Connection> {
    if (this.closed) throw new Error('pool is closed')
    if (this.broken) throw this.broken
    if (this.down && this.rcEnabled) await this.waitForRecovery()
    // reuse a live idle connection (evict dead ones that died while idle)
    while (this.idle.length) {
      const c = this.idle.pop()!
      this.clearIdleTimer(c) // no longer idle
      if (c.state === 'closed') { this.all.delete(c); continue }
      this.out.add(c)
      return c
    }
    if (this.all.size < this.max) {
      try {
        const c = await this.open()
        this.out.add(c)
        return c
      } catch (e) {
        if (!this.rcEnabled) throw e
        this.trip(e as Error)
        if (this.broken) throw this.broken
        return this.acquire() // breaker is now down -> wait for the single probe
      }
    }
    return new Promise<Connection>((resolve, reject) => this.waiters.push({ resolve, reject }))
  }

  private async open(): Promise<Connection> {
    // Pooled connections are fragile: the POOL owns reconnection (its breaker), so a
    // dead member is evicted, not self-reconnecting. Disable Connection-level reconnect.
    const conn = new Connection({ ...this.cfg, reconnect: false })
    this.all.add(conn)
    try { await conn.connect() } catch (e) { this.all.delete(conn); throw e }
    return conn
  }

  // ---- circuit breaker ----
  private trip(err: Error): void {
    if (classify(err) === 'fatal') { this.broken = err; this.settleBreaker(); return }
    if (this.down || this.closed) return
    this.down = true
    this.startProbe()
  }

  // single-flight: exactly one connection probes the server while everyone waits.
  private startProbe(): void {
    void (async () => {
      for (let attempt = 0; this.down && !this.closed; attempt++) {
        const backoff = Math.min(this.maxBackoff, this.base * 2 ** attempt)
        await sleep(backoff * (0.5 + Math.random() * 0.5)) // jitter
        if (!this.down || this.closed) return
        try {
          const c = await this.open()
          this.idle.push(c); this.armIdleTimer(c) // the probe connection becomes the first reusable one
          this.settleBreaker() // recovered -> release the herd
          return
        } catch (e) {
          if (classify(e as Error) === 'fatal') { this.broken = e as Error; this.settleBreaker(); return }
          // otherwise keep probing with growing backoff
        }
      }
    })()
  }

  // Clear the down flag and wake everyone waiting on recovery (they re-check broken/closed).
  private settleBreaker(): void {
    this.down = false
    for (const w of this.recovered.splice(0)) w()
  }

  private waitForRecovery(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let done = false
      const to = setTimeout(() => {
        if (done) return
        done = true
        const i = this.recovered.indexOf(onRecover)
        if (i >= 0) this.recovered.splice(i, 1)
        reject(new Error(`pool acquire timed out after ${this.acquireTimeout}ms waiting for the database to recover`))
      }, this.acquireTimeout)
      const onRecover = () => {
        if (done) return
        done = true
        clearTimeout(to)
        if (this.closed) reject(new Error('pool is closed'))
        else if (this.broken) reject(this.broken)
        else resolve()
      }
      this.recovered.push(onRecover)
    })
  }

  // ---- checkout/checkin ----
  release(conn: Connection): void {
    if (!this.out.has(conn)) return // double-release / foreign connection
    this.out.delete(conn)
    this.emit('release') // signal to attachDatabasePool() that a client returned (extends instance life)
    if (conn.state === 'closed') { this.all.delete(conn); this.refill(); return }
    if (this.closed) { this.all.delete(conn); void conn.end(); return }
    if (conn.inTransaction) { // never leak tx state across checkouts
      conn.query('ROLLBACK').then(() => this.checkin(conn)).catch(() => { this.all.delete(conn); void conn.end(); this.refill() })
      return
    }
    this.checkin(conn)
  }

  private checkin(conn: Connection): void {
    // A live connection returning during an outage proves the server is back.
    if (this.down) { this.idle.push(conn); this.armIdleTimer(conn); this.settleBreaker(); return }
    const w = this.waiters.shift()
    if (w) { this.out.add(conn); w.resolve(conn) } else { this.idle.push(conn); this.armIdleTimer(conn) }
  }

  private refill(): void {
    if (this.down || this.closed) return // the probe handles recovery; don't herd
    while (this.waiters.length && this.all.size < this.max) {
      const w = this.waiters.shift()!
      this.open().then((c) => { this.out.add(c); w.resolve(c) }).catch((e) => {
        if (!this.rcEnabled) return w.reject(e as Error)
        this.waiters.unshift(w)
        this.trip(e as Error)
        if (this.broken) this.failWaiters(this.broken)
      })
    }
  }

  private failWaiters(err: Error): void { for (const w of this.waiters.splice(0)) w.reject(err) }

  async query(sql: string, params: unknown[] = [], opts: QueryOptions = {}): Promise<QueryResult<never>> {
    const conn = await this.acquire()
    try { return await (conn.query as (s: string, p: unknown[], o: QueryOptions) => Promise<QueryResult<never>>)(sql, params, opts) }
    finally { this.release(conn) }
  }

  /** Check out a dedicated connection (e.g. for a transaction). `release()` is idempotent. */
  async connect(): Promise<{ client: Connection; release: () => void }> {
    const client = await this.acquire()
    return { client, release: () => this.release(client) }
  }

  async end(): Promise<void> {
    this.closed = true
    const err = new Error('pool ended')
    this.failWaiters(err)
    for (const w of this.recovered.splice(0)) w() // wake recovery-waiters -> they reject (closed)
    for (const t of this.idleTimers.values()) clearTimeout(t); this.idleTimers.clear()
    const conns = [...this.all]
    this.all.clear(); this.idle = []; this.out.clear()
    await Promise.all(conns.map((c) => c.end()))
  }
}
