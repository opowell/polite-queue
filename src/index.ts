/**
 * Per-domain request scheduling, for fetching other people's pages without
 * hammering them.
 *
 * Work is filed under a key, usually a host name (see `hostOf`), and each key
 * has a policy: how many of its tasks may run at once, the least time between
 * one start and the next, a random extra on top of that, and how long to back
 * off when the server pushes back. A global cap bounds the tasks in flight
 * across all keys. Nothing here fetches anything: a task is whatever the caller
 * does with its turn, whether that is a `fetch`, a tab load, or a Playwright
 * navigation.
 */

export interface Policy {
  /**
   * Least time, in ms, from the latest start or finish on a key to the next
   * start. With one task at a time this is the gap after each request ends.
   */
  minGapMs: number
  /** Up to this many ms more, drawn afresh each time, so requests do not land on a beat. */
  jitterMs: number
  /** Tasks on one key in flight at once. */
  maxActive: number
  /** The first pause `backoff` imposes; each one after it doubles, until `succeed`. */
  backoffMs: number
  /** The longest pause a run of backoffs grows to. */
  maxBackoffMs: number
}

export const DEFAULT_POLICY: Policy = {
  minGapMs: 1_000,
  jitterMs: 0,
  maxActive: 1,
  backoffMs: 30_000,
  maxBackoffMs: 10 * 60_000,
}

export interface QueueOptions {
  /** A key's policy, over `DEFAULT_POLICY`. Read afresh every time it matters, so a changed setting applies at once. */
  policy?: (key: string) => Partial<Policy> | undefined
  /** Tasks in flight across every key. Unlimited when not given. */
  maxActive?: number
  /** For tests: the source of jitter, in [0, 1). */
  random?: () => number
}

export interface RunOptions {
  /** Higher goes first among the tasks waiting on a key; equal priorities go in the order asked. */
  priority?: number
  /** Aborting it takes a waiting task out of the queue and rejects with the signal's reason. A running task is the caller's to stop. */
  signal?: AbortSignal
}

/** One key as `snapshot` reports it. */
export interface KeySnapshot {
  key: string
  /** Tasks waiting for their turn. */
  waiting: number
  /** Tasks running. */
  active: number
  /** When the next task may start, as far as the gap and any pause say (ms since the epoch). */
  readyAt: number
  /** Until when a backoff or `pause` holds the key, or 0. */
  pausedUntil: number
  /** Backoffs since the last `succeed`. */
  strikes: number
}

interface Waiter {
  priority: number
  seq: number
  start: (release: () => void) => void
  fail: (reason: unknown) => void
}

interface KeyState {
  waiting: Waiter[]
  active: number
  /** The latest start or finish; the gap is counted from here. */
  lastAt: number
  /** Where in the jitter range the gap after `lastAt` falls. */
  draw: number
  pausedUntil: number
  strikes: number
}

export class PoliteQueue {
  private readonly keys = new Map<string, KeyState>()
  private policyOf: (key: string) => Partial<Policy> | undefined
  private maxActive: number
  private readonly random: () => number
  private active = 0
  private seq = 0
  private timer: ReturnType<typeof setTimeout> | undefined
  private timerAt = 0

  constructor(options: QueueOptions = {}) {
    this.policyOf = options.policy ?? (() => undefined)
    this.maxActive = options.maxActive ?? Infinity
    this.random = options.random ?? Math.random
  }

  /** Swaps the policy source or the global cap; waiting tasks are re-judged at once. */
  configure(options: Pick<QueueOptions, 'policy' | 'maxActive'>): void {
    if (options.policy) {
      this.policyOf = options.policy
    }
    if (options.maxActive !== undefined) {
      this.maxActive = options.maxActive
    }
    this.pump()
  }

  policy(key: string): Policy {
    const own = this.policyOf(key) ?? {}
    const policy = { ...DEFAULT_POLICY }
    for (const name of Object.keys(policy) as (keyof Policy)[]) {
      const value = own[name]
      if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
        policy[name] = value
      }
    }
    policy.maxActive = Math.max(1, Math.floor(policy.maxActive))
    return policy
  }

  /** Runs `task` when `key`'s turn comes, holding the turn until it settles. */
  async run<T>(key: string, task: () => T | Promise<T>, options?: RunOptions): Promise<T> {
    const release = await this.acquire(key, options)
    try {
      return await task()
    } finally {
      release()
    }
  }

  /**
   * Waits for `key`'s turn and hands back the function that ends it. For a
   * caller whose work is not one promise: a worker that holds a page across
   * several navigations, or a request answered through a callback.
   */
  acquire(key: string, options: RunOptions = {}): Promise<() => void> {
    const signal = options.signal
    if (signal?.aborted) {
      return Promise.reject(signal.reason)
    }
    return new Promise((resolve, reject) => {
      const state = this.state(key)
      const onAbort = () => {
        const at = state.waiting.indexOf(waiter)
        if (at >= 0) {
          state.waiting.splice(at, 1)
          reject(signal!.reason)
          this.pump()
        }
      }
      const waiter: Waiter = {
        priority: options.priority ?? 0,
        seq: this.seq++,
        start: (release) => {
          signal?.removeEventListener('abort', onAbort)
          resolve(release)
        },
        fail: (reason) => {
          signal?.removeEventListener('abort', onAbort)
          reject(reason)
        },
      }
      signal?.addEventListener('abort', onAbort, { once: true })
      // Kept sorted: highest priority first, then oldest.
      let at = state.waiting.length
      while (at > 0 && state.waiting[at - 1].priority < waiter.priority) {
        at--
      }
      state.waiting.splice(at, 0, waiter)
      this.pump()
    })
  }

  /**
   * The server pushed back (a 429, a 503, a bot check): nothing new starts on
   * `key` for a while. Each backoff before the next `succeed` doubles the pause,
   * and a `Retry-After` the server sent is honoured if it asks for longer.
   * Returns the pause, in ms.
   */
  backoff(key: string, retryAfterMs?: number): number {
    const state = this.state(key)
    const policy = this.policy(key)
    state.strikes++
    const doubled = policy.backoffMs * 2 ** Math.min(state.strikes - 1, 30)
    const ms = Math.max(Math.min(doubled, policy.maxBackoffMs), retryAfterMs ?? 0)
    state.pausedUntil = Math.max(state.pausedUntil, Date.now() + ms)
    this.pump()
    return ms
  }

  /** A good answer from `key`: the next backoff starts small again. */
  succeed(key: string): void {
    const state = this.keys.get(key)
    if (state) {
      state.strikes = 0
    }
  }

  /** Holds `key` for `ms`, whatever its strikes. A pause of 0 lifts one in force. */
  pause(key: string, ms: number): void {
    const state = this.state(key)
    state.pausedUntil = ms > 0 ? Math.max(state.pausedUntil, Date.now() + ms) : 0
    this.pump()
  }

  /** Rejects every waiting task, on one key or all of them. Running tasks are left to finish. */
  clear(key?: string, reason: unknown = new Error('Cleared from the queue')): void {
    const entries = key === undefined ? [...this.keys] : [[key, this.keys.get(key)] as const]
    for (const [, state] of entries) {
      for (const waiter of state?.waiting.splice(0) ?? []) {
        waiter.fail(reason)
      }
    }
    this.pump()
  }

  /** Tasks waiting or running on `key`, or on every key. */
  pending(key?: string): number {
    if (key !== undefined) {
      const state = this.keys.get(key)
      return state ? state.waiting.length + state.active : 0
    }
    let count = 0
    for (const state of this.keys.values()) {
      count += state.waiting.length + state.active
    }
    return count
  }

  /** Every key with anything waiting, running, or held. */
  snapshot(): KeySnapshot[] {
    return [...this.keys].map(([key, state]) => ({
      key,
      waiting: state.waiting.length,
      active: state.active,
      readyAt: Math.max(this.readyAt(key, state), state.pausedUntil),
      pausedUntil: state.pausedUntil,
      strikes: state.strikes,
    }))
  }

  private state(key: string): KeyState {
    let state = this.keys.get(key)
    if (!state) {
      state = { waiting: [], active: 0, lastAt: -Infinity, draw: 0, pausedUntil: 0, strikes: 0 }
      this.keys.set(key, state)
    }
    return state
  }

  private readyAt(key: string, state: KeyState): number {
    const policy = this.policy(key)
    return state.lastAt + policy.minGapMs + state.draw * policy.jitterMs
  }

  private mark(state: KeyState): void {
    state.lastAt = Date.now()
    state.draw = this.random()
  }

  /** Starts whatever may start, and sets one timer for whatever is only waiting on the clock. */
  private pump(): void {
    const now = Date.now()
    let wakeAt = Infinity
    for (;;) {
      if (this.active >= this.maxActive) {
        break
      }
      // The ready key whose head waiter has the best claim: priority, then age.
      let best: [string, KeyState] | undefined
      for (const entry of this.keys) {
        const [key, state] = entry
        const head = state.waiting[0]
        if (!head || state.active >= this.policy(key).maxActive) {
          continue
        }
        const at = Math.max(this.readyAt(key, state), state.pausedUntil)
        if (at > now) {
          wakeAt = Math.min(wakeAt, at)
          continue
        }
        const bestHead = best?.[1].waiting[0]
        if (!bestHead || head.priority > bestHead.priority || (head.priority === bestHead.priority && head.seq < bestHead.seq)) {
          best = entry
        }
      }
      if (!best) {
        break
      }
      this.start(best[0], best[1])
    }
    this.forgetIdle(now)
    this.wakeAt(wakeAt)
  }

  private start(key: string, state: KeyState): void {
    const waiter = state.waiting.shift()!
    state.active++
    this.active++
    this.mark(state)
    let released = false
    waiter.start(() => {
      if (released) {
        return
      }
      released = true
      state.active--
      this.active--
      this.mark(state)
      this.pump()
    })
  }

  /** Drops keys with nothing to remember: idle, past their gap, unpaused and without strikes. */
  private forgetIdle(now: number): void {
    for (const [key, state] of this.keys) {
      if (
        state.waiting.length === 0 &&
        state.active === 0 &&
        state.strikes === 0 &&
        state.pausedUntil <= now &&
        this.readyAt(key, state) <= now
      ) {
        this.keys.delete(key)
      }
    }
  }

  private wakeAt(at: number): void {
    if (this.timer !== undefined && this.timerAt === at) {
      return
    }
    if (this.timer !== undefined) {
      clearTimeout(this.timer)
      this.timer = undefined
    }
    if (at === Infinity) {
      return
    }
    this.timerAt = at
    this.timer = setTimeout(() => {
      this.timer = undefined
      this.pump()
    }, Math.max(0, at - Date.now()))
  }
}

/** The host a URL is for — the usual key. Anything that is not a URL is its own key. */
export function hostOf(url: string): string {
  try {
    return new URL(url).hostname
  } catch {
    return url
  }
}

/**
 * The value `table` holds for `host` or the nearest domain above it:
 * `forHost('old.reddit.com', { 'reddit.com': 5000 })` is 5000. The longest
 * match wins, so a subdomain can be given its own.
 */
export function forHost<T>(host: string, table: Record<string, T> | Map<string, T>): T | undefined {
  const get = (name: string) => (table instanceof Map ? table.get(name) : Object.hasOwn(table, name) ? table[name] : undefined)
  for (let name = host.toLowerCase(); name; ) {
    const value = get(name)
    if (value !== undefined) {
      return value
    }
    const dot = name.indexOf('.')
    name = dot < 0 ? '' : name.slice(dot + 1)
  }
  return undefined
}

/** A `Retry-After` header as ms from now: either delay-seconds or an HTTP date. Undefined when absent or unreadable. */
export function retryAfterMs(header: string | null | undefined, now: number = Date.now()): number | undefined {
  if (!header) {
    return undefined
  }
  const trimmed = header.trim()
  if (/^\d+$/.test(trimmed)) {
    return Number(trimmed) * 1000
  }
  const at = Date.parse(trimmed)
  return Number.isNaN(at) ? undefined : Math.max(0, at - now)
}
