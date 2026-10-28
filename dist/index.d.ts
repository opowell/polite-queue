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
    minGapMs: number;
    /** Up to this many ms more, drawn afresh each time, so requests do not land on a beat. */
    jitterMs: number;
    /** Tasks on one key in flight at once. */
    maxActive: number;
    /** The first pause `backoff` imposes; each one after it doubles, until `succeed`. */
    backoffMs: number;
    /** The longest pause a run of backoffs grows to. */
    maxBackoffMs: number;
}
export declare const DEFAULT_POLICY: Policy;
export interface QueueOptions {
    /** A key's policy, over `DEFAULT_POLICY`. Read afresh every time it matters, so a changed setting applies at once. */
    policy?: (key: string) => Partial<Policy> | undefined;
    /** Tasks in flight across every key. Unlimited when not given. */
    maxActive?: number;
    /** For tests: the source of jitter, in [0, 1). */
    random?: () => number;
}
export interface RunOptions {
    /** Higher goes first among the tasks waiting on a key; equal priorities go in the order asked. */
    priority?: number;
    /** Aborting it takes a waiting task out of the queue and rejects with the signal's reason. A running task is the caller's to stop. */
    signal?: AbortSignal;
}
/** One key as `snapshot` reports it. */
export interface KeySnapshot {
    key: string;
    /** Tasks waiting for their turn. */
    waiting: number;
    /** Tasks running. */
    active: number;
    /** When the next task may start, as far as the gap and any pause say (ms since the epoch). */
    readyAt: number;
    /** Until when a backoff or `pause` holds the key, or 0. */
    pausedUntil: number;
    /** Backoffs since the last `succeed`. */
    strikes: number;
}
export declare class PoliteQueue {
    private readonly keys;
    private policyOf;
    private maxActive;
    private readonly random;
    private active;
    private seq;
    private timer;
    private timerAt;
    constructor(options?: QueueOptions);
    /** Swaps the policy source or the global cap; waiting tasks are re-judged at once. */
    configure(options: Pick<QueueOptions, 'policy' | 'maxActive'>): void;
    policy(key: string): Policy;
    /** Runs `task` when `key`'s turn comes, holding the turn until it settles. */
    run<T>(key: string, task: () => T | Promise<T>, options?: RunOptions): Promise<T>;
    /**
     * Waits for `key`'s turn and hands back the function that ends it. For a
     * caller whose work is not one promise: a worker that holds a page across
     * several navigations, or a request answered through a callback.
     */
    acquire(key: string, options?: RunOptions): Promise<() => void>;
    /**
     * The server pushed back (a 429, a 503, a bot check): nothing new starts on
     * `key` for a while. Each backoff before the next `succeed` doubles the pause,
     * and a `Retry-After` the server sent is honoured if it asks for longer.
     * Returns the pause, in ms.
     */
    backoff(key: string, retryAfterMs?: number): number;
    /** A good answer from `key`: the next backoff starts small again. */
    succeed(key: string): void;
    /** Holds `key` for `ms`, whatever its strikes. A pause of 0 lifts one in force. */
    pause(key: string, ms: number): void;
    /** Rejects every waiting task, on one key or all of them. Running tasks are left to finish. */
    clear(key?: string, reason?: unknown): void;
    /** Tasks waiting or running on `key`, or on every key. */
    pending(key?: string): number;
    /** Every key with anything waiting, running, or held. */
    snapshot(): KeySnapshot[];
    private state;
    private readyAt;
    private mark;
    /** Starts whatever may start, and sets one timer for whatever is only waiting on the clock. */
    private pump;
    private start;
    /** Drops keys with nothing to remember: idle, past their gap, unpaused and without strikes. */
    private forgetIdle;
    private wakeAt;
}
/** The host a URL is for — the usual key. Anything that is not a URL is its own key. */
export declare function hostOf(url: string): string;
/**
 * The value `table` holds for `host` or the nearest domain above it:
 * `forHost('old.reddit.com', { 'reddit.com': 5000 })` is 5000. The longest
 * match wins, so a subdomain can be given its own.
 */
export declare function forHost<T>(host: string, table: Record<string, T> | Map<string, T>): T | undefined;
/** A `Retry-After` header as ms from now: either delay-seconds or an HTTP date. Undefined when absent or unreadable. */
export declare function retryAfterMs(header: string | null | undefined, now?: number): number | undefined;
