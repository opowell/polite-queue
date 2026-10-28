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
export const DEFAULT_POLICY = {
    minGapMs: 1_000,
    jitterMs: 0,
    maxActive: 1,
    backoffMs: 30_000,
    maxBackoffMs: 10 * 60_000,
};
export class PoliteQueue {
    keys = new Map();
    policyOf;
    maxActive;
    random;
    active = 0;
    seq = 0;
    timer;
    timerAt = 0;
    constructor(options = {}) {
        this.policyOf = options.policy ?? (() => undefined);
        this.maxActive = options.maxActive ?? Infinity;
        this.random = options.random ?? Math.random;
    }
    /** Swaps the policy source or the global cap; waiting tasks are re-judged at once. */
    configure(options) {
        if (options.policy) {
            this.policyOf = options.policy;
        }
        if (options.maxActive !== undefined) {
            this.maxActive = options.maxActive;
        }
        this.pump();
    }
    policy(key) {
        const own = this.policyOf(key) ?? {};
        const policy = { ...DEFAULT_POLICY };
        for (const name of Object.keys(policy)) {
            const value = own[name];
            if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
                policy[name] = value;
            }
        }
        policy.maxActive = Math.max(1, Math.floor(policy.maxActive));
        return policy;
    }
    /** Runs `task` when `key`'s turn comes, holding the turn until it settles. */
    async run(key, task, options) {
        const release = await this.acquire(key, options);
        try {
            return await task();
        }
        finally {
            release();
        }
    }
    /**
     * Waits for `key`'s turn and hands back the function that ends it. For a
     * caller whose work is not one promise: a worker that holds a page across
     * several navigations, or a request answered through a callback.
     */
    acquire(key, options = {}) {
        const signal = options.signal;
        if (signal?.aborted) {
            return Promise.reject(signal.reason);
        }
        return new Promise((resolve, reject) => {
            const state = this.state(key);
            const onAbort = () => {
                const at = state.waiting.indexOf(waiter);
                if (at >= 0) {
                    state.waiting.splice(at, 1);
                    reject(signal.reason);
                    this.pump();
                }
            };
            const waiter = {
                priority: options.priority ?? 0,
                seq: this.seq++,
                start: (release) => {
                    signal?.removeEventListener('abort', onAbort);
                    resolve(release);
                },
                fail: (reason) => {
                    signal?.removeEventListener('abort', onAbort);
                    reject(reason);
                },
            };
            signal?.addEventListener('abort', onAbort, { once: true });
            // Kept sorted: highest priority first, then oldest.
            let at = state.waiting.length;
            while (at > 0 && state.waiting[at - 1].priority < waiter.priority) {
                at--;
            }
            state.waiting.splice(at, 0, waiter);
            this.pump();
        });
    }
    /**
     * The server pushed back (a 429, a 503, a bot check): nothing new starts on
     * `key` for a while. Each backoff before the next `succeed` doubles the pause,
     * and a `Retry-After` the server sent is honoured if it asks for longer.
     * Returns the pause, in ms.
     */
    backoff(key, retryAfterMs) {
        const state = this.state(key);
        const policy = this.policy(key);
        state.strikes++;
        const doubled = policy.backoffMs * 2 ** Math.min(state.strikes - 1, 30);
        const ms = Math.max(Math.min(doubled, policy.maxBackoffMs), retryAfterMs ?? 0);
        state.pausedUntil = Math.max(state.pausedUntil, Date.now() + ms);
        this.pump();
        return ms;
    }
    /** A good answer from `key`: the next backoff starts small again. */
    succeed(key) {
        const state = this.keys.get(key);
        if (state) {
            state.strikes = 0;
        }
    }
    /** Holds `key` for `ms`, whatever its strikes. A pause of 0 lifts one in force. */
    pause(key, ms) {
        const state = this.state(key);
        state.pausedUntil = ms > 0 ? Math.max(state.pausedUntil, Date.now() + ms) : 0;
        this.pump();
    }
    /** Rejects every waiting task, on one key or all of them. Running tasks are left to finish. */
    clear(key, reason = new Error('Cleared from the queue')) {
        const entries = key === undefined ? [...this.keys] : [[key, this.keys.get(key)]];
        for (const [, state] of entries) {
            for (const waiter of state?.waiting.splice(0) ?? []) {
                waiter.fail(reason);
            }
        }
        this.pump();
    }
    /** Tasks waiting or running on `key`, or on every key. */
    pending(key) {
        if (key !== undefined) {
            const state = this.keys.get(key);
            return state ? state.waiting.length + state.active : 0;
        }
        let count = 0;
        for (const state of this.keys.values()) {
            count += state.waiting.length + state.active;
        }
        return count;
    }
    /** Every key with anything waiting, running, or held. */
    snapshot() {
        return [...this.keys].map(([key, state]) => ({
            key,
            waiting: state.waiting.length,
            active: state.active,
            readyAt: Math.max(this.readyAt(key, state), state.pausedUntil),
            pausedUntil: state.pausedUntil,
            strikes: state.strikes,
        }));
    }
    state(key) {
        let state = this.keys.get(key);
        if (!state) {
            state = { waiting: [], active: 0, lastAt: -Infinity, draw: 0, pausedUntil: 0, strikes: 0 };
            this.keys.set(key, state);
        }
        return state;
    }
    readyAt(key, state) {
        const policy = this.policy(key);
        return state.lastAt + policy.minGapMs + state.draw * policy.jitterMs;
    }
    mark(state) {
        state.lastAt = Date.now();
        state.draw = this.random();
    }
    /** Starts whatever may start, and sets one timer for whatever is only waiting on the clock. */
    pump() {
        const now = Date.now();
        let wakeAt = Infinity;
        for (;;) {
            if (this.active >= this.maxActive) {
                break;
            }
            // The ready key whose head waiter has the best claim: priority, then age.
            let best;
            for (const entry of this.keys) {
                const [key, state] = entry;
                const head = state.waiting[0];
                if (!head || state.active >= this.policy(key).maxActive) {
                    continue;
                }
                const at = Math.max(this.readyAt(key, state), state.pausedUntil);
                if (at > now) {
                    wakeAt = Math.min(wakeAt, at);
                    continue;
                }
                const bestHead = best?.[1].waiting[0];
                if (!bestHead || head.priority > bestHead.priority || (head.priority === bestHead.priority && head.seq < bestHead.seq)) {
                    best = entry;
                }
            }
            if (!best) {
                break;
            }
            this.start(best[0], best[1]);
        }
        this.forgetIdle(now);
        this.wakeAt(wakeAt);
    }
    start(key, state) {
        const waiter = state.waiting.shift();
        state.active++;
        this.active++;
        this.mark(state);
        let released = false;
        waiter.start(() => {
            if (released) {
                return;
            }
            released = true;
            state.active--;
            this.active--;
            this.mark(state);
            this.pump();
        });
    }
    /** Drops keys with nothing to remember: idle, past their gap, unpaused and without strikes. */
    forgetIdle(now) {
        for (const [key, state] of this.keys) {
            if (state.waiting.length === 0 &&
                state.active === 0 &&
                state.strikes === 0 &&
                state.pausedUntil <= now &&
                this.readyAt(key, state) <= now) {
                this.keys.delete(key);
            }
        }
    }
    wakeAt(at) {
        if (this.timer !== undefined && this.timerAt === at) {
            return;
        }
        if (this.timer !== undefined) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        if (at === Infinity) {
            return;
        }
        this.timerAt = at;
        this.timer = setTimeout(() => {
            this.timer = undefined;
            this.pump();
        }, Math.max(0, at - Date.now()));
    }
}
/** The host a URL is for — the usual key. Anything that is not a URL is its own key. */
export function hostOf(url) {
    try {
        return new URL(url).hostname;
    }
    catch {
        return url;
    }
}
/**
 * The value `table` holds for `host` or the nearest domain above it:
 * `forHost('old.reddit.com', { 'reddit.com': 5000 })` is 5000. The longest
 * match wins, so a subdomain can be given its own.
 */
export function forHost(host, table) {
    const get = (name) => (table instanceof Map ? table.get(name) : Object.hasOwn(table, name) ? table[name] : undefined);
    for (let name = host.toLowerCase(); name;) {
        const value = get(name);
        if (value !== undefined) {
            return value;
        }
        const dot = name.indexOf('.');
        name = dot < 0 ? '' : name.slice(dot + 1);
    }
    return undefined;
}
/** A `Retry-After` header as ms from now: either delay-seconds or an HTTP date. Undefined when absent or unreadable. */
export function retryAfterMs(header, now = Date.now()) {
    if (!header) {
        return undefined;
    }
    const trimmed = header.trim();
    if (/^\d+$/.test(trimmed)) {
        return Number(trimmed) * 1000;
    }
    const at = Date.parse(trimmed);
    return Number.isNaN(at) ? undefined : Math.max(0, at - now);
}
