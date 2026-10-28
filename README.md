# polite-queue

Per-domain request scheduling, for fetching other people's pages without hammering them.

Work is filed under a key (usually the host), and each key gets:

- **a gap** — the least time from the latest start or finish on that key to the next start, plus optional random **jitter**;
- **a concurrency cap** for the key, and a **global cap** across all keys;
- **backoff** — when a server answers 429/503 or a bot check, the key pauses, doubling on each repeat until a good answer, and a `Retry-After` is honoured;
- **priority** — an interactive request can go ahead of background fill on the same key.

Nothing here fetches anything. A task is whatever the caller does with its turn: a `fetch`, a browser tab load, a Playwright navigation. Zero dependencies, plain ESM, so `dist/index.js` can be imported as-is by an extension service worker.

## Use

```ts
import { PoliteQueue, forHost, hostOf, retryAfterMs } from 'polite-queue'

const gaps = { 'bricklink.com': 1500, 'lego.com': 2000 }
const queue = new PoliteQueue({
  maxActive: 3, // across every host
  policy: (host) => ({ minGapMs: forHost(host, gaps) ?? 1000, jitterMs: 500, maxActive: 1 }),
})

const response = await queue.run(hostOf(url), () => fetch(url))
if (response.status === 429 || response.status === 503) {
  queue.backoff(hostOf(url), retryAfterMs(response.headers.get('Retry-After')))
} else {
  queue.succeed(hostOf(url))
}
```

`policy` is read every time it matters, so a changed setting applies to requests already waiting; call `queue.configure({})` to re-judge them at once.

When a turn is not one promise — a worker that keeps a page open across navigations, a request answered through a callback — take the turn with `acquire` and end it yourself:

```ts
const release = await queue.acquire(host, { priority: 1, signal })
try { /* ... */ } finally { release() }
```

## API

| | |
|---|---|
| `new PoliteQueue({ policy?, maxActive?, random? })` | `policy(key)` returns a partial `Policy` over `DEFAULT_POLICY` |
| `run(key, task, { priority?, signal? })` | runs `task` in `key`'s turn |
| `acquire(key, options)` | waits for the turn, resolves with `release()` |
| `backoff(key, retryAfterMs?)` | pauses `key`; returns the pause in ms |
| `succeed(key)` | resets `key`'s backoff |
| `pause(key, ms)` | holds `key` for `ms`; `0` lifts a pause |
| `clear(key?, reason?)` | rejects waiting tasks |
| `configure({ policy?, maxActive? })` | swaps the policy or global cap |
| `pending(key?)`, `snapshot()` | what is waiting, running and held |
| `hostOf(url)`, `forHost(host, table)`, `retryAfterMs(header)` | helpers |

`Policy`: `minGapMs` (1000), `jitterMs` (0), `maxActive` (1), `backoffMs` (30 000), `maxBackoffMs` (600 000).

## Develop

```sh
pnpm install
pnpm test
pnpm build   # dist/ is committed, so consumers can install from a git tag
```
