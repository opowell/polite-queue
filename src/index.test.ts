import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { forHost, hostOf, PoliteQueue, retryAfterMs } from './index'

/** A task that runs until `finish` is called, logging when it started. */
function held(log: string[], name: string) {
  let finish!: () => void
  const done = new Promise<void>((resolve) => {
    finish = resolve
  })
  return {
    task: () => {
      log.push(`${name}@${Date.now()}`)
      return done
    },
    finish: () => finish(),
  }
}

beforeEach(() => {
  vi.useFakeTimers({ now: 0 })
})

afterEach(() => {
  vi.useRealTimers()
})

describe('gap between tasks on one key', () => {
  it('starts the first at once and the next a gap after the first finishes', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 1000 }) })
    const log: string[] = []
    const a = held(log, 'a')
    const b = held(log, 'b')
    void queue.run('x', a.task)
    void queue.run('x', b.task)
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0'])

    await vi.advanceTimersByTimeAsync(500)
    a.finish()
    await vi.advanceTimersByTimeAsync(999)
    expect(log).toEqual(['a@0'])
    await vi.advanceTimersByTimeAsync(1)
    expect(log).toEqual(['a@0', 'b@1500'])
  })

  it('counts the gap from the last start when tasks overlap', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 1000, maxActive: 3 }) })
    const log: string[] = []
    for (const name of ['a', 'b', 'c']) {
      void queue.run('x', held(log, name).task)
    }
    await vi.advanceTimersByTimeAsync(2500)
    expect(log).toEqual(['a@0', 'b@1000', 'c@2000'])
  })

  it('keeps keys apart', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 1000 }) })
    const log: string[] = []
    void queue.run('x', held(log, 'x').task)
    void queue.run('y', held(log, 'y').task)
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['x@0', 'y@0'])
  })

  it('adds jitter on top of the gap', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 1000, jitterMs: 400 }), random: () => 0.5 })
    const log: string[] = []
    await queue.run('x', () => {})
    void queue.run('x', held(log, 'b').task)
    await vi.advanceTimersByTimeAsync(1199)
    expect(log).toEqual([])
    await vi.advanceTimersByTimeAsync(1)
    expect(log).toEqual(['b@1200'])
  })

  it('applies a changed policy to tasks already waiting', async () => {
    let gap = 60_000
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: gap }) })
    const log: string[] = []
    await queue.run('x', () => {})
    void queue.run('x', held(log, 'b').task)
    await vi.advanceTimersByTimeAsync(1000)
    expect(log).toEqual([])
    gap = 500
    queue.configure({})
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['b@1000'])
  })
})

describe('global cap', () => {
  it('holds tasks on other keys until a slot frees', async () => {
    const queue = new PoliteQueue({ maxActive: 1, policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    const a = held(log, 'a')
    void queue.run('x', a.task)
    void queue.run('y', held(log, 'b').task)
    await vi.advanceTimersByTimeAsync(100)
    expect(log).toEqual(['a@0'])
    a.finish()
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0', 'b@100'])
  })

  it('gives a freed slot to the longest waiter across keys', async () => {
    const queue = new PoliteQueue({ maxActive: 1, policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    const a = held(log, 'a')
    void queue.run('x', a.task)
    void queue.run('y', held(log, 'y1').task)
    void queue.run('x', held(log, 'x2').task)
    await vi.advanceTimersByTimeAsync(0)
    a.finish()
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0', 'y1@0'])
  })

  it('raising the cap starts waiting tasks', async () => {
    const queue = new PoliteQueue({ maxActive: 1, policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    void queue.run('x', held(log, 'a').task)
    void queue.run('y', held(log, 'b').task)
    await vi.advanceTimersByTimeAsync(0)
    queue.configure({ maxActive: 2 })
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0', 'b@0'])
  })
})

describe('priority', () => {
  it('lets a higher priority jump the waiting line on its key', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    const a = held(log, 'a')
    void queue.run('x', a.task)
    void queue.run('x', held(log, 'low').task)
    void queue.run('x', held(log, 'high').task, { priority: 1 })
    await vi.advanceTimersByTimeAsync(0)
    a.finish()
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0', 'high@0'])
  })
})

describe('backoff', () => {
  it('pauses a key, doubling each time until a success', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0, backoffMs: 1000, maxBackoffMs: 3000 }) })
    expect(queue.backoff('x')).toBe(1000)
    expect(queue.backoff('x')).toBe(2000)
    expect(queue.backoff('x')).toBe(3000)
    queue.succeed('x')
    expect(queue.backoff('x')).toBe(1000)
  })

  it('honours a longer Retry-After', () => {
    const queue = new PoliteQueue({ policy: () => ({ backoffMs: 1000 }) })
    expect(queue.backoff('x', 5000)).toBe(5000)
  })

  it('holds waiting tasks until the pause is over', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0, backoffMs: 1000 }) })
    const log: string[] = []
    queue.backoff('x')
    void queue.run('x', held(log, 'a').task)
    void queue.run('y', held(log, 'b').task)
    await vi.advanceTimersByTimeAsync(999)
    expect(log).toEqual(['b@0'])
    await vi.advanceTimersByTimeAsync(1)
    expect(log).toEqual(['b@0', 'a@1000'])
  })

  it('lifts a pause of 0', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    queue.pause('x', 60_000)
    void queue.run('x', held(log, 'a').task)
    await vi.advanceTimersByTimeAsync(10)
    queue.pause('x', 0)
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@10'])
  })
})

describe('settling', () => {
  it('passes on what the task returns or throws, and frees the turn either way', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0 }) })
    await expect(queue.run('x', () => 42)).resolves.toBe(42)
    await expect(
      queue.run('x', () => {
        throw new Error('boom')
      }),
    ).rejects.toThrow('boom')
    expect(queue.pending()).toBe(0)
  })

  it('takes an aborted waiter out of the line', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    const a = held(log, 'a')
    void queue.run('x', a.task)
    const controller = new AbortController()
    const aborted = queue.run('x', held(log, 'b').task, { signal: controller.signal })
    void queue.run('x', held(log, 'c').task)
    controller.abort(new Error('gone'))
    await expect(aborted).rejects.toThrow('gone')
    a.finish()
    await vi.advanceTimersByTimeAsync(0)
    expect(log).toEqual(['a@0', 'c@0'])
  })

  it('clear rejects waiters and leaves running tasks be', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 0 }) })
    const log: string[] = []
    const a = held(log, 'a')
    const running = queue.run('x', a.task)
    const waiting = queue.run('x', held(log, 'b').task)
    queue.clear()
    await expect(waiting).rejects.toThrow('Cleared')
    expect(queue.pending('x')).toBe(1)
    a.finish()
    await running
    expect(queue.pending()).toBe(0)
  })

  it('acquire hands back an idempotent release', async () => {
    const queue = new PoliteQueue({ maxActive: 1, policy: () => ({ minGapMs: 0 }) })
    const release = await queue.acquire('x')
    release()
    release()
    expect(queue.snapshot().reduce((sum, key) => sum + key.active, 0)).toBe(0)
  })

  it('forgets a key once its gap has passed', async () => {
    const queue = new PoliteQueue({ policy: () => ({ minGapMs: 1000 }) })
    await queue.run('x', () => {})
    expect(queue.snapshot().map((key) => key.key)).toEqual(['x'])
    await vi.advanceTimersByTimeAsync(1000)
    void queue.run('y', () => {})
    await vi.advanceTimersByTimeAsync(0)
    expect(queue.snapshot().map((key) => key.key)).toEqual(['y'])
  })
})

describe('helpers', () => {
  it('hostOf', () => {
    expect(hostOf('https://store.bricklink.com/x?y')).toBe('store.bricklink.com')
    expect(hostOf('not a url')).toBe('not a url')
  })

  it('forHost matches the nearest domain', () => {
    const table = { 'reddit.com': 5000, 'old.reddit.com': 9000 }
    expect(forHost('www.reddit.com', table)).toBe(5000)
    expect(forHost('old.reddit.com', table)).toBe(9000)
    expect(forHost('example.com', table)).toBeUndefined()
    expect(forHost('a.reddit.com', new Map([['reddit.com', 1]]))).toBe(1)
  })

  it('retryAfterMs reads seconds and dates', () => {
    expect(retryAfterMs('120')).toBe(120_000)
    expect(retryAfterMs(new Date(30_000).toUTCString(), 0)).toBe(30_000)
    expect(retryAfterMs('soon')).toBeUndefined()
    expect(retryAfterMs(null)).toBeUndefined()
  })
})
