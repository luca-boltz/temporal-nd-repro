import { condition, Trigger } from '@temporalio/workflow'
import Denque from 'denque'

export interface PQueueLike {
  add<T>(function_: () => PromiseLike<T>, options?: { priority?: number }): Promise<T>
  onIdle(): Promise<void>
}

class PriorityQueue<T> {
  private queues: Map<number, Denque<T>>
  private size: number

  constructor() {
    this.queues = new Map<number, Denque<T>>()
    this.size = 0
  }

  get length(): number {
    return this.size
  }

  private getQueue(priority: number) {
    const existing = this.queues.get(priority)
    if (existing !== undefined) return existing
    const created = new Denque<T>()
    this.queues.set(priority, created)
    return created
  }

  push(x: T, priority: number) {
    this.getQueue(priority).push(x)
    this.size++
  }

  pop(): T | undefined {
    let maxPriority = -Infinity
    let highestQueue: Denque<T> | undefined

    for (const [priority, queue] of this.queues) {
      if (queue.length > 0 && priority >= maxPriority) {
        maxPriority = priority
        highestQueue = queue
      }
    }

    if (!highestQueue) {
      return undefined
    }

    const item = highestQueue.shift()
    this.size--

    if (highestQueue.length === 0) {
      this.queues.delete(maxPriority)
    }

    return item
  }
}

interface TaskTicket {
  priority: number
  startTrigger: Trigger<void>
}

export class SimplePQueue implements PQueueLike {
  private queue: PriorityQueue<TaskTicket>
  private running = 0
  private concurrency: number

  constructor(options: { concurrency?: number }) {
    this.queue = new PriorityQueue<TaskTicket>()
    this.concurrency = options?.concurrency ?? Number.POSITIVE_INFINITY
  }

  add<T>(function_: () => PromiseLike<T>, options?: { priority?: number }): Promise<T> {
    const priority = options?.priority ?? 0
    const startTrigger = new Trigger<void>()

    this.queue.push({ priority, startTrigger }, priority)
    this.processQueue()

    return (async () => {
      await startTrigger

      try {
        return await function_()
      } finally {
        this.running--
        this.processQueue()
      }
    })()
  }

  async onIdle(): Promise<void> {
    await condition(() => this.running === 0 && this.queue.length === 0)
  }

  private processQueue(): void {
    while (this.queue.length > 0 && this.running < this.concurrency) {
      this.running++
      const job = this.queue.pop()
      job?.startTrigger.resolve()
    }
  }
}

export function PQueueFireAndForgetSafe(queue: PQueueLike): PQueueLike {
  const results: PromiseSettledResult<unknown>[] = []
  const promises: Promise<void>[] = []

  function add<T>(fn: () => PromiseLike<T>, options?: { priority?: number }): Promise<T> {
    const originalPromise = queue.add(fn, options)
    promises.push(
      originalPromise.then(
        (value) => {
          results.push({ status: 'fulfilled', value })
        },
        (reason) => {
          results.push({ status: 'rejected', reason })
        },
      ),
    )
    return originalPromise
  }

  async function onIdle(): Promise<void> {
    await queue.onIdle()
    await Promise.all(promises)

    const errors = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected').map((r) => r.reason)

    results.length = 0
    promises.length = 0

    if (errors.length > 0) {
      throw errors[0]
    }
  }

  return { onIdle, add }
}
