import { condition, Trigger } from '@temporalio/workflow'
import type { MutexInterface } from 'async-mutex'
import Denque from 'denque'

const E_CANCELED = new Error('request for lock canceled')

export class FakeMutex implements MutexInterface {
  async acquire(_priority?: number): Promise<MutexInterface.Releaser> {
    return () => {}
  }

  async runExclusive<T>(callback: MutexInterface.Worker<T>, _priority?: number): Promise<T> {
    return await callback()
  }

  async waitForUnlock(_priority?: number): Promise<void> {}

  isLocked(): boolean {
    return false
  }

  release(): void {}

  cancel(): void {}
}

export class TriggerMutex implements MutexInterface {
  private _locked = false
  private _queue = new Denque<Trigger<void>>()
  private _unlockWaiters: Trigger<void>[] = []

  async acquire(_priority?: number): Promise<MutexInterface.Releaser> {
    if (!this._locked) {
      this._locked = true
      return this._newReleaser()
    }

    const trigger = new Trigger<void>()
    this._queue.push(trigger)
    await trigger

    // Lock ownership was passed to us by the releaser — _locked is still true
    return this._newReleaser()
  }

  async runExclusive<T>(callback: MutexInterface.Worker<T>, _priority?: number): Promise<T> {
    const release = await this.acquire()
    try {
      return await callback()
    } finally {
      release()
    }
  }

  async waitForUnlock(_priority?: number): Promise<void> {
    if (!this._locked) return

    const trigger = new Trigger<void>()
    this._unlockWaiters.push(trigger)
    await trigger
  }

  isLocked(): boolean {
    return this._locked
  }

  release(): void {
    if (this._locked) {
      this._dispatch()
    }
  }

  cancel(): void {
    const queue = this._queue
    this._queue = new Denque<Trigger<void>>()
    while (!queue.isEmpty()) {
      queue.shift()!.reject(E_CANCELED)
    }
  }

  private _newReleaser(): MutexInterface.Releaser {
    let called = false
    return () => {
      if (called) return
      called = true
      this._dispatch()
    }
  }

  private _dispatch(): void {
    const next = this._queue.shift()
    if (next) {
      // Pass lock directly to next waiter — _locked stays true
      next.resolve()
    } else {
      this._locked = false
      const waiters = this._unlockWaiters
      this._unlockWaiters = []
      for (const t of waiters) {
        t.resolve()
      }
    }
  }
}

export class ConditionMutex implements MutexInterface {
  private _held = false
  private _turn = 0
  private _nextTicket = 0
  private _generation = 0

  async acquire(_priority?: number): Promise<MutexInterface.Releaser> {
    const myTicket = this._nextTicket++
    const myGeneration = this._generation

    await condition(
      () => (this._turn === myTicket && !this._held) || this._generation !== myGeneration,
    )

    if (this._generation !== myGeneration) {
      throw E_CANCELED
    }

    this._held = true
    return this._newReleaser()
  }

  async runExclusive<T>(callback: MutexInterface.Worker<T>, _priority?: number): Promise<T> {
    const release = await this.acquire()
    try {
      return await callback()
    } finally {
      release()
    }
  }

  async waitForUnlock(_priority?: number): Promise<void> {
    if (!this._held) return
    await condition(() => !this._held)
  }

  isLocked(): boolean {
    return this._held
  }

  release(): void {
    if (this._held) {
      this._held = false
      this._turn++
    }
  }

  cancel(): void {
    this._generation++
    this._turn = 0
    this._nextTicket = 0
  }

  private _newReleaser(): MutexInterface.Releaser {
    let called = false
    return () => {
      if (called) return
      called = true
      this.release()
    }
  }
}
