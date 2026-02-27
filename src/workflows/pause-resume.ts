import { condition, defineUpdate, log, setHandler } from '@temporalio/workflow'
import { Mutex } from 'async-mutex'

// ── Signals & Updates ──

export type WorkflowControlAction = 'pause' | 'resume'

export interface WorkflowControlUpdatePayload {
  action: WorkflowControlAction
  timestamp: Date
}

export const workflowControlUpdate = defineUpdate<boolean, [WorkflowControlUpdatePayload]>('workflowControl')


// ── Pause/Resume State ──

export interface PauseResumeState {
  isPaused: boolean
  checkAndWaitIfPaused(checkpointName: string): Promise<void>
  executeAtCheckpoint<T>(checkpointName: string, fn: () => Promise<T>): Promise<T>
}

class PauseResumeStateImpl implements PauseResumeState {
  isPaused: boolean
  private lastChange: Date
  private mutex: Mutex
  private stateReady: Promise<void>
  private stateReadyResolve?: () => void

  constructor() {
    this.lastChange = new Date(0)
    this.isPaused = false
    this.mutex = new Mutex()
    this.stateReady = new Promise((resolve) => {
      this.stateReadyResolve = resolve
    })
    this.setupHandlers()
  }

  private setupHandlers(): void {
    setHandler(workflowControlUpdate, async (payload: WorkflowControlUpdatePayload) => {
      await this.stateReady
      return await this.mutex.runExclusive(async () => {
        if (payload.timestamp <= this.lastChange) {
          log.info('workflowControlUpdate stale timestamp, ignoring', {})
          return false
        }
        this.lastChange = payload.timestamp
        this.isPaused = payload.action === 'pause'
        log.info('workflowControlUpdate applied', { isPaused: this.isPaused })
        return true
      })
    })

  }

  markReady(): void {
    this.stateReadyResolve?.()
  }

  async checkAndWaitIfPaused(checkpointName: string): Promise<void> {
    await this.stateReady
    if (!this.isPaused) return
    log.info('Paused at checkpoint, waiting for resume', { checkpoint: checkpointName })
    await condition(() => !this.isPaused)
    log.info('Resumed at checkpoint', { checkpoint: checkpointName })
  }

  async executeAtCheckpoint<T>(checkpointName: string, fn: () => Promise<T>): Promise<T> {
    await this.stateReady
    while (true) {
      const iterResult: { done: true; result: T } | { done: false } = await this.mutex.runExclusive(async () => {
        if (!this.isPaused) {
          const result: T = await fn()
          return { done: true, result }
        } else {
          return { done: false }
        }
      })
      if (iterResult.done) {
        return iterResult.result
      }
      await this.checkAndWaitIfPaused(checkpointName)
    }
  }
}

// In non-parent-job workflows, additional setup is needed between instantiation
// and readiness (e.g. setting a jobId used for status tracking). That scenario
// is omitted here for simplicity, so we call markReady() immediately.
export function createPauseStateForParentJob(): PauseResumeState {
  const state = new PauseResumeStateImpl()
  state.markReady()
  return state
}
