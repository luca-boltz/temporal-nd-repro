import { condition, defineUpdate, log, setHandler } from '@temporalio/workflow'
import { MutexInterface, Mutex } from 'async-mutex'
import { ConditionMutex, FakeMutex, TriggerMutex } from './workflow-mutex'

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
}

class PauseResumeStateImpl implements PauseResumeState {
  isPaused: boolean
  private lastChange: Date
  private mutex: MutexInterface

  constructor() {
    this.lastChange = new Date(0)
    this.isPaused = false
    this.mutex = new Mutex()
    this.setupHandlers()
  }

  private setupHandlers(): void {
    setHandler(workflowControlUpdate, async (payload: WorkflowControlUpdatePayload) => {
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

  async checkAndWaitIfPaused(checkpointName: string): Promise<void> {
    if (!this.isPaused) return
    log.info('Paused at checkpoint, waiting for resume', { checkpoint: checkpointName })
    await condition(() => !this.isPaused)
    log.info('Resumed at checkpoint', { checkpoint: checkpointName })
  }
}

export function createPauseStateForParentJob(): PauseResumeState {
  const state = new PauseResumeStateImpl()
  return state
}
