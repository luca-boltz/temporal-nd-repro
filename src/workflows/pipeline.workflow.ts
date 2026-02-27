import {
  allHandlersFinished,
  condition,
  continueAsNew,
  log,
  sleep,
  uuid4,
} from '@temporalio/workflow'
import PQueue from 'p-queue'
import { createPauseStateForParentJob } from './pause-resume'
import { PQueueFireAndForgetSafe, type PQueueLike, SimplePQueue } from './simple-p-queue'
import { executeTioChild } from './temporal-utils'

// ── Helpers ──

function sampleLogNormal(p50Seconds: number, p99Seconds: number): number {
  const u1 = Math.random()
  const u2 = Math.random()
  const z = Math.sqrt(-2 * Math.log(Math.max(u1, 1e-10))) * Math.cos(2 * Math.PI * u2)
  const mu = Math.log(p50Seconds)
  const sigma = (Math.log(p99Seconds) - mu) / 2.326
  const sample = Math.exp(mu + sigma * z)
  return Math.max(p50Seconds * 0.5, Math.min(sample, p99Seconds * 1.5))
}

// ── Child Workflows ──

export interface ChildInput {
  parentJobId: string
  sleepMultiplier: number
}

export async function alphaWorkflow(input: ChildInput): Promise<void> {
  const pauseResumeState = createPauseStateForParentJob()

  try {
    await pauseResumeState.checkAndWaitIfPaused('before-alpha')

    const sleepSeconds = sampleLogNormal(180, 252) * input.sleepMultiplier
    log.info('Alpha sleeping', { sleepSeconds: Math.round(sleepSeconds) })
    await sleep(`${Math.round(sleepSeconds)} seconds`)
    log.info('Alpha completed', {})
  } finally {
    await condition(allHandlersFinished)
  }
}

export async function betaWorkflow(input: ChildInput): Promise<void> {
  const pauseResumeState = createPauseStateForParentJob()

  try {
    await pauseResumeState.checkAndWaitIfPaused('before-beta')

    const sleepSeconds = sampleLogNormal(30, 50) * input.sleepMultiplier
    log.info('Beta sleeping', { sleepSeconds: Math.round(sleepSeconds) })
    await sleep(`${Math.round(sleepSeconds)} seconds`)
    log.info('Beta completed', {})
  } finally {
    await condition(allHandlersFinished)
  }
}

// ── Pipeline Workflow ──

const BATCHES_BEFORE_CONTINUE_AS_NEW = 50

const JOB_PRIORITY = {
  BETA: 10,
  ALPHA: 1,
} as const

export type QueueType = 'simple' | 'p-queue'

export interface PipelineInput {
  parentJobId: string
  totalBatches: number
  betasPerBatch: number
  queueConcurrency: number
  sleepMultiplier: number
  queueType?: QueueType
  useFireAndForgetSafe?: boolean
  continuationData?: {
    batchesCompleted: number
    totalAlphas: number
    totalBetas: number
  }
}

export interface PipelineResult {
  status: 'completed'
  totalAlphas: number
  totalBetas: number
}

export async function pipelineWorkflow(input: PipelineInput): Promise<PipelineResult> {
  const pauseResumeState = createPauseStateForParentJob()

  try {
    const {
      parentJobId,
      totalBatches,
      betasPerBatch,
      queueConcurrency,
      sleepMultiplier,
      continuationData,
    } = input

    const batchesAlreadyCompleted = continuationData?.batchesCompleted ?? 0
    let totalAlphas = continuationData?.totalAlphas ?? 0
    let totalBetas = continuationData?.totalBetas ?? 0

    const remainingBatches = totalBatches - batchesAlreadyCompleted
    if (remainingBatches <= 0) {
      return { status: 'completed', totalAlphas, totalBetas }
    }

    const batchesToRunThisIteration = Math.min(remainingBatches, BATCHES_BEFORE_CONTINUE_AS_NEW)

    log.info('Pipeline iteration', {
      batchesAlreadyCompleted,
      batchesToRunThisIteration,
      remainingBatches,
      totalBatches,
    })

    const queueType = input.queueType ?? 'simple'
    const useFireAndForgetSafe = input.useFireAndForgetSafe ?? true

    const rawQueue: PQueueLike =
      queueType === 'simple'
        ? new SimplePQueue({ concurrency: queueConcurrency })
        : new PQueue({ concurrency: queueConcurrency })

    const queue = useFireAndForgetSafe ? PQueueFireAndForgetSafe(rawQueue) : rawQueue

    log.info('Queue created', { queueType, useFireAndForgetSafe })
    const workflowSuffix = uuid4()

    for (let i = 0; i < batchesToRunThisIteration; i++) {
      const batchIndex = batchesAlreadyCompleted + i

      queue.add(
        async () => {
          await executeTioChild(
            pauseResumeState,
            alphaWorkflow,
            `sim-alpha-${batchIndex}-parent-${parentJobId}-${workflowSuffix}`,
            [{ parentJobId, sleepMultiplier }],
          )
          totalAlphas++

          for (let betaIndex = 0; betaIndex < betasPerBatch; betaIndex++) {
            queue.add(
              async () => {
                await executeTioChild(
                  pauseResumeState,
                  betaWorkflow,
                  `sim-beta-${batchIndex}-${betaIndex}-parent-${parentJobId}-${workflowSuffix}`,
                  [{ parentJobId, sleepMultiplier }],
                )
                totalBetas++
              },
              { priority: JOB_PRIORITY.BETA },
            )
          }
        },
        { priority: JOB_PRIORITY.ALPHA },
      )
    }

    await queue.onIdle()

    const newBatchesCompleted = batchesAlreadyCompleted + batchesToRunThisIteration
    const shouldContinue = newBatchesCompleted < totalBatches

    if (shouldContinue) {
      log.info('Pipeline continuing as new', {
        batchesCompleted: newBatchesCompleted,
        totalBatches,
        totalAlphas,
        totalBetas,
      })
      await continueAsNew<typeof pipelineWorkflow>({
        ...input,
        continuationData: {
          batchesCompleted: newBatchesCompleted,
          totalAlphas,
          totalBetas,
        },
      })
    }

    log.info('Pipeline completed', { totalAlphas, totalBetas })
    return { status: 'completed', totalAlphas, totalBetas }
  } finally {
    await condition(allHandlersFinished)
  }
}
