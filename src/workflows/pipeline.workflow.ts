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

// ── Sleep Workflow ──

export interface SleepInput {
  sleepMultiplier: number
}

export async function sleepWorkflow(input: SleepInput): Promise<void> {
    const sleepSeconds = sampleLogNormal(30, 50) * input.sleepMultiplier
    log.info('Child sleeping', { sleepSeconds: Math.round(sleepSeconds) })
    await sleep(`${Math.round(sleepSeconds)} seconds`)
}

export interface PipelineInput {
  parentJobId: string
  numChildren: number
  queueConcurrency: number
  sleepMultiplier: number
}


export async function pipelineWorkflow(input: PipelineInput): Promise<void> {
  const pauseResumeState = createPauseStateForParentJob()
  const {
    parentJobId,
    numChildren,
    queueConcurrency,
    sleepMultiplier,
  } = input
  const queue = new PQueue({ concurrency: queueConcurrency })
  const workflowSuffix = uuid4()
  for (let i = 0; i < numChildren; i++) {
    queue.add(
      async () => {
        await executeTioChild(
          pauseResumeState,
          sleepWorkflow,
          `sleep-${i}-parent-${parentJobId}-${workflowSuffix}`,
          [{ sleepMultiplier }],
        )
      },
    )
  }
  await queue.onIdle()
}