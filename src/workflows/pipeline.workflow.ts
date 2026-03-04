import {
  condition,
  defineUpdate,
  executeChild,
  log,
  setHandler,
  sleep,
} from '@temporalio/workflow'

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

// ── Main Workflow ──

export interface PipelineInput {
  numChildren: number
  sleepMultiplier: number
}

export const startWorkUpdate = defineUpdate<void, []>('startWork')

export async function pipelineWorkflow(input: PipelineInput): Promise<void> {
  const {
    numChildren,
    sleepMultiplier,
  } = input

  let canStart = false
  setHandler(startWorkUpdate, async () => {
    return await (async () => {
      canStart = true
    })()
  })

  await Promise.all(
    Array.from({ length: numChildren }, async (_, i) => {
      await condition(() => canStart)
      await executeChild(sleepWorkflow, {
        workflowId: `sleep-${i}`,
        args: [{ sleepMultiplier }],
      })
    })
  )
}