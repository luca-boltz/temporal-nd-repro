import { Connection, Client, WorkflowNotFoundError } from '@temporalio/client'
import { createHash } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { workflowControlUpdate, type WorkflowControlUpdatePayload } from './workflows/pause-resume'
import type { PipelineInput } from './workflows/pipeline.workflow'

function deterministicUuid(seed: string): string {
  const hash = createHash('sha256').update(seed).digest('hex')
  return [hash.slice(0, 8), hash.slice(8, 12), hash.slice(12, 16), hash.slice(16, 20), hash.slice(20, 32)].join('-')
}

async function main() {
  const connection = await Connection.connect({ address: 'localhost:7233' })
  const client = new Client({ connection, namespace: 'default' })

  const parentJobId = deterministicUuid('nd-repro:parentJob:0')
  const workflowId = `nd-repro-pipeline-${parentJobId}`

  const input: PipelineInput = {
    parentJobId,
    totalBatches: 200,
    betasPerBatch: 10,
    queueConcurrency: 32,
    sleepMultiplier: 0.5,
  }

  // Start the pipeline
  try {
    await client.workflow.start('pipelineWorkflow', {
      workflowId,
      taskQueue: 'default',
      args: [input],
    })
    console.log(`Started pipeline: ${workflowId}`)
  } catch (error) {
    if (error instanceof Error && error.message.includes('already started')) {
      console.log(`Pipeline already running: ${workflowId}`)
    } else {
      throw error
    }
  }

  // Toggle pause/resume every 30s
  const handle = client.workflow.getHandle(workflowId)
  let nextAction: 'pause' | 'resume' = 'pause'
  const intervalMs = 30_000

  console.log(`Toggling pause/resume every ${intervalMs / 1000}s...`)

  while (true) {
    await setTimeout(intervalMs)

    const payload: WorkflowControlUpdatePayload = { action: nextAction, timestamp: new Date() }
    try {
      const written = await handle.executeUpdate(workflowControlUpdate, { args: [payload] })
      console.log(`Sent ${nextAction} → accepted: ${written}`)
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) {
        console.log('Workflow completed, exiting')
        return
      }
      console.error(`Update failed (${nextAction})`, error)
      return
    }

    nextAction = nextAction === 'pause' ? 'resume' : 'pause'
  }
}

main().catch((err) => {
  console.error('Client failed', err)
  process.exit(1)
})
