import { Connection, Client, WorkflowNotFoundError } from '@temporalio/client'
import { setTimeout } from 'node:timers/promises'
import { workflowControlUpdate, type WorkflowControlUpdatePayload } from './workflows/pause-resume'
import { pipelineWorkflow, type PipelineInput } from './workflows/pipeline.workflow'

async function runInterestingScenario(client:Client) {

  const workflowId = `nd-repro-pipeline`

  const input: PipelineInput = {
    numChildren: 200,
    queueConcurrency: 10,
    sleepMultiplier: 0.1,
  }

  // Start the pipeline
  try {
    await client.workflow.start(pipelineWorkflow, {
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

  // Toggle pause/resume repeatedly
  const handle = client.workflow.getHandle(workflowId)
  let nextAction: 'pause' | 'resume' = 'pause'
  const intervalMs = 10_000 * input.sleepMultiplier

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

async function main() {
  const connection = await Connection.connect({ address: 'localhost:7233' })
  const client = new Client({ connection, namespace: 'default' })
  await runInterestingScenario(client)
}

main().catch((err) => {
  console.error('Client failed', err)
  process.exit(1)
})
