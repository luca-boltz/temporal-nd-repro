import { Connection, Client } from '@temporalio/client'
import { setTimeout } from 'node:timers/promises'
import { pipelineWorkflow, type PipelineInput, startWorkUpdate } from './workflows/pipeline.workflow'

async function runInterestingScenario(client: Client) {

  const workflowId = `nd-repro-pipeline`

  const input: PipelineInput = {
    numChildren: 10,
    sleepMultiplier: 0.5,
  }

  // Start the pipeline
  const handle = await client.workflow.start(pipelineWorkflow, {
    workflowId,
    taskQueue: 'default',
    args: [input],
  })
  console.log(`Started pipeline: ${workflowId}`)

  // After some time, send a 'resume' update
  const intervalMs = 5_000 * input.sleepMultiplier
  await setTimeout(intervalMs)
  console.log('Sending resume signal')
  await handle.executeUpdate(startWorkUpdate)

  // wait
  await handle.result()
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
