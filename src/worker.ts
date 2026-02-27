import { Worker, NativeConnection } from '@temporalio/worker'
import path from 'path'

async function main() {
  const connection = await NativeConnection.connect({ address: 'localhost:7233' })

  const worker = await Worker.create({
    connection,
    namespace: 'default',
    taskQueue: 'default',
    workflowsPath: path.resolve(__dirname, './workflows'),
    maxCachedWorkflows: 0, // Force full replay on every activation
  })

  console.log('Worker started (maxCachedWorkflows: 0)')
  await worker.run()
}

main().catch((err) => {
  console.error('Worker failed', err)
  process.exit(1)
})
