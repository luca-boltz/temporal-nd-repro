import { startChild, workflowInfo } from '@temporalio/workflow'
import type { PauseResumeState } from './pause-resume'

export async function executeTioChild<W extends (...args: any[]) => Promise<any>>(
  pauseResumeState: PauseResumeState,
  workflowFn: W,
  workflowId: string,
  args: Parameters<W>,
): Promise<ReturnType<W>> {
  const childHandle = await pauseResumeState.executeAtCheckpoint(
    `before-child-${workflowFn.name}`,
    async () =>
      startChild(workflowFn.name, {
        workflowId,
        taskQueue: workflowInfo().taskQueue,
        args: args as any,
      }),
  )
  return await childHandle.result() as Awaited<ReturnType<W>>
}
