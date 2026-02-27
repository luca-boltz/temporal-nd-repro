import { executeChild, Workflow, workflowInfo } from '@temporalio/workflow'
import type { PauseResumeState } from './pause-resume'

export async function executeTioChild<W extends Workflow>(
  pauseResumeState: PauseResumeState,
  workflowFn: W,
  workflowId: string,
  args: Parameters<W>,
) {
  await pauseResumeState.checkAndWaitIfPaused(`before-child-${workflowFn.name}`)
  return await executeChild(workflowFn.name, {
    workflowId,
    taskQueue: workflowInfo().taskQueue,
    args: args as any,
  })
}
