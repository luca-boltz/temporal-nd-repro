# Temporal Non-Determinism Reproduction

Standalone reproduction of a batched pipeline scheduling pattern to surface non-determinism errors during workflow replay.

## What this does

A **pipeline workflow** schedules ~2000 child workflows (200 batches x 10 children each) through a priority queue with concurrency control. Each batch first runs an **alpha** child (long-running, ~90s), which then enqueues N **beta** children (short-running, ~15s) at higher priority. A separate client process toggles pause/resume every 30s via Temporal updates, exercising a mutex-guarded checkpoint system that gates child workflow starts.

The worker runs with `maxCachedWorkflows: 0`, which forces a full history replay on every workflow activation instead of using cached state. This is the scenario where non-determinism bugs surface — if the replay produces different commands than the original execution, Temporal raises a `[TMPRL1100] Nondeterminism` error.

## Architecture

```
pipeline (parent)
├── SimplePQueue (priority-based concurrency limiter)
├── PauseResumeState (mutex-guarded pause/resume via Temporal updates)
│
├── alpha-0 (child, sleeps ~90s)
│   ├── beta-0-0 (child, sleeps ~15s)
│   ├── beta-0-1
│   └── ...
├── alpha-1
│   ├── beta-1-0
│   └── ...
└── ...
```

## Configuration

The pipeline input supports two flags for comparing queue implementations:

- **`queueType`**: `'simple'` (default) uses a custom deterministic `SimplePQueue` built on Temporal's `Trigger` primitive. `'p-queue'` uses the npm `p-queue` library instead.
- **`useFireAndForgetSafe`**: `true` (default) wraps the queue with `PQueueFireAndForgetSafe`, which collects settled results and re-throws the first error on `onIdle()`. `false` uses the raw queue directly.

These can be set in `src/client.ts` when constructing the `PipelineInput`.

## Prerequisites

- Node.js 20+
- [pnpm](https://pnpm.io/)
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)

## Run

```bash
pnpm install
pnpm dev
```

This starts the Temporal dev server, worker, and client in one terminal.

To run each piece separately:

```bash
temporal server start-dev   # terminal 1
pnpm worker                 # terminal 2
pnpm client                 # terminal 3
```

## What to look for

- `[TMPRL1100] Nondeterminism` errors in worker output
- Pipeline workflow in the Temporal UI at http://localhost:8233
