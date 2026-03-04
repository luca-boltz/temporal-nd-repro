# Temporal Non-Determinism Reproduction

Standalone reproduction of a pipeline scheduling pattern to surface non-determinism errors during workflow replay.

## What this does

A **pipeline workflow** schedules N child workflows (default 10) via `Promise.all`. Each child is a simple **sleep workflow** with a log-normal duration. All children block on `condition(() => canStart)` until the client sends a `startWork` Temporal update that flips the gate open.

The worker runs with `maxCachedWorkflows: 0`, which forces a full history replay on every workflow activation instead of using cached state. This is the scenario where non-determinism bugs surface — if the replay produces different commands than the original execution, Temporal raises a `[TMPRL1100] Nondeterminism` error.

## Architecture

```
pipeline (parent)
├── canStart gate (flipped by startWork update)
│
├── sleep-0 (child, random duration)
├── sleep-1
├── sleep-2
└── ...
```

## Configuration

The pipeline input is configured in `src/client.ts`:

- **`numChildren`**: Number of child workflows to schedule (default 10).
- **`sleepMultiplier`**: Scales child sleep durations and client delay before sending the start update (default 0.5).

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

- `[TMPRL1100] Nondeterminism` errors in worker output — this is a different ND error from the one originally investigated, but nonetheless a valid non-determinism bug. Specifically, the child workflow state machine receives a `WorkflowExecutionUpdateCompleted` event it doesn't expect during replay. This happens when the `startWork` update completes at a point in the history that gets interleaved with child workflow commands.
- Pipeline workflow in the Temporal UI at http://localhost:8233
