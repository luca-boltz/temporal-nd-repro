# Temporal Non-Determinism Reproduction

Standalone reproduction of a pipeline scheduling pattern to surface non-determinism errors during workflow replay.

## What this does

A **pipeline workflow** schedules N child workflows (default 200) through a `p-queue` concurrency limiter. Each child is a simple **sleep workflow** with a log-normal duration. Before starting each child, the pipeline waits on a `condition(() => !isPaused)` gate. A separate client process toggles pause/resume every few seconds via a Temporal update handler that flips the `isPaused` boolean inside a mutex.

The worker runs with `maxCachedWorkflows: 0`, which forces a full history replay on every workflow activation instead of using cached state. This is the scenario where non-determinism bugs surface — if the replay produces different commands than the original execution, Temporal raises a `[TMPRL1100] Nondeterminism` error.

## Architecture

```
pipeline (parent)
├── PQueue (concurrency limiter via p-queue)
├── isPaused boolean (toggled via Temporal update + mutex)
│
├── sleep-0 (child, random duration)
├── sleep-1
├── sleep-2
└── ...
```

## Configuration

The pipeline input is configured in `src/client.ts`:

- **`numChildren`**: Number of child workflows to schedule (default 200).
- **`queueConcurrency`**: Max concurrent child workflows (default 10).
- **`sleepMultiplier`**: Scales child sleep durations and pause/resume interval (default 0.5).

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
