# stepline

Durable, Postgres-backed step-graph workflows for Node. Kill the worker in the
middle of a step; it resumes from where the lease was lost, not from scratch —
and there's a test that actually does this (`SIGKILL`s a real child process
mid-step) rather than asserting it in a design doc.

![ci](https://github.com/justinmreynolds93-afk/stepline/actions/workflows/ci.yml/badge.svg)
![license](https://img.shields.io/badge/license-MIT-blue)
![node](https://img.shields.io/badge/node-%E2%89%A520-brightgreen)

## Why

Any process that goes "do A, wait a while, do B, maybe branch to C" outlives a
single request/response cycle and needs to survive the worker restarting,
deploying, or crashing mid-step. `stepline` is the minimum viable version of
what Temporal/Cadence/Inngest solve, built on infrastructure you already run —
Postgres — instead of a separate durable-execution service:

- **Workflows are data, not code.** Each workflow is a named graph of steps —
  a `run()` function, a retry policy, and "given the output, what's next" —
  stored and versioned like any other config. No deterministic-replay-of-
  arbitrary-code machinery to get subtly wrong.
- **Durability comes from Postgres, not a special runtime.** Workflow state,
  the append-only event log, and the task queue are three tables. `SELECT ...
  FOR UPDATE SKIP LOCKED` leases a task; a lease that expires (worker crashed)
  is simply eligible to be leased again — no separate reaper process.
- **At-least-once, honestly.** A step can run more than once (that's what
  "durable" costs) — `once()` narrows the window for a side effect to repeat,
  it does not close it. See [What this does not guarantee](#what-this-does-not-guarantee).

## Example

```ts
import { defineWorkflow, startWorkflow, once } from "stepline";

defineWorkflow({
  type: "onboarding-drip",
  start: "sendWelcome",
  steps: {
    sendWelcome: {
      retry: { max: 3 },
      async run(ctx) {
        await once(pool, ctx.idempotencyKey, () => sendEmail("welcome", ctx.input.userEmail));
        return { type: "continue", next: "waitOneDay" };
      },
    },
    waitOneDay: {
      async run() {
        return { type: "wait", next: "sendTips", delayMs: 24 * 60 * 60 * 1000 };
      },
    },
    sendTips: {
      retry: { max: 3 },
      async run(ctx) {
        await once(pool, ctx.idempotencyKey, () => sendEmail("tips", ctx.input.userEmail));
        return { type: "complete" };
      },
    },
  },
});

const id = await startWorkflow(pool, "onboarding-drip", { userEmail: "a@example.com" });
```

Full working version (with the "wait 3 days, then branch on whether the user
activated" step) in [`examples/onboarding-drip.ts`](examples/onboarding-drip.ts).
Run it: `npm run test:db:up && DATABASE_URL=postgres://postgres:postgres@localhost:55432/stepline npm run example`.

## Architecture

```
startWorkflow()  ─▶  stepline_workflows (1 row)  +  stepline_tasks (1 row: the start step)
                                                          │
                     ┌────────────────────────────────────┘
                     ▼
              worker polls: SELECT ... FOR UPDATE SKIP LOCKED
              WHERE due AND (pending OR lease expired)
                     │
                     ▼
              runs the step's run(ctx)
                     │
        ┌────────────┼─────────────┬───────────────┐
        ▼            ▼             ▼               ▼
   {continue}    {wait, delay}  {complete}      {fail}         (thrown error)
   next task     next task,        │               │                │
   run_at=now    run_at=now+delay  workflow         workflow         retry with
                                   completed        failed           backoff, or
                                                                      fail after
                                                                      max attempts
```

Every transition — scheduled, completed, failed, retried, timed — is appended
to `stepline_events`, an append-only log you can replay for an audit trail or
a debugging session (`stepline history <id>`).

## Install

Not on npm yet — install straight from the tagged release:

```bash
npm install github:justinmreynolds93-afk/stepline#v0.1.1 pg
npx stepline migrate   # or call migrate(pool) yourself at startup
```

```ts
import { createPool, migrate, runWorker } from "stepline";

const pool = createPool(process.env.DATABASE_URL!);
await migrate(pool);
await runWorker(pool); // polls forever; call the returned function to stop
```

Or run a worker as its own process: `DATABASE_URL=... npx stepline worker`.

## CLI

```
stepline migrate                 apply pending schema migrations
stepline worker                  run a worker (Ctrl+C to stop)
stepline list [--status <s>]     list recent workflows
stepline status <workflow-id>    show one workflow's current state
stepline history <workflow-id>   show its full event log
```

## What this does not guarantee

- **Not exactly-once.** `once()` records a side effect's result *after* it
  succeeds; if the process is killed in the gap between the side effect
  completing and that record committing, a retry repeats it. Real exactly-once
  for an external system (send an email, charge a card) needs support from
  that system (a provider-side idempotency key), not just bookkeeping on this
  side.
- **Not deterministic-replay.** Workflows are an explicit graph, not arbitrary
  code the engine replays step-by-step. You cannot write a `for` loop that
  dynamically decides what steps exist; you write a `next` function that
  picks among steps declared up front. That's a deliberate trade: less
  expressive, far easier to reason about what a crash recovery actually does.
- **At-most one active task per workflow.** A workflow's own step graph never
  runs two of its steps concurrently. Running many *different* workflows
  concurrently is exactly what the worker pool is for.

## Testing this claim, not just making it

[`test/chaos.test.ts`](test/chaos.test.ts) starts a workflow, spawns a real
worker as a child process, waits for it to be mid-step (confirmed via a
recorded idempotency marker), `SIGKILL`s it, spawns a second worker, and
asserts the workflow still reaches `completed` — with the task's `attempt`
counter proving it really was re-leased, not just lucky. `test/retry.test.ts`
covers thrown-error backoff and retry exhaustion; `test/timer.test.ts` covers
that a `wait` step's target genuinely doesn't fire early.

```bash
npm run test:db:up
npm test
```

## License

[MIT](LICENSE)
