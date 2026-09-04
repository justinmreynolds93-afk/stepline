import type { Pool } from "pg";
import { defineWorkflow, once } from "../src/index.js";

/**
 * A workflow with one deliberately slow step, used by chaos.test.ts to prove
 * that killing the worker mid-step doesn't lose the workflow.
 */
export function registerCrashTestWorkflow(pool: Pool): void {
  defineWorkflow({
    type: "crash-test",
    start: "slowStep",
    steps: {
      slowStep: {
        retry: { max: 5, backoffMs: 50 },
        async run(ctx) {
          // Recorded before the slow part, so the test can prove attempt #1
          // really began before it got killed.
          await once(pool, `${ctx.workflowId}:slowStep:started`, async () => ({ at: Date.now() }));
          await new Promise((r) => setTimeout(r, 2000));
          return { type: "continue", next: "finalStep", output: { slowDone: true } };
        },
      },
      finalStep: {
        async run() {
          return { type: "complete", output: { done: true } };
        },
      },
    },
  });
}

/** A step that always throws — proves retries exhaust and the workflow ends up failed. */
export function registerPoisonPillWorkflow(): void {
  defineWorkflow({
    type: "poison-pill",
    start: "alwaysThrows",
    steps: {
      alwaysThrows: {
        retry: { max: 3, backoffMs: 10 },
        async run() {
          throw new Error("simulated permanent failure");
        },
      },
    },
  });
}
