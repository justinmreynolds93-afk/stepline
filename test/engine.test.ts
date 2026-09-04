import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import {
  createPool,
  defineWorkflow,
  getWorkflow,
  listWorkflowEvents,
  migrate,
  pollOnce,
  startWorkflow,
} from "../src/index.js";
import type pg from "pg";

const URL = process.env.TEST_DATABASE_URL ?? "postgres://postgres:postgres@localhost:55432/stepline";
let pool: pg.Pool;

before(async () => {
  pool = createPool(URL);
  await migrate(pool);
});
after(async () => {
  await pool.end();
});

test("a linear three-step workflow runs to completion via pollOnce", async () => {
  const calls: string[] = [];
  defineWorkflow({
    type: "linear-3",
    start: "a",
    steps: {
      a: {
        async run() {
          calls.push("a");
          return { type: "continue", next: "b", output: { from: "a" } };
        },
      },
      b: {
        async run(ctx) {
          calls.push("b");
          assert.deepEqual(ctx.context.a, { from: "a" }, "b should see a's output in context");
          return { type: "continue", next: "c", output: { from: "b" } };
        },
      },
      c: {
        async run() {
          calls.push("c");
          return { type: "complete", output: { done: true } };
        },
      },
    },
  });

  const id = await startWorkflow(pool, "linear-3", {});
  for (let i = 0; i < 5; i++) {
    const did = await pollOnce(pool, "test-worker");
    if (!did) break;
  }

  assert.deepEqual(calls, ["a", "b", "c"]);
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "completed");
  assert.deepEqual(wf?.context, { a: { from: "a" }, b: { from: "b" }, c: { done: true } });

  const events = await listWorkflowEvents(pool, id);
  assert.deepEqual(
    events.map((e) => e.kind),
    [
      "started",
      "step_scheduled",
      "step_completed",
      "step_scheduled",
      "step_completed",
      "step_scheduled",
      "step_completed",
      "completed",
    ],
  );
});

test("an explicit { type: 'fail' } outcome fails the workflow without retrying", async () => {
  let calls = 0;
  defineWorkflow({
    type: "explicit-fail",
    start: "checkSomething",
    steps: {
      checkSomething: {
        retry: { max: 5 },
        async run() {
          calls++;
          return { type: "fail", error: "business rule says no" };
        },
      },
    },
  });

  const id = await startWorkflow(pool, "explicit-fail", {});
  await pollOnce(pool, "test-worker");

  assert.equal(calls, 1, "an explicit fail is not retried");
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "failed");
  assert.equal(wf?.error, "business rule says no");
});
