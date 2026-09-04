import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { createPool, defineWorkflow, getWorkflow, migrate, pollOnce, startWorkflow } from "../src/index.js";
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

test("a step that throws twice then succeeds retries and completes", async () => {
  let calls = 0;
  defineWorkflow({
    type: "flaky-then-ok",
    start: "flaky",
    steps: {
      flaky: {
        retry: { max: 5, backoffMs: 5, backoffFactor: 1 }, // fast, fixed backoff for the test
        async run() {
          calls++;
          if (calls < 3) throw new Error(`transient failure #${calls}`);
          return { type: "complete", output: { calls } };
        },
      },
    },
  });

  const id = await startWorkflow(pool, "flaky-then-ok", {});
  // backoff is 5ms; poll with small sleeps until the workflow settles
  for (let i = 0; i < 50; i++) {
    await pollOnce(pool, "test-worker");
    const wf = await getWorkflow(pool, id);
    if (wf?.status !== "running") break;
    await new Promise((r) => setTimeout(r, 10));
  }

  assert.equal(calls, 3);
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "completed");
  assert.deepEqual(wf?.context.flaky, { calls: 3 });
});

test("a step that always throws exhausts retries and fails the workflow", async () => {
  let calls = 0;
  defineWorkflow({
    type: "always-throws",
    start: "boom",
    steps: {
      boom: {
        retry: { max: 3, backoffMs: 5, backoffFactor: 1 },
        async run() {
          calls++;
          throw new Error("nope");
        },
      },
    },
  });

  const id = await startWorkflow(pool, "always-throws", {});
  for (let i = 0; i < 50; i++) {
    await pollOnce(pool, "test-worker");
    const wf = await getWorkflow(pool, id);
    if (wf?.status !== "running") break;
    await new Promise((r) => setTimeout(r, 10));
  }

  assert.equal(calls, 3, "exactly max attempts, no more");
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "failed");
  assert.match(wf?.error ?? "", /nope/);
});
