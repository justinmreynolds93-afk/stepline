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

test("a 'wait' outcome does not run the next step before its delay elapses", async () => {
  let laterRan = false;
  defineWorkflow({
    type: "timer-basic",
    start: "waitABit",
    steps: {
      waitABit: {
        async run() {
          return { type: "wait", next: "later", delayMs: 400 };
        },
      },
      later: {
        async run() {
          laterRan = true;
          return { type: "complete" };
        },
      },
    },
  });

  const id = await startWorkflow(pool, "timer-basic", {});

  // t=0: the wait step itself is due and runs immediately, scheduling "later" for t≈400ms
  assert.equal(await pollOnce(pool, "w"), true);
  // immediately after, "later" is NOT due yet
  assert.equal(await pollOnce(pool, "w"), false, "the timer step should not be due yet");
  assert.equal(laterRan, false);

  let wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "running");
  assert.equal(wf?.currentStep, "later");

  await new Promise((r) => setTimeout(r, 500));

  assert.equal(await pollOnce(pool, "w"), true, "the timer should be due now");
  assert.equal(laterRan, true);
  wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "completed");
});
