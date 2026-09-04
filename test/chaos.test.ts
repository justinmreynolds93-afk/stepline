import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { createPool, getWorkflow, migrate, startWorkflow } from "../src/index.js";
import { registerCrashTestWorkflow } from "./crash-workflow.js";
import type pg from "pg";

const DB_URL = process.env.TEST_DATABASE_URL ?? "postgres://postgres:postgres@localhost:55432/stepline";
const WORKER_SCRIPT = path.join(path.dirname(fileURLToPath(import.meta.url)), "worker-process.ts");
let pool: pg.Pool;

before(async () => {
  pool = createPool(DB_URL);
  await migrate(pool);
  registerCrashTestWorkflow(pool);
});
after(async () => {
  await pool.end();
});

function spawnWorker(leaseSeconds: number): ChildProcess {
  return spawn(process.execPath, ["--import", "tsx", WORKER_SCRIPT], {
    env: { ...process.env, DATABASE_URL: DB_URL, STEPLINE_LEASE_SECONDS: String(leaseSeconds) },
    stdio: "ignore",
    windowsHide: true,
  });
}

test(
  "a worker killed mid-step does not lose the workflow — a second worker resumes and completes it",
  { timeout: 30_000 },
  async () => {
    const id = await startWorkflow(pool, "crash-test", {});

    const leaseSeconds = 2;
    const worker1 = spawnWorker(leaseSeconds);
    try {
      // slowStep sleeps 2000ms; give the worker time to lease it and be mid-sleep
      await new Promise((r) => setTimeout(r, 700));

      const started = await pool.query(`select 1 from stepline_idempotent_calls where key = $1`, [
        `${id}:slowStep:started`,
      ]);
      assert.equal(started.rowCount, 1, "attempt #1 should have started before we kill it");

      const midCrash = await getWorkflow(pool, id);
      assert.equal(midCrash?.status, "running", "should still be mid-flight, not yet complete");
    } finally {
      worker1.kill("SIGKILL");
    }

    // give the OS a moment to actually reap the killed process
    await new Promise((r) => setTimeout(r, 300));

    // the task is still "leased" with a short-lived lease; a second worker
    // should pick it up once that lease expires and re-run slowStep to completion
    const worker2 = spawnWorker(leaseSeconds);
    try {
      const deadline = Date.now() + 20_000;
      let wf = await getWorkflow(pool, id);
      while (wf && wf.status === "running" && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 200));
        wf = await getWorkflow(pool, id);
      }

      assert.equal(wf?.status, "completed");
      assert.deepEqual(wf?.context.finalStep, { done: true });
    } finally {
      worker2.kill("SIGKILL");
    }

    const task = await pool.query(
      `select attempt from stepline_tasks where workflow_id = $1 and step = 'slowStep'`,
      [id],
    );
    assert.ok(
      (task.rows[0]?.attempt ?? 0) >= 2,
      `slowStep should have been leased at least twice (crash + resume), got ${task.rows[0]?.attempt}`,
    );
  },
);
