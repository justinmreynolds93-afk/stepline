/**
 * A standalone worker, spawned as a real child process by chaos.test.ts so it
 * can be genuinely SIGKILL'd — killing an in-process function call doesn't
 * exercise the same crash-recovery path as killing the OS process actually
 * holding the task lease.
 *
 *   DATABASE_URL=... STEPLINE_LEASE_SECONDS=2 node --import tsx test/worker-process.ts
 */
import { createPool, migrate, runWorker } from "../src/index.js";
import { registerCrashTestWorkflow, registerPoisonPillWorkflow } from "./crash-workflow.js";

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("set DATABASE_URL");
  const pool = createPool(url);
  await migrate(pool);
  registerCrashTestWorkflow(pool);
  registerPoisonPillWorkflow();

  await runWorker(pool, {
    pollIntervalMs: 100,
    leaseSeconds: Number(process.env.STEPLINE_LEASE_SECONDS ?? 30),
  });

  await new Promise(() => {
    /* run until killed */
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
