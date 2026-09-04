/**
 * Runs the onboarding-drip example end to end against DATABASE_URL.
 *   DATABASE_URL=postgres://postgres:postgres@localhost:55432/stepline npm run example
 */
import { createPool, migrate, runWorker, startWorkflow, getWorkflow, listWorkflowEvents } from "../src/index.js";
import { registerOnboardingDrip } from "./onboarding-drip.js";

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("set DATABASE_URL, e.g. from test/docker-compose.yml on :55432");
  const pool = createPool(url);
  await migrate(pool);
  registerOnboardingDrip(pool);

  const id = await startWorkflow(pool, "onboarding-drip", {
    userEmail: "demo@example.com",
    willActivate: false,
    demo: { day1Ms: 1000, day3Ms: 1500 }, // compressed for the demo
  });
  console.log(`started workflow ${id}`);

  const stop = await runWorker(pool, { pollIntervalMs: 100 });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const wf = await getWorkflow(pool, id);
    if (wf && wf.status !== "running") break;
    await new Promise((r) => setTimeout(r, 200));
  }
  stop();

  const final = await getWorkflow(pool, id);
  console.log("\nfinal state:", JSON.stringify(final, null, 2));
  console.log("\nevent log:");
  for (const e of await listWorkflowEvents(pool, id)) {
    console.log(`  ${e.seq}. ${e.kind}${e.step ? ` (${e.step})` : ""}`);
  }
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
