// Runs once before the test runner starts (wired as npm's "pretest"). Test
// files run concurrently as separate processes but share one Postgres
// instance; without this, workflow rows left `running` by a previous
// `npm test` run (or a crashed one) are still "due" and get re-leased
// alongside the fresh rows the next run creates. Truncating here, before any
// test file starts, avoids racing a concurrently-starting file's own inserts.
import pg from "pg";
import { migrate } from "../src/db.js";

const url = process.env.TEST_DATABASE_URL ?? "postgres://postgres:postgres@localhost:55432/stepline";

async function main() {
  const pool = new pg.Pool({ connectionString: url });
  await migrate(pool);
  await pool.query("truncate table stepline_workflows, stepline_idempotent_calls restart identity cascade");
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
