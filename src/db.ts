import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import pg from "pg";

const { Pool } = pg;
export type { Pool as PgPool } from "pg";

const MIGRATIONS_DIR = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "migrations",
);

// Arbitrary fixed key for a session-level Postgres advisory lock, held only
// around migrate()'s DDL. Plain `CREATE TABLE IF NOT EXISTS` is not safe
// against two connections migrating at the same instant — Postgres documents
// a race where both pass the existence check before either commits, and the
// implicit pg_type row each CREATE TABLE inserts collides. Any process (or
// test file) calling migrate() concurrently serializes on this lock instead.
const MIGRATION_LOCK_KEY = 869_211_734;

/**
 * Applies every migrations/*.sql file whose number is greater than the
 * currently-recorded schema version, in order, inside one transaction.
 * Safe to call on every process start — a fully-migrated database is a no-op.
 * Safe to call concurrently from multiple processes — see MIGRATION_LOCK_KEY.
 */
export async function migrate(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("select pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    try {
      await client.query("begin");
      const files = (await readdir(MIGRATIONS_DIR))
        .filter((f) => f.endsWith(".sql"))
        .sort();
      for (const file of files) {
        const sql = await readFile(path.join(MIGRATIONS_DIR, file), "utf8");
        await client.query(sql);
      }
      await client.query("commit");
    } catch (err) {
      await client.query("rollback");
      throw err;
    } finally {
      await client.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]);
    }
  } finally {
    client.release();
  }
}

export function createPool(connectionString: string): pg.Pool {
  return new Pool({ connectionString });
}
