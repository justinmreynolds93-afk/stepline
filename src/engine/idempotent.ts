import type { Pool } from "pg";

/**
 * Runs `fn()` at most once for a given key — call it from inside a step's
 * `run()` around the actual side effect (send the email, charge the card),
 * keyed by `ctx.idempotencyKey`, so a retry of the same logical step doesn't
 * repeat it.
 *
 * What this does NOT guarantee: if the process is killed after `fn()`
 * succeeds but before the result row is committed, a retry will call `fn()`
 * again — there is no way to make an arbitrary external side effect and a
 * local commit atomic without support from the external system (idempotency
 * keys on the provider's API, e.g. Stripe's). This narrows the duplicate
 * window to "crashed at the exact wrong instant," it does not close it.
 */
export async function once<T>(pool: Pool, key: string, fn: () => Promise<T>): Promise<T> {
  const existing = await pool.query<{ result: T }>(
    `select result from stepline_idempotent_calls where key = $1`,
    [key],
  );
  if (existing.rows.length > 0) return existing.rows[0]!.result;

  const result = await fn();
  await pool.query(
    `insert into stepline_idempotent_calls (key, result) values ($1, $2)
     on conflict (key) do nothing`,
    [key, JSON.stringify(result ?? null)],
  );
  return result;
}
