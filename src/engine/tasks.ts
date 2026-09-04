import type { Pool, PoolClient } from "pg";

export interface LeasedTask {
  id: string;
  workflowId: string;
  step: string;
  attempt: number;
  maxAttempts: number;
  input: Record<string, unknown>;
}

const DEFAULT_LEASE_SECONDS = 30;

/**
 * Leases one due task and increments its attempt counter, all in one
 * `UPDATE ... FOR UPDATE SKIP LOCKED`. A task is due when it's `pending` and
 * its run_at has arrived, OR it's `leased` but the lease expired — that
 * second clause, not a separate reaper process, is what makes a crashed
 * worker's task get picked back up. Returns null if nothing is due right now.
 *
 * Only leases tasks whose workflow `type` is in `registeredTypes` — a worker
 * only ever picks up work it has a `defineWorkflow()` for. This also means
 * multiple worker pools can specialize by workflow type against one queue,
 * and it's what keeps this process's tasks isolated from another process's
 * (or, in tests, another test file's) tasks sharing the same database.
 *
 * `leaseSeconds` defaults to 30; tests shrink it so a simulated crash doesn't
 * need 30 real seconds to be reclaimed.
 */
export async function leaseNextTask(
  pool: Pool,
  workerId: string,
  registeredTypes: string[],
  leaseSeconds = DEFAULT_LEASE_SECONDS,
): Promise<LeasedTask | null> {
  if (registeredTypes.length === 0) return null;
  const client = await pool.connect();
  try {
    await client.query("begin");
    const { rows } = await client.query(
      `with candidate as (
         select t.id from stepline_tasks t
         join stepline_workflows w on w.id = t.workflow_id
         where t.run_at <= now()
           and (t.status = 'pending' or (t.status = 'leased' and t.locked_until < now()))
           and w.type = any($3)
         order by t.run_at
         for update of t skip locked
         limit 1
       )
       update stepline_tasks t
       set status = 'leased',
           locked_by = $1,
           locked_until = now() + make_interval(secs => $2),
           attempt = t.attempt + 1
       from candidate
       where t.id = candidate.id
       returning t.id, t.workflow_id, t.step, t.attempt, t.max_attempts, t.input`,
      [workerId, leaseSeconds, registeredTypes],
    );
    await client.query("commit");
    if (rows.length === 0) return null;
    const r = rows[0];
    return {
      id: r.id,
      workflowId: r.workflow_id,
      step: r.step,
      attempt: r.attempt,
      maxAttempts: r.max_attempts,
      input: r.input,
    };
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
}

export async function scheduleTask(
  client: PoolClient,
  workflowId: string,
  step: string,
  runAt: Date,
  maxAttempts: number,
): Promise<void> {
  await client.query(
    `insert into stepline_tasks (workflow_id, step, run_at, max_attempts) values ($1,$2,$3,$4)`,
    [workflowId, step, runAt, maxAttempts],
  );
}

export async function markTaskDone(client: PoolClient, taskId: string): Promise<void> {
  await client.query(`update stepline_tasks set status = 'done' where id = $1`, [taskId]);
}

export async function markTaskFailed(client: PoolClient, taskId: string): Promise<void> {
  await client.query(`update stepline_tasks set status = 'failed' where id = $1`, [taskId]);
}

/** Re-queues the SAME task row (same attempt count already incremented by the lease) for a later retry. */
export async function rescheduleTaskForRetry(
  client: PoolClient,
  taskId: string,
  runAt: Date,
): Promise<void> {
  await client.query(
    `update stepline_tasks
     set status = 'pending', run_at = $2, locked_by = null, locked_until = null
     where id = $1`,
    [taskId, runAt],
  );
}
