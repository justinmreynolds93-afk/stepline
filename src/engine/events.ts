import type { PoolClient } from "pg";

/**
 * Appends one row to the append-only event log. The `seq` subquery is safe
 * without an explicit lock because of stepline's core invariant: a workflow
 * has at most one non-`done` task at a time, so only the worker holding that
 * task's lease ever appends events for this workflow — there is no concurrent
 * writer to race against.
 */
export async function appendEvent(
  client: PoolClient,
  workflowId: string,
  kind: string,
  step: string | null,
  data: Record<string, unknown> = {},
): Promise<void> {
  await client.query(
    `insert into stepline_events (workflow_id, seq, kind, step, data)
     values (
       $1,
       coalesce((select max(seq) + 1 from stepline_events where workflow_id = $1), 1),
       $2, $3, $4
     )`,
    [workflowId, kind, step, JSON.stringify(data)],
  );
}
