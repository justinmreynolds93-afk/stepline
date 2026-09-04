import type { Pool } from "pg";
import type { WorkflowEvent, WorkflowRecord, WorkflowStatus } from "../types.js";

function toRecord(row: Record<string, unknown>): WorkflowRecord {
  return {
    id: row.id as string,
    type: row.type as string,
    status: row.status as WorkflowStatus,
    currentStep: (row.current_step as string) ?? null,
    input: row.input,
    context: row.context as Record<string, unknown>,
    error: (row.error as string) ?? null,
    createdAt: row.created_at as Date,
    updatedAt: row.updated_at as Date,
  };
}

export async function getWorkflow(pool: Pool, id: string): Promise<WorkflowRecord | null> {
  const { rows } = await pool.query(`select * from stepline_workflows where id = $1`, [id]);
  return rows[0] ? toRecord(rows[0]) : null;
}

export async function listWorkflows(
  pool: Pool,
  status?: WorkflowStatus,
  limit = 50,
): Promise<WorkflowRecord[]> {
  const { rows } = status
    ? await pool.query(
        `select * from stepline_workflows where status = $1 order by created_at desc limit $2`,
        [status, limit],
      )
    : await pool.query(`select * from stepline_workflows order by created_at desc limit $1`, [limit]);
  return rows.map(toRecord);
}

export async function listWorkflowEvents(pool: Pool, workflowId: string): Promise<WorkflowEvent[]> {
  const { rows } = await pool.query(
    `select * from stepline_events where workflow_id = $1 order by seq asc`,
    [workflowId],
  );
  return rows.map((r) => ({
    id: Number(r.id),
    workflowId: r.workflow_id,
    seq: r.seq,
    kind: r.kind,
    step: r.step,
    data: r.data,
    createdAt: r.created_at,
  }));
}
