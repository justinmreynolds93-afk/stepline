import type { Pool } from "pg";
import { getWorkflowDef } from "./registry.js";
import { appendEvent } from "./events.js";
import { scheduleTask } from "./tasks.js";

/** Creates a new workflow row and schedules its start step. Returns the workflow id. */
export async function startWorkflow<TInput>(
  pool: Pool,
  type: string,
  input: TInput,
): Promise<string> {
  const def = getWorkflowDef(type);
  const client = await pool.connect();
  try {
    await client.query("begin");
    const { rows } = await client.query(
      `insert into stepline_workflows (type, status, current_step, input)
       values ($1, 'running', $2, $3) returning id`,
      [type, def.start, JSON.stringify(input ?? {})],
    );
    const workflowId = rows[0].id as string;
    await appendEvent(client, workflowId, "started", null, { input: input ?? {} });
    const startStep = def.steps[def.start]!;
    await scheduleTask(client, workflowId, def.start, new Date(), startStep.retry?.max ?? 5);
    await appendEvent(client, workflowId, "step_scheduled", def.start, {});
    await client.query("commit");
    return workflowId;
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
}
