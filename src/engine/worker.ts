import type { Pool, PoolClient } from "pg";
import { getRegisteredTypes, getWorkflowDef } from "./registry.js";
import {
  type LeasedTask,
  leaseNextTask,
  markTaskDone,
  markTaskFailed,
  rescheduleTaskForRetry,
  scheduleTask,
} from "./tasks.js";
import { appendEvent } from "./events.js";
import type { StepDef, StepOutcome } from "../types.js";

export interface WorkerOptions {
  pollIntervalMs?: number;
  workerId?: string;
  /** How long a lease is held before it's considered expired. Default 30s. */
  leaseSeconds?: number;
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`step timed out after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

/** Leases and executes exactly one due task, if any. Returns true iff it did work. */
export async function pollOnce(pool: Pool, workerId: string, leaseSeconds?: number): Promise<boolean> {
  const task = await leaseNextTask(pool, workerId, getRegisteredTypes(), leaseSeconds);
  if (!task) return false;

  const wfRes = await pool.query(
    `select type, input, context from stepline_workflows where id = $1`,
    [task.workflowId],
  );
  const wf = wfRes.rows[0];
  if (!wf) return true; // workflow row is gone (e.g. manually deleted) — drop the orphaned task

  if (task.attempt > task.maxAttempts) {
    // Backstop for a step that crashes the whole worker process every time:
    // its lease keeps expiring and getting re-leased (incrementing `attempt`)
    // without ever reaching handleStepError's own attempt check below.
    await failWorkflow(
      pool,
      task.workflowId,
      task.id,
      task.step,
      `step "${task.step}" exceeded ${task.maxAttempts} attempts`,
    );
    return true;
  }

  const def = getWorkflowDef(wf.type as string);
  const step = def.steps[task.step];
  if (!step) {
    await failWorkflow(
      pool,
      task.workflowId,
      task.id,
      task.step,
      `step "${task.step}" is not defined on workflow type "${wf.type}"`,
    );
    return true;
  }

  const ctx = {
    workflowId: task.workflowId,
    input: wf.input,
    context: (wf.context as Record<string, unknown>) ?? {},
    attempt: task.attempt,
    idempotencyKey: `${task.workflowId}:${task.step}`,
  };

  try {
    const outcome = await withTimeout(step.run(ctx), step.timeoutMs ?? 30_000);
    await applyOutcome(pool, task.workflowId, task.id, task.step, outcome);
  } catch (err) {
    await handleStepError(pool, task, step, err);
  }
  return true;
}

async function applyOutcome(
  pool: Pool,
  workflowId: string,
  taskId: string,
  step: string,
  outcome: StepOutcome,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("begin");

    if (outcome.type === "complete") {
      await mergeContextAndSetStatus(client, workflowId, step, outcome.output, "completed", null);
      await appendEvent(client, workflowId, "step_completed", step, { output: outcome.output ?? null });
      await appendEvent(client, workflowId, "completed", null, {});
      await markTaskDone(client, taskId);
    } else if (outcome.type === "fail") {
      await client.query(
        `update stepline_workflows set status='failed', error=$2, updated_at=now() where id=$1`,
        [workflowId, outcome.error],
      );
      await appendEvent(client, workflowId, "step_failed", step, { error: outcome.error, terminal: true });
      await appendEvent(client, workflowId, "failed", null, { error: outcome.error });
      await markTaskDone(client, taskId);
    } else {
      const wfType = await client.query(`select type from stepline_workflows where id=$1`, [workflowId]);
      const def = getWorkflowDef(wfType.rows[0].type);
      const nextStep: StepDef | undefined = def.steps[outcome.next];
      if (!nextStep) {
        throw new Error(`step "${step}" returned unknown next step "${outcome.next}"`);
      }
      const runAt = outcome.type === "wait" ? new Date(Date.now() + outcome.delayMs) : new Date();

      await mergeContextAndSetStatus(client, workflowId, step, outcome.output, "running", outcome.next);
      await appendEvent(client, workflowId, "step_completed", step, { output: outcome.output ?? null });
      if (outcome.type === "wait") {
        await appendEvent(client, workflowId, "timer_scheduled", outcome.next, { runAt: runAt.toISOString() });
      }
      await scheduleTask(client, workflowId, outcome.next, runAt, nextStep.retry?.max ?? 5);
      await appendEvent(client, workflowId, "step_scheduled", outcome.next, {});
      await markTaskDone(client, taskId);
    }

    await client.query("commit");
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
}

async function mergeContextAndSetStatus(
  client: PoolClient,
  workflowId: string,
  step: string,
  output: unknown,
  status: "completed" | "running",
  currentStep: string | null,
): Promise<void> {
  await client.query(
    `update stepline_workflows
     set status = $2, current_step = $3, updated_at = now(),
         context = context || $4::jsonb
     where id = $1`,
    [workflowId, status, currentStep, JSON.stringify({ [step]: output ?? null })],
  );
}

async function handleStepError(
  pool: Pool,
  task: LeasedTask,
  step: StepDef,
  err: unknown,
): Promise<void> {
  const message = err instanceof Error ? err.message : String(err);
  const max = step.retry?.max ?? task.maxAttempts;

  if (task.attempt >= max) {
    await failWorkflow(pool, task.workflowId, task.id, task.step, message);
    return;
  }

  const base = step.retry?.backoffMs ?? 1000;
  const factor = step.retry?.backoffFactor ?? 2;
  const delayMs = base * Math.pow(factor, task.attempt - 1);

  const client = await pool.connect();
  try {
    await client.query("begin");
    await appendEvent(client, task.workflowId, "step_failed", task.step, {
      error: message,
      attempt: task.attempt,
      terminal: false,
    });
    await rescheduleTaskForRetry(client, task.id, new Date(Date.now() + delayMs));
    await client.query("commit");
  } catch (e) {
    await client.query("rollback");
    throw e;
  } finally {
    client.release();
  }
}

async function failWorkflow(
  pool: Pool,
  workflowId: string,
  taskId: string,
  step: string,
  error: string,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `update stepline_workflows set status='failed', error=$2, updated_at=now() where id=$1`,
      [workflowId, error],
    );
    await appendEvent(client, workflowId, "step_failed", step, { error, terminal: true });
    await appendEvent(client, workflowId, "failed", null, { error });
    await markTaskFailed(client, taskId);
    await client.query("commit");
  } catch (e) {
    await client.query("rollback");
    throw e;
  } finally {
    client.release();
  }
}

/** Starts an in-process poll loop. Returns a function that stops it. */
export async function runWorker(pool: Pool, opts: WorkerOptions = {}): Promise<() => void> {
  const workerId = opts.workerId ?? `worker-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  const interval = opts.pollIntervalMs ?? 250;
  let stopped = false;

  void (async () => {
    while (!stopped) {
      let did = false;
      try {
        did = await pollOnce(pool, workerId, opts.leaseSeconds);
      } catch (err) {
        console.error("[stepline] poll error:", err);
      }
      if (!did) await new Promise((r) => setTimeout(r, interval));
    }
  })();

  return () => {
    stopped = true;
  };
}
