import type { WorkflowDef } from "../types.js";

const registry = new Map<string, WorkflowDef>();

/** Registers a workflow definition (import its module before starting/working). */
export function defineWorkflow<TInput>(def: WorkflowDef<TInput>): WorkflowDef<TInput> {
  if (!def.steps[def.start]) {
    throw new Error(`workflow "${def.type}": start step "${def.start}" is not defined in steps`);
  }
  registry.set(def.type, def as WorkflowDef);
  return def;
}

/** Workflow types this process has defined — a worker only ever leases tasks for these. */
export function getRegisteredTypes(): string[] {
  return [...registry.keys()];
}

export function getWorkflowDef(type: string): WorkflowDef {
  const def = registry.get(type);
  if (!def) {
    throw new Error(
      `no workflow registered for type "${type}" — did you import the module that calls defineWorkflow() for it?`,
    );
  }
  return def;
}
