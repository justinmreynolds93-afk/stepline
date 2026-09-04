# Changelog

## 0.1.0

Initial release.

- Postgres-backed durable execution engine: workflows, append-only event log,
  and a `SELECT ... FOR UPDATE SKIP LOCKED` task queue with lease-expiry crash
  recovery (no separate reaper process).
- Declarative step-graph workflow definitions (`defineWorkflow`) with
  `continue` / `wait` / `complete` / `fail` step outcomes.
- Per-step retry policy with exponential backoff; thrown errors retry, an
  explicit `{ type: "fail" }` outcome does not.
- `once()` idempotency helper for at-least-once side effects, with its real
  limitation documented rather than papered over.
- CLI: `migrate`, `worker`, `list`, `status`, `history`.
- Test suite covering linear execution, retry exhaustion, timers, and a real
  crash/recovery scenario (`SIGKILL` of a child-process worker mid-step).
