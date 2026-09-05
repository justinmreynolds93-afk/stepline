# Changelog

## 0.1.1

- Add a `prepare` script (`tsc -p tsconfig.json`) so installing stepline as a
  git dependency (`"stepline": "github:justinmreynolds93-afk/stepline#v0.1.1"`)
  actually produces `dist/` — `main`/`bin`/`types` all point there, and `dist/`
  itself is gitignored, so without this a git-based install resolved to
  nothing. Not needed when installing from a published npm tarball (which
  runs `prepack` and ships `dist/` directly); only matters for git deps.

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
