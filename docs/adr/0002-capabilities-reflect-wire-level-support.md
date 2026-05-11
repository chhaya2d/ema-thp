# ADR-0002: Capabilities reflect wire-level provider support, not aspirational support

- **Status:** Accepted (extended by [ADR-0003](0003-connector-contract-excludes-limit.md))
- **Date:** 2026-05-09
- **Relates to:** [ADR-0001](0001-capability-model-is-boolean-only.md) (boolean capability model)
- **Extended by:** [ADR-0003](0003-connector-contract-excludes-limit.md) — removes the
  `supportsLimit` capability and `ConnectorQuery.limit` field entirely, since both connectors
  have universally `supportsLimit=false` under this ADR.

## Context

The planner reads each connector's `CapabilitySet` to decide which clauses to push down vs.
keep pending. The pushdown labels in the demo output (`PUSHED: WHERE, ORDER BY, LIMIT, ...`)
imply that the named clause was actually transmitted to the provider.

Initially `GoogleDriveConnector` advertised `supportsLimit=true` because LIMIT *as a concept*
is something Google Drive results respect — you can ask for fewer rows. But the wire reality
is more nuanced: the Google Drive Files API has no total-result cap parameter, only `pageSize`.
The `GoogleQueryTranslator` had nothing to map `ConnectorQuery.limit()` to, so it silently
dropped the value. Result:

- Planner reported "PUSHED LIMIT" in the demo.
- The translator dropped the value.
- The provider call had no LIMIT.
- `QueryExecutor` enforced the cap engine-side, across pages.

The label and the wire reality were inconsistent. A reader looking at the demo output and the
provider call could not reconcile them.

## Decision

A connector advertises `supportsX=true` only when the underlying provider has a wire-level
parameter the translator can populate to actually push X down. Capabilities describe what
*reaches the provider*, not what the connector "knows about" or "could enforce locally".

Concretely:

- `GoogleDriveConnector.supportsLimit` is **false**. Drive's Files API has `pageSize`, not a
  total cap.
- `NotionConnector.supportsLimit` is **false**. Notion's search API has `page_size`, not a
  total cap.
- Both connectors keep `supportsPagination=true`. Both APIs natively paginate; that is wire-
  level pageability, distinct from a total cap.

Engine-side LIMIT enforcement in `QueryExecutor` does not change. The planner now reports LIMIT
as PENDING for both connectors, accurately reflecting that the cap is engine-imposed.

## Consequences

**Good**

- Planner output is a literal description of what reaches the wire. "PUSHED" means "the
  provider received and acted on it".
- Future connector authors have a clear rule: don't claim a capability your translator can't
  fulfill. Bad capability advertisements would be lies, not just optimistic hints.
- Easier reasoning about correctness: the planner's pending list now matches what the engine
  needs to execute residually, by definition.

**Bad**

- Lose the ability to express "the connector knows about LIMIT and might hint with it" without
  literally pushing it. If a future connector wraps a backend that uses LIMIT as e.g. a
  per-page hint, we'll need a richer capability model — see ADR-0001 for the operator/field
  case, same shape of problem.
- Slightly more pending operations in the demo output, which means slightly more engine work
  in a future residual executor (negligible at current scale).

## Alternatives considered

- **(B) Translator uses `limit` as a `pageSize` hint when `pageSize` is null.** Keeps
  `supportsLimit=true` defensible for non-paginated calls. Rejected: muddles two distinct
  concepts (per-page vs total) and makes the capability's meaning depend on a sibling field's
  value, which is hard to document and easy to misuse.
- **(C) Connector internalizes the pagination loop and applies LIMIT itself.** Wire-honest at
  the connector boundary. Rejected: duplicates `QueryExecutor`'s pagination loop on every
  connector and removes the engine's ability to coordinate across federated connectors later.
- **Keep current state and document the gap with `@implNote`.** What we had before this ADR.
  Rejected: documentation can't fix a misleading demo label; the planner output is a primary
  artifact of this codebase.

## Notes

This decision is narrow: it's about how capabilities are advertised, not whether engine-side
enforcement should exist. `QueryExecutor`'s engine-side LIMIT cap is the floor regardless —
even if a future connector legitimately advertises `supportsLimit=true` (e.g. wraps a SQL
backend that takes `LIMIT` directly), engine-side enforcement is still required for federated
queries that span multiple connectors.
