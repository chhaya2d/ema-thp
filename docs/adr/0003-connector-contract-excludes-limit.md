# ADR-0003: Connector contract excludes LIMIT (cursor-only pagination)

- **Status:** Accepted
- **Date:** 2026-05-09
- **Builds on:** [ADR-0002](0002-capabilities-reflect-wire-level-support.md) (capabilities are wire-honest)

## Context

ADR-0002 made connector capabilities wire-honest: a connector advertises `supportsX=true` only
when its translator can populate a real provider parameter. As a consequence, both connectors
ended up with `supportsLimit=false`, the `Planner` always marked LIMIT as PENDING, and the
`GoogleQueryTranslator` carried an explicit `// NOTE:` saying "we receive `ConnectorQuery.limit()`
but ignore it because there's nowhere to put it".

This left the codebase in an awkward intermediate state:

- `CapabilitySet` had a `supportsLimit` boolean that no concrete connector set to `true`.
- `ConnectorQuery` had a `limit` field that no translator ever read meaningfully.
- The translator had a defensive comment explaining why a slot in its input was deliberately
  ignored — a load-bearing comment for a load-bearing absence.
- The planner had a LIMIT push-rule that, given the universal `supportsLimit=false`, was a
  branch that always took the "PENDING" path.

In short: the *type system* still pretended LIMIT-pushdown was a thing, while the *runtime*
unanimously agreed it wasn't. This is the worst kind of abstraction — one whose emptiness has
to be reaffirmed in every contributor's head.

There's also a federation argument: even if some hypothetical future connector legitimately
wraps a backend with a real `LIMIT` clause (Postgres, Snowflake, etc.), the engine still has
to enforce LIMIT at the *federated* result level — across multiple connectors — so an
engine-side cap is mandatory regardless. A connector-side LIMIT would at best save one extra
page fetch per connector, which is dwarfed by the cost of cross-connector merging.

## Decision

The connector contract is **cursor-pagination only**. Connectors can be invoked with
`ConnectorQuery(projection, where, orderBy, cursor, pageSize)` and that's the entire surface.
LIMIT is exclusively engine-enforced.

Concretely, in code:

- `CapabilitySet` no longer has a `supportsLimit` field.
- `ConnectorQuery` no longer has a `limit` field.
- `Planner` no longer has a LIMIT push-rule and never adds `"LIMIT"` to `pendingOperations`.
- `QueryExecutor.execute(connector, pushedQuery, residualOps, logicalLimit)` keeps the
  `logicalLimit` parameter; it is the *only* place LIMIT exists at the engine boundary.
- The demo's `Main` prints `Engine cap: LIMIT N` as its own line, parallel to (not nested
  under) PUSHED / PENDING.

## Consequences

**Good**

- The connector contract is now *minimal and complete*: every field on `ConnectorQuery` is
  meaningful, every boolean on `CapabilitySet` reflects a real translator decision. No more
  fields-that-exist-to-be-ignored.
- New connector authors get the rule for free: "implement filter, sort, project, paginate;
  the engine handles LIMIT". They literally cannot be tempted to honor LIMIT because there's
  nothing to honor.
- The planner output is simpler. PUSHED / PENDING / Residual is now strictly about *relational
  operations the planner decides where to run*. LIMIT, an always-engine concern, is shown
  separately so it can't be misread as a planner decision.
- ADR-0002's "PUSHED label must be honest" property is enforced at the type level rather than
  by convention: if a slot doesn't exist, no one can claim to push it.

**Bad**

- A future connector that genuinely *can* push LIMIT cheaply (e.g. wrapping Postgres) cannot
  do so without re-introducing the field. If/when that happens, the right move is to add the
  field back as an *optimization hint* — clearly distinct from the federated cap, which still
  runs in `QueryExecutor`. The engine cap is the floor; pushed LIMIT would be an upper-bound
  optimization.
- Removes the symmetric four-fold ("filter / sort / limit / paginate") capability shape that's
  intuitive at first glance. The new shape (filter / sort / paginate, with LIMIT structurally
  outside) is more accurate but slightly less symmetric.

## Alternatives considered

- **Keep the field, document its emptiness more loudly.** Effectively the pre-ADR state. The
  original `// NOTE:` in `GoogleQueryTranslator` was already a loud documentation: "this slot
  is deliberately ignored." Documenting unused state is strictly worse than removing it.
- **Make LIMIT part of `ResidualOps`.** Then the engine's LIMIT enforcement would be modeled
  as just another residual operation. Rejected because (a) LIMIT enforcement is *unconditional*
  at the engine — it's not "what's left over after pushdown", it's "always", which is a
  different semantic; and (b) `ResidualOps.isEmpty()` is used by `QueryExecutor` to decide
  whether to disable page-level short-circuit; a non-empty residual triggers a full-scan.
  Modeling LIMIT as residual would force a full-scan on every query that has a LIMIT, defeating
  the optimization.
- **Push LIMIT only when ORDER BY is also pushed.** A correctness-preserving micro-optimization
  for the day a connector has a real LIMIT clause. Rejected for now because no such connector
  exists; the cost (re-introducing a field nobody uses) outweighs the benefit. Easy to revisit.

## Notes

This decision is the type-level enforcement of ADR-0002's wire-honesty principle. ADR-0002
said "don't lie about capabilities"; ADR-0003 says "don't even provide the slot a lie could
fit in". The engine-side LIMIT cap in `QueryExecutor` is the floor regardless of any future
connector capability — federation forces it.
