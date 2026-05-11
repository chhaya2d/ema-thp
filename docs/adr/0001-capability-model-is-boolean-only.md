# ADR-0001: Capability model is boolean-only

- **Status:** Accepted
- **Date:** 2026-05-09

## Context

The planner needs to decide which slices of a `Query` (`WHERE`, `ORDER BY`, `LIMIT`,
`PROJECTION`, pagination) can be pushed to a connector vs. kept pending for residual execution.

A real federation engine typically uses a richer capability model: per-field operator support,
operator-specific cost, sub-expression matching, etc. Building that machinery early would lock
us into a particular shape and consume disproportionate effort relative to the lessons we want
this codebase to demonstrate.

## Decision

`CapabilitySet` exposes only:

- `boolean supportsFiltering`
- `boolean supportsProjection`
- `boolean supportsSorting`
- `boolean supportsLimit`
- `boolean supportsPagination`
- `Set<String> supportedFields`        — informational, not consulted by the planner today
- `Set<Operator> supportedOperators`   — informational, not consulted by the planner today

The planner's pushdown decisions are derived from the booleans alone. The fields/operators sets
exist for connectors to *advertise* their capabilities (and for future planner enhancements),
but the current rules don't read them.

## Consequences

**Good**

- Planner is trivial to reason about (~80 lines, four conditional blocks).
- Adding a new connector means filling in five booleans, not designing a capability lattice.
- Demo output is readable; pushdown vs pending is a stark binary, not a partial result.

**Bad**

- A connector that supports `EQ` on `title` but not `LIKE` cannot express that — it must say
  either "supports filtering" (and break at runtime on `LIKE`) or "doesn't support filtering"
  (and pessimize away its real capability).
- Translators currently throw `IllegalArgumentException` at runtime when a query asks for
  something the underlying API doesn't support, because the planner can't know to keep that
  predicate pending.

## Alternatives considered

- **Per-operator booleans** (`supportsEqOnTitle`, etc.). Doesn't scale — explodes
  combinatorially with fields × operators.
- **Functional capabilities** (`Function<ComparisonExpr, Boolean> canPush`). Most flexible, but
  requires the planner to walk into the connector for each predicate. Right answer eventually;
  premature today.
- **Cost-based planning** with operator costs. Right answer when there are multiple connectors
  serving overlapping data and the planner picks among plans. Out of scope until federation grows
  beyond fan-out.
