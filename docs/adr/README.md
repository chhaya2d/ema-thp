# Architecture Decision Records

Short, immutable records of significant design decisions in this codebase.
One file per decision, numbered sequentially. Format:

```
# ADR-NNNN: <Title>

- **Status:** Accepted | Superseded by ADR-XXXX | Deprecated
- **Date:** YYYY-MM-DD

## Context

What forces are at play? What problem are we solving?

## Decision

What did we decide?

## Consequences

What follows from this decision (good and bad)?

## Alternatives considered

Briefly: what we didn't do and why.
```

## When to write one

Write an ADR when:

- A decision shapes more than one file or component
- A decision could be revisited later
- A future reader will ask "why did they do it this way?"

Don't write one for:

- Local one-line tradeoffs — use `// NOTE:` at the call site
- Method-level behavior caveats — use Javadoc `@implNote` / `@apiNote`
- Active work — use `// TODO:`
- Bugs / known wrong code — use `// FIXME:` or `// HACK:`

## Index

- [ADR-0001](0001-capability-model-is-boolean-only.md) — Capability model is boolean-only (no operator/field-specific support).
- [ADR-0002](0002-capabilities-reflect-wire-level-support.md) — Capabilities reflect wire-level provider support, not aspirational support.
- [ADR-0003](0003-connector-contract-excludes-limit.md) — Connector contract excludes LIMIT (cursor-only pagination; LIMIT is exclusively engine-enforced).
