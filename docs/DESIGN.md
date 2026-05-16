# Ema THP — Design

How Ema's federated query layer is shaped today, why each non-obvious choice was made, and what the production version would look like. Reads top-to-bottom — current architecture first, then trade-offs, then forward path.

For lower-level decisions, see `docs/adr/` (Architecture Decision Records). This document is the overview; ADRs are the detail.

## Contents

- [Problem framing](#problem-framing)
- [Architecture at a glance](#architecture-at-a-glance)
- [Connector](#connector)
- [Authentication](#authentication)
- [Authorization](#authorization)
- [HTTP surface](#http-surface)
- [Platform](#platform) — [Planner](#planner) · [Engine](#engine-hybrid-runner) · [Snapshot cache](#snapshot-cache)
- [Concurrency](#concurrency)
- [Operability](#operability)
- [Deliberately out of scope](#deliberately-out-of-scope)
- [Six-month plan](#six-month-plan)

---

## Problem framing

Enterprise data lives in many SaaS apps (Drive, Notion, Salesforce, Slack, …). Each has its own API, auth model, rate limits, and freshness semantics. Customers want to query across all of them as if they were one database — without ETL, without giving up source-side permissions, without violating per-app rate limits.

Ema is the layer that makes "one SQL across N apps" real. The federated query layer in this repo is the first slice: parse SQL → plan what each source can do natively → execute with caching, rate limits, RBAC, and observability.

---

## Architecture at a glance

```
SQL string
   │
   ▼
Parser (jsqlparser)
   │
   ▼
Planner ── reads ──► Connector.capabilities()
   │     produces:
   │       pushedQuery (what the source can do natively)
   │       residualOps (what we do in Ema after fetch)
   ▼
Engine / QueryExecutor
   │     applies: connector.search() loop → residual filter/sort/limit → tag filter
   ▼
Snapshot cache (filesystem chunks)
   │     single-flight on miss; per-connector freshness ceiling
   ▼
Response (rows + traceId + freshness_ms + rate_limit_status)

Sidecars (cut across all layers):
   RequestContext       — identity, traceId, tenantId, startedAt
   TagAccessPolicy      — role → allowed tags
   RateLimiter          — service-layer (user + tenant) and connector-layer
   Metrics              — Prometheus, /metrics endpoint
   TokenStore           — opaque Credential for connector auth
```

Six seams are deliberately pluggable: `Connector`, `PrincipalRegistry`, `TokenStore`, `TagAccessPolicy`, `RateLimiter`, `SnapshotStore`. Swapping any of them is a constructor change, not a refactor.

---

## Connector

A **Connector** is what lets Ema talk to one source app — Google Drive, Notion, Salesforce, etc. Each connector declares a small set of things so the rest of Ema knows how to plan queries, cache results, and apply security correctly.

### 1. Capability announcement

Every connector tells the planner what it can do natively:

- Which filters it supports (`=`, `IN`, range, `LIKE`, full-text)
- Which sort columns it supports
- Whether it supports projection (asking for specific fields)
- Whether it paginates with cursors or offsets
- Which operators and field types it understands

Example: Notion's API supports filtering by `updatedAt` but not by arbitrary text. The planner pushes `WHERE updatedAt > '2024-01-01'` to Notion, but keeps `WHERE title LIKE 'budget%'` to handle in Ema after the rows come back.

### 2. Data scope — drives cache sharing

Each connector declares how the source scopes data, picking from `USER / ROLE / TENANT / PUBLIC`:

- **USER** — each user sees a different result. Drive search with a personal OAuth token. Cache key includes the user.
- **ROLE** — same role sees the same data. Notion workspace shared with an HR group. Cache key includes role, not user.
- **TENANT** — all users in the same company see the same data. Cache key is just tenant.
- **PUBLIC** — everyone sees the same data. Single cache.

The cache key is derived from scope. Broader scope = more sharing = higher hit rate, but only safe when the source actually scopes that broadly.

Example: alice and bob are both in tenant `t1`, role `hr`. With a `ROLE`-scoped Notion connector, alice's query warms the cache for bob.

### 3. Data freshness — connector declares the ceiling

Each connector declares `maxFreshnessTtl()` — the longest cached data is allowed to live. Driven by what the source supports (Google's search cursor expires after a few hours; Notion data changes faster than Drive metadata).

The query also passes `maxStaleness` — how stale the caller will tolerate.

Both are upper bounds. Effective TTL is the `min` of the two:

| Client says | Connector says | Effective | Why |
|---|---|---|---|
| 5 min | 6 h | 5 min | Client wants tighter — fine |
| 24 h | 6 h | 6 h | Connector ceiling wins — source's data isn't valid past 6 h |
| (none) | 6 h | 6 h | No client preference — use ceiling |
| 5 min | (none) | 5 min | No ceiling — use client target |

Clients can ask for fresher data than the connector allows (just causes a cache miss). Clients **cannot** ask for staler data than the connector allows — the source's validity contract is non-negotiable. Same pattern as HTTP `Cache-Control`: origin's `max-age` bounds any downstream cache.

### 4. Tag metadata rides along with rows

The connector attaches tag metadata to each row as it pulls data — either from the source's own labels, or from a platform-defined mapping. The connector itself does no access enforcement. Tags are just metadata; the actual access decision happens in the engine layer (see [Authorization](#authorization)).

Example: when the Drive connector pulls a policy doc owned by an HR member, it attaches the `hr` tag. The engine later decides whether the current caller's role can see rows with that tag.

### 5. Auth surface — connector handles the flow, platform handles the credential

The connector knows *how* its source authenticates (OAuth 2.0 for Drive, integration token for Notion, API key for a third-party). The platform owns the credential lifecycle (storage, refresh, rotation, audit). The connector receives an opaque `Credential` from the platform's `TokenStore` and applies it to the outbound call.

Example: at request time, the Drive connector calls `tokenStore.get(tenantId, userId, "google-drive")`, receives an OAuth access token, and attaches it as a `Bearer` header. If the source returns 401, the connector signals the store to refresh — the store decides *how* (refresh-token rotation, exponential backoff, etc.).

### 6. Today: raw HTTP. Production: SDK.

For the demo we use raw HTTP against Google's REST API. Visible code, fewer dependencies. In production, the official SDK is the right call — it handles OAuth refresh, retries with backoff, typed responses, field-mask helpers, and API version migration. Swap is well-scoped (~half a day per connector).

---

## Authentication

Authentication answers "who is the caller?" Ema needs to know the tenant, user, and role for every request so it can scope data, apply rate limits, and enforce access policy.

### Today (THP demo)

The demo uses **H2** (an embedded in-process database) and an in-memory `PrincipalRegistry` that maps username → `(tenantId, role)`. Principals are seeded at startup:

- `alice` → tenant `t1`, role `hr`
- `bob` → tenant `t1`, role `engineering`
- `carol` → tenant `t2`, role `hr`

Tokens (OAuth refresh tokens, connector API keys) live in an in-memory `TokenStore`.

### Why this works for the demo but not for production

H2 is single-process and embedded. One JVM, no replication, no sharing across instances. The moment you want HA or horizontal scale, this breaks.

### Production shape

Three concerns, three storage tiers — they don't belong in one box:

| Concern | Demo | Production | Why production differs |
|---|---|---|---|
| **Who is the user?** | H2 + `PrincipalRegistry` | Enterprise IdP (Okta, Azure AD, Google Workspace) via SAML/OIDC | Customers already own their identity store — Ema integrates, doesn't replicate. |
| **What credentials does the connector need?** | In-memory `TokenStore` | Vault / cloud KMS / Secrets Manager | Tokens need at-rest encryption, audit logs, rotation APIs. Co-locating with identity is a needless blast radius. |
| **What policies apply?** | In-code config | Postgres-backed admin UI with versioning, or a policy engine (OPA / Cedar) | Policies are authored by non-engineers and change without code releases. |

Migration from H2 to Postgres is JDBC-compatible — schema move, not a rewrite. The THP choice doesn't paint into a corner.

---

## Authorization

Authorization answers "is this caller allowed to see this row?" The connector tags rows; the engine decides whether to return them.

### Today: tag-based RBAC

Two things define access:

1. **Tags on rows** — attached by the connector at ingestion. Example: a Drive policy doc owned by an HR member gets the `hr` tag.
2. **Role → tag mapping** — declared in `TagAccessPolicy`. Example: role `hr` is allowed to read rows tagged `hr` or `public`; role `engineering` is allowed `engineering` or `public`.

The engine applies the row filter after the connector returns rows but before they go back to the caller. A row is dropped if none of its tags are in the caller's role's allow-list.

### Concrete example

Alice (tenant `t1`, role `hr`) queries `SELECT * FROM drive`. The Drive connector returns 20 rows tagged with `hr`, `engineering`, or `public`. The engine consults `TagAccessPolicy` for role `hr`, which allows `hr` and `public`. Engineering-only rows are dropped. Alice gets back 14 rows.

Bob (same tenant `t1`, role `engineering`) runs the same query. Same 20 source rows. Engine drops the `hr`-only ones. Bob gets back a different 14.

### Why tags and not native source ACLs (for this THP)

Tags are the simplest model that works across connector types. Some sources have native ACLs (Drive), some don't (a CSV file, a public API). Tags give a uniform mental model and let the demo work without integrating each source's permissions system.

### Production trade-offs

Tags work for small permission models. They fall over as permission groups grow:

- **Manual tagging doesn't scale.** Real enterprises have thousands of groups; hand-mapping is unsustainable.
- **Drift.** Source ACLs change without Ema knowing.
- **Auditor unfriendly.** Hard to answer "why did alice see this row?" with a single source of truth.

Two prod-shaped improvements, complementary not exclusive:

1. **Source-ACL pass-through.** For sources that have native ACLs (Drive, SharePoint), call them with the user's own OAuth token. The source returns only what the user can see — Ema never has to second-guess. Cache key becomes finer-grained (USER scope), but correctness is delegated to the source.
2. **Group-mapping sync from the IdP.** Reuse existing group memberships from Okta / Azure AD. Sync groups to tags automatically — "Okta group `hr-team` → tag `hr` on every doc the group owns." Removes manual tagging.

For overlays that the source doesn't know about ("HR sees all policy docs even if Drive ACL is open"), layer a small policy engine (OPA / Cedar) on top. Policies become versioned and auditable.

### Why H2 is fine for policy storage today

The role → tag mapping is small, rarely changes, and is read-only at request time. H2 works. In production, this moves into the same backing store as identity (either Postgres or a policy engine), versioned, with an admin UI for non-engineer authoring.

---

## HTTP surface

All knowledge of HTTP headers lives in one class — `org.emathp.web.HttpEnvelope`. The principle: **context lives in headers, payload lives in the body.** Everything downstream consumes `RequestContext` / `ResponseContext` and is HTTP-agnostic — to swap HTTP for gRPC, you'd rewrite this one class and `DemoWebServer.handleQuery`; nothing else.

### Request headers consumed

| Header | Becomes | Notes |
|---|---|---|
| `Content-Type` | JSON vs form parser switch | Standard HTTP |
| `Content-Length` | body size limit | Standard HTTP |
| `X-User-Id` | `UserContext.userId` | Header-first; falls back to body `mockUserId` / `demoUserId` when absent (keeps existing tests + curl scripts working) |
| `Cache-Control: max-age=N` | `FederatedQueryRequest.maxStaleness = Duration.ofSeconds(N)` | Standard HTTP framing; overrides body `maxStaleness` when both present |
| `Debug` | gates the debug response headers below | `Debug: true` enables; anything else is treated as false |

### Response headers — always set

| Header | Value |
|---|---|
| `Content-Type` | `application/json; charset=utf-8` (or `text/html` for browser fallback, `text/plain; version=0.0.4` for `/metrics`) |
| `X-Trace-Id` | server-generated UUID, also echoed in the body envelope and in every log line |
| `X-RateLimit-Status` | `OK` on successful + non-rate-limit-failure responses; `EXHAUSTED` on `RATE_LIMIT_EXHAUSTED` failures. Mirrors the body's `rate_limit_status` field. |

### Response headers — conditional

| Header | When | Value |
|---|---|---|
| `X-Cache-Status` | Success path | `HIT` if no provider call (full-materialization for joins, or all per-side caches hit for single-source); `MISS` otherwise |
| `X-Freshness-Ms` | Success with non-null body `freshness_ms` | Age in milliseconds of the freshest used chunk — matches the body `freshness_ms` field 1:1. Absent on zero-row responses or responses that touched no chunks. |
| `X-RateLimit-Scope` | `RATE_LIMIT_EXHAUSTED` failure that carries a scope | `USER` / `TENANT` / `CONNECTOR` — which bucket tripped (from `failure.violatedScope()`) |
| `Retry-After` | Failure with `RATE_LIMIT_EXHAUSTED` (HTTP 429) | seconds, e.g. `30` |
| `Location` | OAuth redirects | URL |

### Response headers — debug-only (caller sent `Debug: true`)

| Header | Source | Why it earns a header |
|---|---|---|
| `X-Snapshot-Path` | response body `snapshotPath` | Filesystem path to inspect this query's chunks — concrete developer action |
| `X-Query-Hash` | response body `queryHash` | Correlates request to log lines and the cache directory name |
| `X-Tenant-Id` | `RequestContext.tenantId` | Confirms which tenant the server resolved the caller to |
| `X-Role` | `RequestContext.scope().roleSlug()` | Confirms which `TagAccessPolicy` applied |

Identity headers (`X-Tenant-Id`, `X-Role`) are gated behind `Debug` so default responses don't leak principal resolution to callers; debug-mode is opt-in and intended for developers / operators.

### Where the `X-Cache-Status` signal comes from

Both query shapes already track this; `UnifiedSnapshotWebRunner` surfaces a top-level `cacheHit` boolean on every successful response, derived as:

| Query shape | Source | HIT means |
|---|---|---|
| Single-source | `summarizeSidesFetchMode(sides) == "all_cached"` | Every connector side served from chunks; no provider call |
| Join | `FullMaterializationCoordinator.Outcome.reusedFromDisk()` | Full assembled join result was reused from disk |

`HttpEnvelope.applyCacheStatusHeader` reads `body.cacheHit` and emits `X-Cache-Status: HIT|MISS`. The body itself keeps both `cacheHit` (top-level) and the more granular `serveMode` per side / `fullMaterializationReuse` for joins — header is the summary, body has the breakdown.

### What's not parsed as a header today (and why)

- **`Authorization: Bearer <jwt>`** — no JWT issuance in the demo; identity is `X-User-Id` (header) or `mockUserId` / `demoUserId` (body fallback). When an IdP is wired, the JWT validator runs at this same boundary and populates `UserContext`; the downstream code is unchanged.
- **`X-Tenant-Id` inbound** — tenant is derived from principal lookup, not caller-supplied. Avoids the "caller claims tenant X" trust question for the demo.
- **`If-None-Match` / `ETag`** — would be the right HTTP-native form for `cacheHit`-style conditional reads, but adds protocol complexity that doesn't earn its place at THP scope.

---

## Platform

Three components share the request lifecycle: the **planner** decides what each connector does natively, the **engine** drives execution and applies what the planner left behind, the **snapshot cache** stores results so the next caller doesn't repeat the work. Concurrency cuts across all three and is covered in its own section.

### Planner

The planner reads each connector's capability declaration and splits the SQL query into two parts:

- **`pushedQuery`** — what the connector can do natively (filtering, projection, sorting, pagination).
- **`residualOps`** — what's left for the engine to do after the connector returns rows.

#### Pushdown order is cascading

Decisions happen in logical order: **WHERE → ORDER BY → pagination**. Each step gates the next.

- If WHERE can't push (unsupported field or operator), ORDER BY can't push either — the connector would be sorting a row set that doesn't match what we'll keep, so its sort is meaningless.
- If ORDER BY can't push, pagination can't push — pagination requires a stable order.

Example: `SELECT * FROM notion WHERE updatedAt > '2024-01-01' AND title LIKE 'budget%' ORDER BY updatedAt DESC LIMIT 10`. Notion supports `updatedAt` filtering but not `title LIKE`. The planner pushes `WHERE updatedAt > '2024-01-01'`, keeps `title LIKE 'budget%'` as residual, can't push the sort (residual filter changes the row set), can't push pagination either. The engine pulls all matching pages from Notion, filters by `title LIKE`, sorts in-memory, applies `LIMIT 10`.

#### PROJECTION is independent

Field selection doesn't cascade. The planner pushes `SELECT title, updatedAt` to the connector whenever the connector supports projection, regardless of what happens upstream. Less data over the wire, lower latency, lower cost.

#### LIMIT is never pushed

`LIMIT` always runs in the engine — see [ADR-0003](adr/0003-connector-contract-excludes-limit.md). Reason: with residual filters present, the connector doesn't know how many rows will survive. Pushed `LIMIT 10` against a connector returns 10 rows pre-filter; the residual filter might drop 7, leaving the caller with 3 instead of 10. So LIMIT lives where it can see post-residual rows: in the engine.

#### Today: rules. Production: cost-based for cross-connector joins.

The current planner is rule-driven and reads connector capability booleans. That's the right call when each query has one connector — rules are legible, easy to debug, and sufficient for the demo.

The moment joins span connectors, "which side do I drive from" becomes a real question that rules can't answer well. Drive 100k Drive docs and look each up in Notion → bad. Drive 50 Notion entries and look each up in Drive → fine. The answer depends on estimated cardinality. Cost-based planning (cardinality stats + join selectivity estimation) is the prod move when cross-connector joins become common.

### Engine (Hybrid Runner)

The engine is the orchestrator. For each query it: drives the connector's page loop, applies residual ops (filter / sort / limit), applies the tag filter, returns rows.

The engine is also a **runner-selector**. Different queries need different execution shapes — the runner is picked per-query.

#### The runner interface

One execution surface, multiple implementations:

| Runner | When | What it does |
|---|---|---|
| **InProcessRunner** | Small queries, single connector | Single thread per connector side. Today's default. |
| **ParallelConnectorRunner** | Joins, multiple connectors, fits in memory | Fan out connector calls across threads; combine in-memory. |
| **DistributedRunner** | Analytic-scale (TB joins, large aggregations) | Hand off to Spark / Flink / DuckDB-cluster. |

The planner picks based on estimated cardinality + connector capabilities + query shape (join arity, aggregation presence). Selection becomes one method: `Planner.selectRunner(plan, stats)`.

#### Intelligent partitioning

Stateless executors are the seam that makes distributed runners possible. The runner emits **work units** (range scans, page batches, sub-joins) that any worker can pick up. Workers don't coordinate; the runtime decides where each work unit lands.

This works when the connector supports range-bounded cursors (e.g. `cursor BETWEEN x AND y`). For connectors that only support sequential paging (offset or next-token), partitioning falls back to "one worker per connector side."

#### Parallelisation in joins

Today: connector sides in a join run sequentially. With ParallelConnectorRunner, left and right sides start at the same time; results combine when both finish. Latency drops to `max(leftSide, rightSide)` instead of `leftSide + rightSide`.

This only helps when sides are independent (no nested-loop dependency). A cost-based planner picks broadcast-join vs nested-loop based on cardinality.

#### Streaming vs materialize

Today: materialize fully then return. Simpler. Worst case = full result set in memory.

Streaming is the prod move for very large result sets, but it complicates joins (you'd need a streaming join operator) and complicates the cache (you can't write a chunk you haven't fully seen). InProcessRunner stays materialized; DistributedRunner is streaming by definition (Spark/Flink handle it).

#### Graduation path

The Hybrid Runner is the path from "Java process" to "real data platform." Same plan flows through, runner choice changes. A query that runs in 50ms on InProcessRunner today and 500ms on a 1B-row join tomorrow lands on DistributedRunner — same SQL, same planner, different runner. The engine isn't rewritten; it's *layered*.

### Snapshot cache

The snapshot cache stores connector-side results on disk so the next caller doesn't pay the provider call cost.

#### Layout

One directory per `(queryRoot, connector)`. Each directory holds chunks (data files) plus metadata files. Chunk metadata records the row range, `createdAt`, `freshnessUntil`, the cursor for the next page, and the page size.

#### Cache key uses connector data scope

The directory path includes scope-derived segments — see [Connector / Data scope](#2-data-scope--drives-cache-sharing). USER scope = `(tenant, role, user)` segments; ROLE = `(tenant, role)`; TENANT = `(tenant)`; PUBLIC = global. Broader scope = more sharing across callers, but only safe when the source actually scopes that broadly.

#### Freshness: min of client and connector

Both `clientMaxStaleness` and `connector.maxFreshnessTtl()` are upper bounds on cache age. Effective TTL = `min(both)`. The lookup check is `now < min(created + maxStaleness, stamped freshnessUntil)`. Detailed table in [Connector / Data freshness](#3-data-freshness--connector-declares-the-ceiling).

#### Atomicity

Today: in-process single-flight (see [Concurrency](#concurrency)) gives "one writer per `connectorDir` at a time" within a single JVM. Multi-process or crash-during-write is not handled.

Production fix: **atomic rename**. Write to a sibling staging directory (`connectorDir.tmp-<uuid>`), then `Files.move(staging, connectorDir, ATOMIC_MOVE, REPLACE_EXISTING)`. Filesystem rename is atomic on the same volume — readers see either old or new, never partial. ~30 LOC change.

#### Production gaps beyond what the demo does

Today the cache is plain JSON chunks on disk. Production needs:

- **Serialization upgrade.** JSON → columnar (Parquet/ORC). Order-of-magnitude faster scan, smaller on disk.
- **Compression.** zstd or lz4 — typical 3–5× size reduction for row data.
- **Encryption at rest.** Per-tenant keys via cloud KMS. Required for SOC2 and enterprise contracts.
- **Eviction / size cap.** Today disk grows unbounded. Prod = per-tenant disk budget + LRU eviction + TTL-based sweep.
- **Integrity / checksums.** Each chunk carries CRC32 or SHA. Corrupt chunk = lookup miss + alert (today: silent return of garbage).
- **Schema versioning.** Chunks tagged with format version. Readers ignore versions they don't understand. Without this, every format change requires a global flush.
- **Audit log.** Compliance regimes (SOC2, GDPR) want a record of "who read what cached row when."

#### Distributed cache — two layers, not one

| | Object store (S3 / GCS) | Distributed memory (Redis / Memcached) |
|---|---|---|
| Latency | Tens of ms | Sub-ms |
| Durability | Strong (multi-AZ replication) | Weak (restart loses state) |
| Cost | Cheap per GB | Expensive per GB |
| Fits for Ema | Chunk bodies (large, slow-changing) | Metadata index (small, hot) |

Production shape is **both, layered**. Object store is the source of truth for chunks. Redis is the metadata index — "for `(tenant, role, queryHash)`, which chunks exist and when do they expire?" Cache lookup is a sub-ms Redis check followed by an object-store fetch only on hit. Cache miss = both layers miss.

This avoids the false choice between "fast and volatile" and "durable and slow" — each layer solves a different problem.

---

## Concurrency

Concurrency cuts across the request lifecycle: HTTP intake, rate limiting, cache lookup, single-flight on miss. This section covers the parts that aren't obvious from reading any one file.

### HTTP intake

The JDK `HttpServer` is configured with `backlog=200` and a fixed-size 16-thread executor. The defaults (`backlog=0` ≈ 50, single-threaded executor) caused 4.7% connection failures at 100 RPS in k6 testing. Tuning brought failures to 0.17%, then to 0 after fixing the races below.

### Two-layer rate limit

Two limiters in series, each protecting a different thing:

| Layer | Where | Buckets | Protects |
|---|---|---|---|
| **Service** | `FederatedQueryService.execute` (entry point) | `user`, `tenant` | Ema's own SLO — fires on every request, including cache hits |
| **Connector** | Inside the per-page loop | `connector` (per source) | Upstream provider quotas — fires only on provider calls |

The service layer was added because a single-layer (connector-only) design let cache hits silently bypass rate limiting — a tenant could burst 1000 RPS at Ema as long as the cache absorbed it, violating Ema's own throughput contract. Two layers make Ema honest about both its own SLO and the source's quota.

### Single-flight on cache miss

When 16 threads simultaneously miss the cache for the same `connectorDir`, only one fetches from the provider; the other 15 wait on the same `CompletableFuture` and reuse its result.

Implementation: a static `ConcurrentHashMap<Path, CompletableFuture<SidePageResult>>`. The first thread to call `computeIfAbsent` for a given path installs the future and runs the fetch + write. Concurrent threads find the same future and call `.get()` on it. The winner cleans up via `inFlight.remove(connectorDir, mine)` in a `finally` block.

This collapsed observed redundant provider calls from 28 → 1 under a 100 RPS burst. The pattern also eliminates the second race (below) as a side effect.

### Two races this fixed

Before single-flight + tuned HTTP pool, k6 surfaced two races that the previous single-threaded executor had masked:

1. **Thundering herd**: 16 concurrent threads see "no chunk exists," all 16 fetch from the provider, all 16 write to disk. 28 redundant provider calls observed.
2. **Disk write TOCTOU**: in the cache-miss branch, `store.exists(...) → deleteRecursively → createDirectories → writeChunk` is non-atomic. Thread A's delete interleaved with Thread B's create caused 19 INTERNAL errors / 11062 requests (0.17%).

Single-flight eliminated both: only one thread is ever in the write branch per `connectorDir`, so neither race can fire. Atomic rename (described in [Snapshot / Atomicity](#atomicity)) would also fix race 2 and is still wanted for multi-process safety.

### Invariant worth alerting on

Per connector: `cache_misses == provider_calls`.

- If `cache_misses > provider_calls`: single-flight is broken (someone bumped misses without going to the provider).
- If `cache_misses < provider_calls`: something is calling the provider outside the pipeline.

Either inequality is a useful alert.

---

## Operability

*To be written.* Topics: 8 Prometheus metrics + `/metrics` endpoint, `traceId` end-to-end, uniform error envelope with `ErrorCode`, alerts worth wiring (`cache_misses > provider_calls`, `rate_limit_denied` per-tenant spikes, p99 latency per connector).

## Deliberately out of scope

*To be written.* What's missing and why it's OK for the THP:

- Atomic-rename for multi-process disk safety (in-process single-flight covers this scope)
- Write path / schema evolution
- OTel distributed tracing (the seam is in place; exporter is config)
- Real connector marketplace
- Refresh-token rotation audit log
- HA / horizontal scaling (single-JVM is the THP boundary)

## Six-month plan

*To be written.* Order, with each step anchored to a customer milestone:

- **M1–2:** Object-store snapshot backend + atomic rename → unblocks multi-process correctness, sets up HA.
- **M3–4:** Cost-based planner with cross-connector joins → unblocks first complex enterprise customer.
- **M5–6:** Connector SDK + marketplace pattern; OTel exporter; SLO dashboards; first paying-customer onboarding playbook.
