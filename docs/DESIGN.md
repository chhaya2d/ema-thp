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
- [Testing](#testing)
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

### 6. Raw HTTP today; SDK is the prod move

The Drive connector uses raw HTTP rather than Google's SDK — fewer dependencies, request/response shape visible in code. See [Out of scope §Connectors](#connectors-and-source-integration) for the SDK trade-off detail.

---

## Authentication

Authentication answers "who is the caller?" Ema needs to know the tenant, user, and role for every request so it can scope data, apply rate limits, and enforce access policy.

The demo uses **H2** (an embedded in-process database) and an in-memory `PrincipalRegistry` that maps username → `(tenantId, role)`. Principals are seeded at startup:

- `alice` → tenant `t1`, role `hr`
- `bob` → tenant `t1`, role `engineering`
- `carol` → tenant `t2`, role `hr`

Tokens (OAuth refresh tokens, connector API keys) live in an in-memory `TokenStore`. All three stores are **single-JVM only**.

---

## Authorization

Authorization answers "is this caller allowed to see this row?" The connector tags rows; the engine decides whether to return them.

Demo supports tag-based RBAC

Two things define access:

1. **Tags on rows** — attached by the connector at ingestion. Example: a Drive policy doc owned by an HR member gets the `hr` tag.
2. **Role → tag mapping** — declared in `TagAccessPolicy`. Example: role `hr` is allowed to read rows tagged `hr` or `public`; role `engineering` is allowed `engineering` or `public`.

The engine applies the row filter after the connector returns rows but before they go back to the caller. A row is dropped if none of its tags are in the caller's role's allow-list.

### Concrete example

Alice (tenant `t1`, role `hr`) queries `SELECT * FROM drive`. The Drive connector returns 20 rows tagged with `hr`, `engineering`, or `public`. The engine consults `TagAccessPolicy` for role `hr`, which allows `hr` and `public`. Engineering-only rows are dropped. Alice gets back 14 rows.

Bob (same tenant `t1`, role `engineering`) runs the same query. Same 20 source rows. Engine drops the `hr`-only ones. Bob gets back a different 14.

### Why tags

Tags are the simplest model that works across connector types. Some sources have native ACLs (Drive), some don't (CSV, public API). Tags give a uniform mental model that lets every connector participate in row-level RBAC without integrating its source's permissions system.

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

`LIMIT` always runs in the engine
#### Rules today; cost-based is a gap

The current planner is rule-driven and reads connector capability booleans — legible, easy to debug, sufficient for single-connector queries. Cross-connector joins (which side drives, hash vs sort-merge vs broadcast) need cardinality stats and a cost model — see [Out of scope §Query capabilities](#query-capabilities).

### Engine

The engine is the orchestrator. For each query it drives the connector's page loop, applies residual ops, applies the tag filter, returns rows.

#### Per-side page loop

`SingleSourceSidePipeline` calls `connector.search(...)` in a loop until LIMIT is satisfied (post-residual) or the connector returns no next cursor. Each page is filtered + sorted + tag-filtered in memory before the next page is requested.

#### Residual operations

Whatever the planner couldn't push runs here, in order: residual WHERE → residual ORDER BY → tag filter → LIMIT. LIMIT runs last because residual filters change the row count — it must see post-filter rows or it would truncate too early.

#### Joins

`FullMaterializationCoordinator` runs both sides of a join sequentially, materializes the result sets in memory, hash-matches them, and writes the combined output as a single chunk for reuse. One join strategy (full materialize + hash-match) — see [Out of scope §Query capabilities](#query-capabilities) for the multiple-strategy + cost-based-planner gap. The join TTL is `min(client.maxStaleness, left.maxFreshnessTtl, right.maxFreshnessTtl)`.

### Snapshot cache

The snapshot cache stores connector-side results on disk so the next caller doesn't pay the provider call cost.

#### Layout

One directory per `(queryRoot, connector)`. Each directory holds chunks (data files) plus metadata files. Chunk metadata records the row range, `createdAt`, `freshnessUntil`, the cursor for the next page, and the page size.

#### Cache key uses connector data scope

The directory path includes scope-derived segments — see [Connector / Data scope](#2-data-scope--drives-cache-sharing). USER scope = `(tenant, role, user)` segments; ROLE = `(tenant, role)`; TENANT = `(tenant)`; PUBLIC = global. Broader scope = more sharing across callers, but only safe when the source actually scopes that broadly.

#### Freshness: min of client and connector

Both `clientMaxStaleness` and `connector.maxFreshnessTtl()` are upper bounds on cache age. Effective TTL = `min(both)`. The lookup check is `now < min(created + maxStaleness, stamped freshnessUntil)`. Detailed table in [Connector / Data freshness](#3-data-freshness--connector-declares-the-ceiling).

#### Atomicity

In-process single-flight (see [Concurrency](#concurrency)) gives "one writer per `connectorDir` at a time" within a single JVM. Concurrent missers for the same key collapse onto one `CompletableFuture` — the winner fetches and writes; the rest read the result via the future and never touch disk. 

*Code: `org.emathp.snapshot.adapters.fs.FsSnapshotStore`, `org.emathp.snapshot.pipeline.SingleSourceSidePipeline` (single-flight + freshness).*

#### Data format

Plain JSON on disk today, no compression, no encryption.

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

The system is designed to be debuggable at 3 a.m. without source-code access. Every request has a stable trace identity, every failure has a typed code, every behavior of interest is a counted metric.

### Metrics — `/metrics` endpoint, Prometheus text format

Eight counters and histograms, exposed at `GET /metrics` (no auth, hand-rolled — no `simpleclient` dep). Each one answers a specific operational question:

| Metric | Type | Answers |
|---|---|---|
| `emathp_provider_calls_total{connector,outcome}` | counter | "Is each connector behaving? What's the ok/error split?" |
| `emathp_provider_call_duration_seconds{connector}` | histogram | "Is connector X getting slower?" — drives p50/p95/p99 alerts |
| `emathp_snapshot_cache_hits_total{connector}` + `_misses_total` | counter | "Is the cache effective? What's the hit ratio per source?" |
| `emathp_snapshot_stale_restarts_total` | counter | "How often is `maxStaleness` actually invalidating chunks?" |
| `emathp_response_freshness_ms` | histogram | "What's the distribution of data age served to clients?" |
| `emathp_planner_pushdown_total{connector,op}` + `_residual_total` | counter | "Is pushdown working? Which ops are escaping to the engine?" |
| `emathp_query_errors_total{code}` | counter | "What's failing? `RATE_LIMIT_EXHAUSTED`, `BAD_QUERY`, `INTERNAL`?" |
| `emathp_rate_limit_denied_total{scope}` | counter | "Which bucket is tripping? `USER`, `TENANT`, `CONNECTOR`?" |

All counters/histograms are static singletons on `org.emathp.metrics.Metrics`; call sites are one-liners (`Metrics.PROVIDER_CALLS.inc(connector.source(), "ok")`). Cumulative values; per-minute rates come from a Prometheus scrape diffing consecutive samples (the standard pattern). Sample post-burst output: [`docs/sample-output/metrics-after-k6.txt`](sample-output/metrics-after-k6.txt).

### Trace identity end-to-end

Every request gets a server-generated UUID at the HTTP boundary (`HttpEnvelope.parse` → `RequestContext.traceId`). The same UUID appears in:

- **`X-Trace-Id` response header** — set by `HttpEnvelope` on every response (success and failure)
- **JSON response envelope `traceId` field** — for body-only consumers
- **`logs/web-query-trace.log`** — one structured line per successful query (`traceId=...`, `sql=...`, `queryHash=...`, `snapshotPath=...`, per-side `pushedSummary` / `serveMode`)
- **`ResponseContext.Outcome.Failure`** — failure envelope carries the same `traceId`

Result: an on-call engineer sees a 5xx in their dashboard, copies the `X-Trace-Id`, greps the trace log and the structured logs, and lands on the exact request shape + cache state + connector outcome. Single ID, three surfaces. Sample: [`docs/sample-output/trace-log-sample.log`](sample-output/trace-log-sample.log).

### Uniform error envelope

All failures cross the service boundary as `ResponseContext.Outcome.Failure(code, message, retryAfterMs, violatedScope)`. The HTTP layer maps this to a stable JSON shape:

```json
{
  "ok": false,
  "code": "RATE_LIMIT_EXHAUSTED",
  "message": "...",
  "traceId": "abc-123",
  "rate_limit_status": "EXHAUSTED",
  "retryAfterMs": 30000,
  "violatedScope": "USER"
}
```

Plus status code from `ErrorCode.httpStatus()` (e.g. `429` for `RATE_LIMIT_EXHAUSTED`, `400` for `BAD_QUERY`) and `Retry-After` / `X-RateLimit-Scope` response headers when applicable. Clients branch on the typed `code`, not on free-form message strings.

### Invariants worth alerting on

These are the assertions a Grafana / PromQL alert should fire on:

| Alert | Threshold | What it tells you |
|---|---|---|
| `cache_misses_total != provider_calls_total` per connector | any non-equal value over a 5-min window | single-flight is broken **or** provider is being called outside the pipeline. Either way, a bug. |
| `rate_limit_denied_total{scope="USER"}` rate spike per tenant | > 10× baseline over 1 min | abusive tenant or runaway client, distinct from a normal burst |
| `provider_call_duration_seconds` p99 per connector | > tenant SLA (e.g. 2s for Drive) | upstream degrading; cache TTL might need tightening |
| `query_errors_total{code="INTERNAL"}` rate | any non-zero rate sustained | unhandled exception path; warrants a paging alert |

The first invariant is the most useful — it's a property of the architecture, not a threshold. If it ever breaks, something fundamental is wrong (single-flight collapse failed, or someone added a code path that bypasses `SingleSourceSidePipeline`).

### Debugging walkthrough — what an on-call sees

1. Dashboard alert: `cache_misses` climbing while `provider_calls` stays flat for the `notion` connector.
2. Open the metrics log: confirm `emathp_snapshot_cache_misses_total{connector="notion"}` is 3x `emathp_provider_calls_total{connector="notion"}` over 10 min.
3. Grep `web-query-trace.log` for `connector=notion` in that window — find one structured line per request with `snapshotReuseNoProviderCall=false`.
4. Pick any `traceId`, search server logs for that ID, find the stack from `SingleSourceSidePipeline.execute()`.
5. The line bumping `Metrics.SNAPSHOT_CACHE_MISSES` should also call `executor.execute(...)` → if it isn't, the bug is right there.

The whole flow is `X-Trace-Id` plus four files. No source needed beyond that.

*Code: `org.emathp.metrics.Metrics`, `org.emathp.web.WebQueryTraceLogger`, `org.emathp.web.ErrorResponder`.*

## Testing

Four layers of coverage, each verifying a different surface. No single layer claims to prove the whole system — they compose.

| Layer | Where | What it verifies | Run |
|---|---|---|---|
| **Unit** | `src/test/java/.../*Test.java` per package | Per-class behavior in isolation — planner rules, freshness policy, token-bucket math, etc. | `gradlew test` (all) |
| **Integration (service-layer)** | [`ShowcaseTest`](../src/test/java/org/emathp/showcase/ShowcaseTest.java) | End-to-end through `service.execute()` — planner → engine → snapshot → tag filter → rate limit → `ResponseContext`. 12 narrated tests with display names; assertions on `ResponseContext` fields = wire-contract assertions. | `gradlew test --tests "org.emathp.showcase.ShowcaseTest" -i` |
| **HTTP serialization** | [`HttpEnvelopeTest`](../src/test/java/org/emathp/web/HttpEnvelopeTest.java) | `ResponseContext` → response headers, request headers → `RequestHeaders`. 33 unit tests; covers every header in the [HTTP surface](#http-surface). | Bundled in `gradlew test` |
| **Load & concurrency** | [`scripts/k6-burst.js`](../scripts/k6-burst.js) | 100 RPS for 2 min — rate-limit behavior, single-flight cache fill, the `cache_misses == provider_calls` invariant under burst. | `k6 run scripts/k6-burst.js` (server must be running) |

### The coverage logic

The interesting claim is that **ShowcaseTest + HttpEnvelopeTest jointly verify the HTTP wire contract**, even though neither one fires real HTTP. The reasoning:

1. `ResponseContext` is the typed canonical representation of every response header (see [HTTP surface](#http-surface)).
2. `ShowcaseTest` asserts on `ResponseContext` fields after service execution → proves the service produces correct values for `cacheStatus`, `freshnessMs`, `rateLimitStatus`, `debug.snapshotPath`, etc.
3. `HttpEnvelopeTest` asserts that those same `ResponseContext` fields serialize to the correct HTTP header values.
4. Composition: service produces correct `ResponseContext` AND envelope serializes it correctly ⇒ the wire response carries the correct headers.

No integration test stands up a real HTTP server, fires real requests, and asserts on real headers — that gap is acknowledged but doesn't add coverage the composition above doesn't already provide. The cost (~150 LOC + ephemeral-port harness) outweighs the marginal signal for THP scope.

### What's deliberately not tested

- **Real browser interaction** — manual walkthrough (see [README §2 Real UI](../README.md#2-real-ui--the-demo-web-server)), not Playwright/Selenium automation.
- **OAuth flow against live Google** — needs creds + network, not CI-friendly. The token-store contract is unit-tested (`GoogleTokenStoreTest`); the OAuth code path is exercised manually.
- **Multi-process correctness** — single-JVM is the THP boundary; multi-process scenarios are noted in [Deliberately out of scope](#deliberately-out-of-scope).

*Code: see test directory structure under `src/test/java/org/emathp/`.*

## Deliberately out of scope

What's missing and why it's OK at THP scope. Most of these are seams the architecture already supports — the code lives behind interfaces (`Connector`, `TokenStore`, `PrincipalRegistry`, `RateLimiter`, `SnapshotStore`) that swap rather than refactor.

### Connectors and source integration

**SDK over raw HTTP.** The Drive connector uses raw HTTP against the REST API; production = official SDK (Google, Notion, etc.) — handles OAuth refresh, retries with backoff, typed responses, field-mask helpers, API version migration. Swap is well-scoped (~half a day per connector) but adds dependency weight; raw HTTP keeps the demo's request/response shape visible in code.

**Typed connector error taxonomy + internationalization.** Today connectors throw raw exceptions that bubble up as `ErrorCode.INTERNAL`; production = each connector declares typed errors with `retryable` and `scope` so the engine routes them correctly (auth-expired → refresh; throttled → backoff; upstream 5xx → retry). Error messages are English-only; production = `Accept-Language`-aware localization. Same surface, two separate gaps.

### Identity, authn, credentials

**Caller-to-Ema authentication.** Today there's no platform-level authn: `X-User-Id` request header is trusted, body `mockUserId` is trusted. Production = `Authorization: Bearer <jwt>` issued by the customer's IdP (Okta / Azure AD / Google Workspace), validated at `HttpEnvelope.parse`, populates `RequestContext`. Distinct from connector auth (Ema-to-source); the demo handles only the second.

**Credential flow as deployment choice.** Server-stored tokens today (in-memory `TokenStore`). For high-compliance verticals (finance, healthcare, government) where vendor credential storage is contractually prohibited, the alternative is BYO-token mode — caller supplies short-lived access tokens per request; Ema persists nothing. The `TokenStore` interface supports both impls; only the server-stored one ships.

### Authorization

**Cell-level security with a data catalog.** Today: row-level RBAC via tags — engine drops whole rows the caller's role isn't allowed. Production for healthcare/finance/government = cell-level encryption with per-classification keys (PII / PHI / financial) fetched from KMS, gated by role lookup. Requires a **data catalog** ("Drive's `ssn` column is PII") to know which cells get which keys — that's a separate platform missing from the demo entirely.

### Storage and cache

**Distributed cache backend.** Local disk today; single-host only. Production = Redis as the metadata index ("which chunks exist for this `(tenant, role, queryHash)`, when do they expire?") + object store as the chunk body. Object store alone is wrong — S3 RTT (~50–200ms) can exceed Drive / Notion API latency, making the cache anti-value. Two layers, each solving a different problem.

**Cache hygiene at rest.** Plain JSON on disk today, no compression, no encryption. Production = columnar (Parquet / ORC) for scan speed + zstd compression + per-tenant KMS encryption (SOC2 and enterprise contracts require it). Plus eviction (per-tenant disk budget + LRU), integrity checksums, schema versioning, and an audit log of cache reads — all standard cache hygiene the demo skips for simplicity.

### Query capabilities

**Cost-based planning + multiple join strategies.** Rules-based planner today; single full-materialization join strategy (both sides into memory, hash-match). Production = cardinality estimation + per-query selection of hash / sort-merge / broadcast / nested-loop based on cost. Hardcoded rules break the moment a join is 100k Drive docs × 50 Notion entries — the planner needs to pick the driving side from stats.

**Write path / DDL / DML.** Read-only by design. Adding writes opens idempotency keys, optimistic locking, write-through cache invalidation, audit-log immutability — a whole second architecture parallel to the read path. Out of scope at THP scale; the right v1 cut.

### Scale and decomposition

**Multi-process / horizontal scaling.** Single JVM bounds everything today: H2 for OAuth tokens, in-memory `PrincipalRegistry`, in-memory `TokenBucket` rate limiter, in-memory `TagAccessPolicy`, local-disk snapshot cache, single-instance metrics registry. Production = cluster-aware replacements for each (Redis-backed rate limiter, Postgres-backed principals, KMS/Vault-backed tokens, the distributed cache above, scraped/aggregated metrics). The seams to swap exist; the wiring is single-instance.

**Distributed processing (Spark / Flink) for analytic-scale.** Today's engine materializes everything in-process — fine for OLTP-shaped queries (50ms, hundreds of rows). Production at analytic scale (TB joins, large aggregations, complex windowing) = hand off the plan to a distributed runtime: emit `pushedQuery` + `residualOps` as work units, let Spark / Flink / DuckDB-cluster execute, stream results back. Same planner, same connector contracts, different runner. A hybrid runner-selector picks per-query based on estimated cardinality so small queries stay in-process and only the analytic ones pay the distributed-engine startup cost.

### Observability

**OTel distributed tracing.** Today: server-generated `X-Trace-Id`, propagated through logs + body + response headers. Production = W3C Trace Context (`traceparent` request header accepted from callers, propagated into outbound connector calls, exported to Jaeger / Tempo / Honeycomb / DataDog). The seam is in place; only the exporter and inbound parse are missing.

**Operational hygiene gaps.** Three smaller items grouped together:

- **Log rotation.** `logs/web-query-trace.log` grows unbounded; production = logback / log4j with size+time rolling + retention policy (90 days hot, 7 years cold for compliance verticals).
- **Refresh-token rotation + audit log.** Google rotates refresh tokens on use; we read but don't rotate. Production = rotation handling, key versioning, audit log of credential accesses ("who fetched the Drive token at 03:47Z").
- **Denser metrics.** 8 metrics today, mostly global or per-connector. Production = 30+ with cardinality budgets — per-tenant breakdowns of every counter, executor queue depth, per-planner-phase timings, per-connector latency histograms with quantiles.

### Stepping back: not a monolith

Most of the gaps above land on the same conclusion — Ema in production isn't one JVM, it's a small fleet of services (HTTP gateway, query engine, connector pool, cache service, rate limiter, token store, observability pipeline) decomposed along the seams the THP already establishes. Same code, more boxes, with shared infrastructure (Redis, object store, KMS, OTel collector) replacing the in-process structures. This document maps the seams; the productionization map is a separate doc.

## Six-month plan

*To be written.* Order, with each step anchored to a customer milestone:

- **M1–2:** Object-store snapshot backend + atomic rename → unblocks multi-process correctness, sets up HA.
- **M3–4:** Cost-based planner with cross-connector joins → unblocks first complex enterprise customer.
- **M5–6:** Connector SDK + marketplace pattern; OTel exporter; SLO dashboards; first paying-customer onboarding playbook.
