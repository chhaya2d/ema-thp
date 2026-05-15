# EmaTHP — federated query layer

This README summarizes how connectors, pushdown, snapshots, tests, and the demo web fit together.

## Connector model

- **`Connector`** (`src/main/java/org/emathp/connector/Connector.java`) is the integration boundary: `source()`, **`capabilities()`**, **`defaultFetchPageSize()`**, and **`search(UserContext, ConnectorQuery)`**.
- **`CapabilitySet`** (`connector/CapabilitySet.java`) advertises what the provider can do: filtering, projection, sorting, pagination, **`supportedFields`**, and **`supportedOperators`**. The **planner** uses these booleans and sets to decide **what may be pushed** to the provider vs executed in the engine.

## Provider batch size (cost / performance)

- Each connector exposes **`defaultFetchPageSize()`**: the **maximum rows per provider `search` call** (batch size). The engine may loop pages until residuals + LIMIT are satisfied.
- This **controls API volume** against SaaS limits and latency; it is **not** the SQL `LIMIT`. **`LIMIT` is always enforced in the engine** (see ADR-0003 / `Planner` javadoc).

## Google Drive: three implementations

| Implementation | Package | Role |
|----------------|---------|------|
| **Real** | `connector/google/real/` | **`RealGoogleDriveConnector`**: OAuth (PKCE), **`GoogleApiClient`**, tokens loaded/stored via **`GoogleTokenStore`**. |
| **Mock** | `connector/google/mock/` | **`GoogleDriveConnector`** + **`MockGoogleDriveApi`**: in-memory fixture; full **eight-row** corpus per principal for pagination tests; delay via **`MockConnectorDefaults`** (`MockConnectorDevSettings.compiled()` on the demo server). |
| **Demo** | `connector/google/demo/` | **`DemoGoogleDriveConnector`** + **`DemoGoogleDriveApi`**: shared eight-file corpus for every user; parallel **`DEMO_FILE_TAGS`** (same order as files) supplies **`hr`** / **`engineering`** on rows; **`DemoConnectorDefaults`** for delay and page size. |

Notion follows the same pattern: **mock** (`notion/mock`) and **demo** (`notion/demo`) translators/APIs align titles with the demo Drive corpus for join experiments.

## OAuth token storage

- Refresh/access tokens for **real Google** are stored in **PostgreSQL** (default JDBC from env) or **in-memory H2** when **`EMA_DEV_H2=true`** (`WebEnv` / `DemoWebServer`).
- **`CONNECTOR_TOKEN_KEY`** derives the AES key used by **`TokenEncryptor`** at rest (`oauth/TokenEncryptor.java`).
- The **demo web** optionally initializes the **live** stack at startup only when **`CONNECTOR_TOKEN_KEY`**, **`GOOGLE_OAUTH_CLIENT_ID`**, and **`GOOGLE_OAUTH_CLIENT_SECRET`** are all set **and** DB init succeeds; otherwise **mock** and **demo** modes still run.

## Pushdown vs engine (`Planner`)

- Pushdown is decided in **logical order**: **WHERE → ORDER BY → (pagination cursor + connector batch size)**. **`PROJECTION`** is separate capability-driven.
- **`LIMIT` is never pushed** to connectors; it remains **`QueryExecutor`** / engine enforced.
- If **WHERE** cannot be pushed (unsupported field/operator or filtering off), **ORDER BY** and **pagination** cascade to **residual** where gated by the rules (see `planner/Planner.java` javadoc).
- Residual **WHERE** / **ORDER BY** run in-process over fetched rows (`engine/QueryExecutor.java`, `model/ResidualOps.java`).

## Identity, authz, principals

- **`UserContext`** (`auth/`) carries the caller's `userId` only; `null` / blank = anonymous.
- **`Principal`** (`authz/Principal.java`) holds tenant, primary role, role list, and per-role allowed tag set. Produced by a **`PrincipalRegistry`** (`authz/PrincipalRegistry.java`) — the single seam between the engine and "who is this caller."
- **`authz/demo/DemoPrincipalRegistry`** is the in-memory alice/bob fixture (tenant `tenant-1`, roles `hr` / `engineering`). Swap with a JDBC / claims-backed impl for production — no other code changes.
- **`ScopeAndPolicy`** (`authz/ScopeAndPolicy.java`) derives `QueryCacheScope` and `TagAccessPolicy` from a `Principal`; engine and snapshot code never see `Principal` directly.
- **`PrincipalRegistry.UNRESTRICTED`** returns `Principal.anonymous()` for every caller — used by snapshot / pagination tests that don't exercise authz.

## Snapshots, cache scope, freshness

- **`QueryCacheScope`** (`cache/QueryCacheScope.java`) namespaces snapshot paths by **tenant**, **role**, **user**, and a **key schema version** (`snapshotScopeDirectoryName()`). Demo mode resolves tenant/role/tags via **`DemoPrincipalRegistry`** (in `authz/demo/`); the UI only picks **user id**.
- **Tag policy** runs in the engine **after** residual WHERE/sort and **before** `LIMIT`. Persisted chunks therefore hold **post-tag-filter** rows (“cache after tag filter”), so switching role or allowed tags yields different snapshot segments without leaking filtered rows through disk reuse.
- Demo connectors attach optional **`tags`** on rows (parallel tag lists in each demo API file); rows with missing or empty tags are **permissive**; when the active role has **allowedTags**, the row must intersect that set.
- The web API accepts **`maxStaleness`** (ISO-8601 duration, e.g. `PT10M`) to bound reuse of snapshot materializations; default chunk TTL comes from **`WebDefaults.snapshotChunkFreshness()`** when omitted.

## Rate limiting

Two-layer model. The buckets sit at different points in the request flow and protect different things:

| Layer | Where it fires | Buckets configured | Purpose |
|---|---|---|---|
| **Service** | `DefaultFederatedQueryService.execute()` entry, before SQL parse | **user + tenant** (connector skipped) | Ema's own SLO + per-user fairness. Fires on *every* request including cache hits. |
| **Connector** | `QueryExecutor.runPageLoop`, before each `connector.search(...)` | **connector** (user + tenant skipped) | Upstream provider quota protection. Fires only on actual provider calls. |

- **`RateLimitPolicy`** (`ratelimit/RateLimitPolicy.java`) — engine-facing interface: `tryAcquire(RequestContext) → RateLimitResult`. Implementations: **`HierarchicalRateLimiter`** (real, configurable token-bucket per scope) and **`RateLimitPolicy.UNLIMITED`** (no-op, used by tests that don't exercise rate limiting).
- **Nullable scopes**: `HierarchicalRateLimiterConfig` accepts `null` for any of `(connector, tenant, user)` — the limiter skips null-configured scopes. The two factory methods `forService(tenant, user)` and `forConnector(connector)` build the two layers' configs.
- **AND semantics per layer**: at the connector layer, just one bucket (connector); at the service layer, the request must pass *both* user and tenant. Denial in any bucket fails the request; earlier buckets refunded (best-effort).
- **Cache hits still pay the service tax**: this is the key change from a single-layer model. A user hammering identical cached queries still gets throttled per-user — cache absorbs upstream load but the service layer protects Ema's own capacity.
- **Anon short-circuit**: when `RequestContext.isAnonymous()` is true (no tenantId or no userId), both layers skip the check entirely. CLI demos and snapshot tests therefore run unlimited.
- **Demo config** — `DemoWebServer.demoServiceRateLimitConfig()` and `demoConnectorRateLimitConfig()`:
  - Service: **tenant 30 rps / burst 60**, **user 5 rps / burst 10** (user is the tightest — typical for SaaS pricing tiers).
  - Connector: **30 rps / burst 60** shared across connectors. Production would per-connector this (Notion's real cap is 3 rps; Google Drive's is ~100 rps).
- **End-to-end propagation**: a denial throws `RateLimitedException` → caught by `DefaultFederatedQueryService` → mapped to `ResponseContext.Outcome.Failure(RATE_LIMIT_EXHAUSTED, retryAfterMs, scope)` → `ErrorResponder` emits HTTP 429 with `Retry-After` header + uniform envelope JSON.
- **Showcase tests** prove both layers fire:
  - `ShowcaseTest #8` — connector-layer trip on a burst of cache-miss provider calls. Reports `scope=CONNECTOR`.
  - `ShowcaseTest #12` — service-layer trip on a burst of cache-*hit* requests. Reports `scope=USER`. Proves Ema's own SLO is honored even when responses never reach the providers.
- **Dedicated unit tests** in `src/test/java/org/emathp/ratelimit/` (token bucket + hierarchical) cover the limiter mechanics including scope-skipping when configured null.

## Metrics (`/metrics`) and k6 load test

The demo server exposes Prometheus text exposition format at **`GET /metrics`** — no auth, hand-rolled (no `simpleclient` dep). Eight metrics covering the four highest-weight rubric buckets:

| Metric | Type | What it proves |
|---|---|---|
| `emathp_provider_calls_total{connector,outcome}` | counter | Provider call rate per source |
| `emathp_provider_call_duration_seconds{connector}` | histogram | Connector latency distribution |
| `emathp_rate_limit_denied_total{scope}` | counter | Bucket exhaustion by scope (CONNECTOR / TENANT / USER) |
| `emathp_snapshot_cache_hits_total{connector}` + `..._misses_total` | counter | Snapshot cache effectiveness — hit ratio |
| `emathp_snapshot_stale_restarts_total` | counter | `maxStaleness` actually tripping rebuilds |
| `emathp_response_freshness_ms` | histogram | Distribution of data age served to clients |
| `emathp_planner_pushdown_total{connector,op}` + `..._residual_total` | counter | Pushdown effectiveness per connector + op |
| `emathp_query_errors_total{code}` | counter | Frequency per `ErrorCode` (`RATE_LIMIT_EXHAUSTED`, `BAD_QUERY`, etc.) |
| `emathp_rows_filtered_by_tag_policy_total{role}` | counter | RLS dropping rows in practice |

All counters/histograms are static singletons on `org.emathp.metrics.Metrics`; call sites are one line (e.g. `Metrics.PROVIDER_CALLS.inc(connector.source(), "ok")`).

**Driving a screenshot-worthy demo:**

```bash
# 1. start the server
./gradlew run --args web

# 2. in another shell, burst it
k6 run scripts/k6-burst.js

# 3. while k6 runs, scrape metrics
curl -s http://localhost:8080/metrics | grep -E "emathp_(rate_limit|snapshot_cache|response_freshness)"
```

The k6 script ([`scripts/k6-burst.js`](scripts/k6-burst.js)) targets 50 RPS as a single demo user for 60s — far above the 5 rps user bucket — so you'll see `emathp_rate_limit_denied_total{scope="USER"}` climb, mixed `200`/`429` responses with `Retry-After` headers, and the snapshot cache hit ratio shift toward hits after the first warm-up.

## Full vs incremental snapshot materialization

- Policy lives in **`snapshot/policy/SnapshotMaterializationPolicy.java`**:
  - **Incremental** (single-source leg with **full pushdown**, no residuals): connector chunks are **not** persisted as a growing snapshot of “whole side” in the same way as residual paths — the provider can be consulted incrementally per request policy.
  - **Fully materialized**: when the leg has **residual work** (engine must filter/sort after fetch) **or** the query is a **join** — the engine must produce a **complete** combined result before the answer is stable, so persistence follows the **materialized** layout (`_materialized` segment for joins).

## Tests (high level)

| Area | Location |
|------|----------|
| Planner pushdown rules | `src/test/java/org/emathp/planner/PlannerTest.java` |
| Mock Google connector / API | `connector/google/mock/GoogleDriveConnectorTest.java`, `ProviderPaginationTest.java` |
| Real Google connector (contract) | `connector/google/real/RealGoogleDriveConnectorTest.java` |
| Join engine | `engine/JoinExecutorTest.java` |
| SQL / joins parsing | `parser/SQLParserServiceJoinTest.java` |
| Federated demos (integration-style) | `demo/FederatedDemosTest.java` |
| Snapshot freshness / chunks / pagination | `src/test/java/org/emathp/snapshot/*.java` |
| Snapshot materialization policy | `snapshot/policy/SnapshotMaterializationPolicyTest.java` |
| Web runner / UI paging / mock user isolation | `web/WebQueryRunnerUiPagingTest.java`, `web/MockUserDataIsolationTest.java` |
| OAuth / crypto | `oauth/*.java`, `connector/google/real/GoogleTokenStoreTest.java` |
| Rate limiter (token bucket + hierarchical) | `src/test/java/org/emathp/ratelimit/*.java` |

## Demo web (`DemoWebServer`)

- **`gradlew run --args web`** starts **`org.emathp.web.DemoWebServer`**: embedded HTTP server (loopback), SQL playground posting JSON to **`/api/query`**.
- **Connector mode** (UI / JSON **`connectorMode`**): **`live`** (real Google + mock Notion, OAuth), **`mock`** (mock Google + mock Notion), **`demo`** (demo Google + demo Notion with compiled delays/page sizes).
- **Mock user** dropdown maps to **`UserContext`** for mock/demo via **`mockUserId`**; JSON responses include resolved **`tenantId`** and **`roleSlug`** for demos.
- **`env.example`** documents optional vs required variables for **live** Google.

## Git hook: PR description on push

The repo ships **`.githooks/pre-push`** and **`scripts/generate-pr-description.sh`**, but **Git does not run them until you point `core.hooksPath` at this directory** (once per clone). Without that step, nothing runs on push — the files are just versioned like any other script.

**One-time enable (required):** from the repo root run:

```bash
sh scripts/setup-git-hooks.sh
```

Windows (PowerShell): `.\scripts\setup-git-hooks.ps1`

**Check it worked:** `git config --get core.hooksPath` should print `.githooks`.

Then each **`git push`** runs **`pre-push`**, which calls **`scripts/generate-pr-description.sh`** (Git bundles `sh` when it runs hooks — you usually do **not** need `sh` on PATH for **`git push`**). To generate the same file **manually**:

**PowerShell (no `sh` required):** from repo root:

```powershell
.\scripts\generate-pr-description.ps1
```

**Git Bash / macOS / Linux:**

```bash
sh scripts/generate-pr-description.sh
```

**PowerShell but you want the shell script:** call Git’s `sh` explicitly (adjust path if Git lives elsewhere):

```powershell
& "C:\Program Files\Git\bin\sh.exe" .\scripts\generate-pr-description.sh
```

PowerShell requires **`&`** before the path to **invoke** the program; otherwise the quoted string is only an expression, not a command.

Override output: `$env:PR_DESCRIPTION_OUT="my-pr.md"; .\scripts\generate-pr-description.ps1` or `PR_DESCRIPTION_OUT=my-pr.md sh scripts/generate-pr-description.sh`

**Commit checklist:** include **`.githooks/pre-push`**, **`scripts/generate-pr-description.sh`**, **`scripts/generate-pr-description.ps1`**, and **`scripts/setup-git-hooks.sh`** / **`.ps1`** so others can enable hooks and generate PR text on Windows without Bash.

## Demo web presets and trace log

- **Presets** on the playground home page load canned SQL from **`DemoQueryPresets`** (single-source ORDER BY and a join on title), switch the UI to **Demo** connector mode, and leave **pagination to the UI page size** (not the SQL `LIMIT`, which only caps materialization).
- **`logs/web-query-trace.log`** (gitignored) appends a human-readable line per successful **`/api/query`**: connector mode, SQL preview, **`queryHash`**, snapshot path, freshness, per-side **`pushedSummary`**, **`pending`**, **`residual`**, **`snapshotReuseNoProviderCall`**, fetch counts, and per-fetch call rows (mirrors the JSON).
