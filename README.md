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

- **`RateLimitPolicy`** (`ratelimit/RateLimitPolicy.java`) is the engine-facing interface — `tryAcquire(RequestContext) -> RateLimitResult`. Implementations: **`HierarchicalRateLimiter`** (real, three-axis token-bucket) and **`RateLimitPolicy.UNLIMITED`** (no-op, used by every snapshot / pagination test).
- **Hierarchical AND-semantics**: each provider call must pass **connector**, **tenant**, and **user** token buckets. A denial in any bucket fails the request; earlier buckets are refunded (best-effort) so they don't leak quota.
- **Where the check fires**: inside **`QueryExecutor.runPageLoop`**, **before each `connector.search(...)`** call. A single SQL query that does N page fetches debits N times per (connector, tenant, user). A denial throws **`RateLimitedException`** and unwinds the whole `/api/query` request — no partial rows surface.
- **Anon short-circuit**: when `UserContext.userId()` is null/blank, or **`PrincipalRegistry.lookup`** returns `Principal.anonymous()`, **`UnifiedSnapshotWebRunner`** passes `tenantId = null` down through `SidePageRequest` / `FullMaterializationCoordinator`; the page loop sees a blank tenant and skips the limiter entirely. CLI demos and snapshot tests therefore run unlimited.
- **Demo config** lives in **`DemoWebServer.demoRateLimitConfig()`** — uniform shape today (connector 30 rps / burst 60, tenant 10 / 20, user 5 / 10). Per-user / per-tier overrides will come via a per-key config resolver in a follow-up.
- **Dedicated limiter tests** sit in **`src/test/java/org/emathp/ratelimit/`** (token bucket + hierarchical). Engine / snapshot / web tests inject `UNLIMITED` so bucket state never affects them.

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
