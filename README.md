# Ema THP — federated SQL across enterprise apps

Parse a SELECT, plan what each source connector can do natively, execute with caching, rate limits, RBAC, and observability. Single-source queries and cross-connector joins both work. Drive + Notion are wired today; the connector seam is the extension point.

This README is the **how-to-run** doc. Architecture, trade-offs, and the production path live in **[docs/DESIGN.md](docs/DESIGN.md)**.

---

## Quick start

Java 21 + Gradle (wrapper included).

```bash
./gradlew test                        # full test suite (~2 min)
./gradlew run --args=web              # demo server → http://localhost:8080
```

Windows (PowerShell): `.\gradlew.bat test` / `.\gradlew.bat run --args=web`.

---

## Three ways to verify it works

### 1. `ShowcaseTest` — narrated integration tests

12 end-to-end tests with reviewer-readable display names. Each one wires the full path (parser → planner → engine → snapshot → tags → rate limit → `ResponseContext`) against real demo connectors, real filesystem snapshots, and the real principal registry — stops short of HTTP because the **HTTP layer is a thin serializer** over `ResponseContext`. See [`ShowcaseTest`](src/test/java/org/emathp/showcase/ShowcaseTest.java) class Javadoc for the request/response header mapping.

```bash
./gradlew test --tests "org.emathp.showcase.ShowcaseTest" -i
```

What's covered: pushdown decisions, snapshot reuse, tenant/role isolation, cache key derivation, rate limiting (connector-layer and service-layer), error envelope shape, freshness TTL, join materialization reuse. Roughly 30 of the 65 assertions hit `ResponseContext` fields — those are equivalent to asserting on response headers.

HTTP header serialization itself is covered separately by [`HttpEnvelopeTest`](src/test/java/org/emathp/web/HttpEnvelopeTest.java) (33 unit tests). Together: every wire header has a typed test source.

### 2. Real UI — the demo web server

The fastest reviewer demo. Boots a real HTTP server with a query playground, no setup needed for mock/demo modes.

```bash
./gradlew run --args=web
# open http://localhost:8080
```

What to try in the browser:

| Step | Action | What to look for |
|---|---|---|
| 1 | Pick **Connector mode = demo** + **Mock user = alice** | tenant `tenant-1`, role `hr` |
| 2 | Use the preset **"Notion ORDER BY updatedAt"** → Run | first run = MISS, fetches from demo Notion fixture |
| 3 | Run the same query again | second run = HIT, `serveMode: cached`, `freshness_ms > 0` |
| 4 | Switch user to **bob** (engineering), re-run | different snapshot path; tag filter drops `hr`-only rows |
| 5 | Use the preset **"Join: Google × Notion on title"** → Run twice | second run shows `fullMaterializationReuse: true` |
| 6 | In a terminal: `curl -s http://localhost:8080/metrics \| grep emathp_` | live cumulative counters per connector / status / scope |

The response JSON has the full debug surface (`snapshotPath`, `queryHash`, `freshness_ms`, per-side `serveMode`, `pages`). Adding `Debug: true` request header surfaces the same fields as HTTP response headers (`X-Snapshot-Path`, `X-Query-Hash`, `X-Tenant-Id`, `X-Role`).

**Live Google Drive** is optional (mock and demo modes don't need it) — see [Optional: live Google Drive](#optional-live-google-drive-oauth) below.

### 3. k6 — load test under burst

Verifies concurrency, the two-layer rate limit, single-flight cache filling, and the `cache_misses == provider_calls` invariant.

```bash
# server must be running in another shell
k6 run scripts/k6-burst.js
```

100 RPS for 2 minutes, all as user `alice` on the Notion connector. Captured output and metric snapshots from a real run live under **[docs/sample-output/](docs/sample-output/)**:

| File | Purpose |
|---|---|
| `k6-run.log` | Full k6 stdout — checks, thresholds, p50/p95/p99 latencies |
| `metrics-baseline.txt` | `/metrics` scrape before the run (all counters at 0) |
| `metrics-after-k6.txt` | `/metrics` scrape after — see `_total` deltas |
| `trace-log-sample.log` | Tail of `logs/web-query-trace.log` from the run — one structured line per successful query, with traceId, snapshotPath, per-side `serveMode` |

**Headline numbers from the captured run** (11,524 requests over 2 min):

```
emathp_provider_calls_total{connector="notion",outcome="ok"}   1     ← single-flight + cache TTL absorb the burst
emathp_snapshot_cache_misses_total{connector="notion"}          1     ← exactly one true miss
emathp_snapshot_cache_hits_total{connector="notion"}            590   ← everything else from cache
emathp_query_errors_total{code="RATE_LIMIT_EXHAUSTED"}          10932 ← service-layer USER bucket fires on the burst
```

The `cache_misses == provider_calls == 1` invariant holds — single-flight is collapsing concurrent misses for the same connectorDir onto one upstream call. The 10,932 denials prove the service-layer (per-user) rate limiter fires on cache hits too, not just on provider calls.

---

## Code map — where to look

| Concern | Entry point |
|---|---|
| Bootstrap / wiring | `org.emathp.web.DemoWebServer.main` |
| HTTP boundary (parse + write) | `org.emathp.web.HttpEnvelope` |
| Service layer (`ResponseContext` producer) | `org.emathp.web.DefaultFederatedQueryService` |
| Planner (rules, pushdown order) | `org.emathp.planner.Planner` |
| Engine / executor | `org.emathp.engine.QueryExecutor` |
| Per-side snapshot pipeline | `org.emathp.snapshot.pipeline.SingleSourceSidePipeline` |
| Join materialization | `org.emathp.snapshot.pipeline.FullMaterializationCoordinator` |
| Connector capability declaration | `org.emathp.connector.Connector` |
| Rate limit (two-layer) | `org.emathp.ratelimit.HierarchicalRateLimiter` |
| Identity / principal lookup | `org.emathp.authz.demo.DemoPrincipalRegistry` |
| Metrics registry | `org.emathp.metrics.Metrics` |

Three Drive connector flavors live side-by-side: `connector/google/real/` (OAuth), `connector/google/mock/` (in-memory fixture for tests), `connector/google/demo/` (web demo fixture). Notion has mock + demo. The connector interface is identical across all three.

---

## Optional: live Google Drive (OAuth)

Mock + demo modes always work. To enable **live** mode (real Google Drive via OAuth):

1. Copy `env.example` → `.env` in the project root.
2. Fill `GOOGLE_OAUTH_CLIENT_ID` + `GOOGLE_OAUTH_CLIENT_SECRET` from a Google Cloud project (Authorized redirect URI = `http://localhost:8080/oauth/google/callback`).
3. Set `CONNECTOR_TOKEN_KEY` to any non-default value (AES key for token storage at rest).
4. Optional: point at Postgres via `POSTGRES_*` env vars, or set `EMA_DEV_H2=true` for in-memory H2 token storage.
5. Restart `./gradlew run --args=web`. The UI's **live** mode becomes available; first use triggers OAuth at `/oauth/google/start`.

If any of those env vars are missing, the server still runs but the **live** dropdown stays disabled. Mock + demo are unaffected.

---

## Pre-push hook (auto-generates PR description)

A pre-push hook runs `scripts/generate-pr-description.sh` to write `PR_DESCRIPTION.generated.md` (gitignored). One-time setup per clone:

```bash
sh scripts/setup-git-hooks.sh                # Linux/macOS/Git Bash
.\scripts\setup-git-hooks.ps1                # Windows PowerShell
```

Check: `git config --get core.hooksPath` should print `.githooks`.

---

## What's where in the docs

| Doc | Purpose |
|---|---|
| [docs/DESIGN.md](docs/DESIGN.md) | Architecture, trade-offs, prod-shape, deliberately out of scope, six-month plan |
| [docs/sample-output/](docs/sample-output/) | k6 run output + metric snapshots referenced above |

Header contract specifically — [docs/DESIGN.md#http-surface](docs/DESIGN.md#http-surface).
