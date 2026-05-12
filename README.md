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
| **Demo** | `connector/google/demo/` | **`DemoGoogleDriveConnector`** + **`DemoGoogleDriveApi`**: fixed join-oriented corpus (**alice** / **bob** slices), **`DemoConnectorDefaults`** for delay and page size. |

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

## Snapshots, cache scope, freshness

- **`QueryCacheScope`** (`cache/QueryCacheScope.java`) namespaces snapshot paths by **`userId`** (and a **key schema version**), so different principals do not share on-disk snapshot trees.
- The web API accepts **`maxStaleness`** (ISO-8601 duration, e.g. `PT10M`) to bound reuse of snapshot materializations; default chunk TTL comes from **`WebDefaults.snapshotChunkFreshness()`** when omitted.

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

## Demo web (`DemoWebServer`)

- **`gradlew run --args web`** starts **`org.emathp.web.DemoWebServer`**: embedded HTTP server (loopback), SQL playground posting JSON to **`/api/query`**.
- **Connector mode** (UI / JSON **`connectorMode`**): **`live`** (real Google + mock Notion, OAuth), **`mock`** (mock Google + mock Notion), **`demo`** (demo Google + demo Notion with compiled delays/page sizes).
- **Mock user** dropdown maps to **`UserContext`** for mock/demo via **`mockUserId`**.
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
