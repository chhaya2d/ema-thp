package org.emathp.engine;

import java.util.ArrayList;
import java.util.List;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.model.ConnectorQuery;
import org.emathp.engine.policy.TagAccessPolicy;
import org.emathp.engine.policy.TagRowFilter;
import org.emathp.model.ResidualOps;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;

/**
 * Engine-side execution for a single connector. Responsible for:
 *
 * <ul>
 *   <li><b>Pagination loop:</b> repeatedly invokes
 *       {@link Connector#search(org.emathp.auth.UserContext, ConnectorQuery)},
 *       threading the previous response's normalized {@code nextCursor} into the next call's
 *       {@code ConnectorQuery.cursor()}, until the connector reports no more pages.</li>
 *   <li><b>Residual operations:</b> applies the in-memory WHERE / ORDER BY described by
 *       {@link ResidualOps} after the page loop completes, before the LIMIT cap.</li>
 *   <li><b>Logical LIMIT enforcement:</b> caps the final row count at the user's logical
 *       {@code limit} regardless of pushdown status.</li>
 * </ul>
 *
 * @apiNote {@code logicalLimit} is the user's intended cap, threaded through directly from
 *          {@code Query.limit()}. {@link ConnectorQuery} has no {@code limit} field by design
 *          (ADR-0003); the connector contract is cursor-pagination only, so LIMIT enforcement
 *          happens here exclusively.
 * @implNote When residual operations are non-empty, the page loop fetches <em>all</em> pages
 *           before applying them — short-circuiting at {@code logicalLimit} during the loop
 *           would slice an arbitrary prefix, and a downstream sort/filter on that prefix
 *           wouldn't match "top-N from the full result". When there are no residuals, the
 *           loop shortImplement filesystem-backed incremental query snapshot materialization for the existing Java 21 federated query engine.
 *
 * IMPORTANT:
 * This implementation is for:
 *
 * * logical query pagination
 * * incremental snapshot materialization
 * * connector continuation
 * * freshness-aware cache reuse
 * * stable paginated query semantics
 *
 * DO NOT implement:
 *
 * * DuckDB
 * * Redis
 * * distributed cache
 * * CDC
 * * background refresh jobs
 * * compression
 * * async workers
 * * object storage
 *
 * Keep implementation lightweight and filesystem-backed.
 *
 * # IMPORTANT ARCHITECTURAL MODEL
 *
 * The system must clearly separate THREE pagination layers:
 *
 * | Layer                    | Owner     |
 * | ------------------------ | --------- |
 * | Provider pagination      | Connector |
 * | Snapshot materialization | Engine    |
 * | UI logical pagination    | Engine    |
 *
 * IMPORTANT:
 * Provider pagination is:
 *
 * * transport optimization only
 *
 * Logical pagination is:
 *
 * * user-visible semantics
 *
 * # PAGINATION CONFIGURATION
 *
 * # Mock Connectors
 *
 * Provider page sizes:
 *
 * * Mock Google = 6
 * * Mock Notion = 4
 *
 * UI page size for tests/demo:
 *
 * ```text id="d9r2qp"
 * 2
 * ```
 *
 * # Real Google Connector
 *
 * Provider page size:
 *
 * ```text id="x7m4tl"
 * 20
 * ```
 *
 * UI page size:
 *
 * ```text id="p4n8vk"
 * 10
 * ```
 *
 * IMPORTANT:
 * Provider page size and UI page size MUST remain fully independent.
 *
 * # IMPORTANT FRESHNESS MODEL
 *
 * Freshness determines:
 *
 * * whether an existing snapshot may continue incrementally
 * * whether snapshot must be invalidated and restarted
 *
 * # Fresh Snapshot
 *
 * If:
 *
 * ```text id="u6z1qw"
 * now < freshnessUntil
 * ```
 *
 * THEN:
 *
 * * reuse snapshot
 * * continue provider pagination lazily if needed
 *
 * # Stale Snapshot
 *
 * If:
 *
 * ```text id="e3m7yr"
 * now >= freshnessUntil
 * ```
 *
 * THEN:
 *
 * * delete entire query snapshot directory
 * * rerun query from beginning
 *
 * IMPORTANT:
 * Do NOT continue stale provider cursors.
 *
 * # FILESYSTEM ARCHITECTURE
 *
 * Use filesystem-backed incremental query snapshots.
 *
 * Base directories:
 *
 * ```text id="r1x8mp"
 * data/
 *   prod/
 *   test/
 * ```
 *
 * IMPORTANT:
 *
 * * production runtime uses data/prod/
 * * tests use data/test/
 *
 * # QUERY SNAPSHOT STRUCTURE
 *
 * Each query snapshot:
 *
 * ```text id="k9t2vq"
 * data/<env>/<userId>/<queryHash>/
 * ```
 *
 * Example:
 *
 * ```text id="z4m7pn"
 * data/prod/user1/abcd123/
 * ```
 *
 * # QUERY SNAPSHOT FILES
 *
 * Each query snapshot directory contains:
 *
 * ```text id="n5r1yk"
 * query_info.json
 *
 * 000000_000005_data.json
 * 000000_000005_meta.json
 *
 * 000006_000011_data.json
 * 000006_000011_meta.json
 *
 * 000012_000017_data.json
 * 000012_000017_meta.json
 * ```
 *
 * IMPORTANT:
 * Chunk files represent:
 *
 * * incrementally materialized logical row ranges
 *
 * NOT:
 *
 * * UI page ranges
 *
 * # query_info.json
 *
 * Contains ONLY informational metadata.
 *
 * Example:
 *
 * ```json id="w2m8ql"
 * {
 *   "queryHash": "abcd123",
 *   "userId": "user1",
 *   "normalizedQuery": "...",
 *   "createdAt": "..."
 * }
 * ```
 *
 * IMPORTANT:
 * query_info.json must NOT contain:
 *
 * * continuation state
 * * freshness state
 * * execution state
 *
 * # CHUNK DATA FILES
 *
 * Example:
 *
 * ```text id="c7v4tm"
 * 000000_000005_data.json
 * ```
 *
 * Contains:
 *
 * * normalized EngineRow rows
 *
 * # CHUNK METADATA FILES
 *
 * Example:
 *
 * ```text id="f8n1zr"
 * 000000_000005_meta.json
 * ```
 *
 * Contains:
 *
 * ```json id="m3x7vp"
 * {
 *   "startRow": 0,
 *   "endRow": 5,
 *   "createdAt": "...",
 *   "freshnessUntil": "...",
 *   "nextCursor": "cursor_6",
 *   "exhausted": false,
 *   "providerFetchSize": 6
 * }
 * ```
 *
 * # IMPORTANT EXECUTION INVARIANT
 *
 * The chunk metadata file with:
 *
 * * highest endRow
 *
 * represents:
 *
 * * authoritative continuation state
 *
 * This means:
 *
 * * latest chunk metadata determines
 *
 *   * freshness
 *   * continuation cursor
 *   * exhausted state
 *
 * IMPORTANT:
 * There is NO separate global execution metadata file.
 *
 * # QUERY EXECUTION FLOW
 *
 * # Initial Query
 *
 * If query snapshot directory does NOT exist:
 *
 * * create query snapshot directory
 * * execute provider fetch
 * * persist chunk files
 * * return requested logical page
 *
 * # Existing Fresh Snapshot
 *
 * If:
 *
 * * snapshot exists
 * * latest chunk freshness valid
 *
 * THEN:
 *
 * * reuse cached chunks
 * * continue provider pagination lazily if needed
 *
 * # Existing Stale Snapshot
 *
 * If:
 *
 * * latest chunk freshness expired
 *
 * THEN:
 *
 * * delete entire snapshot directory
 * * rerun query from beginning
 *
 * # INCREMENTAL SNAPSHOT EXPANSION
 *
 * When requested logical rows exceed cached rows:
 *
 * AND:
 *
 * * latest chunk exhausted == false
 * * latest chunk nextCursor != null
 *
 * THEN:
 *
 * * continue provider pagination
 * * fetch next provider batch
 * * append new chunk files
 * * continue until:
 *
 *   * requested logical rows available
 *     OR
 *   * provider exhausted
 *
 * IMPORTANT:
 * Continuation must happen lazily.
 *
 * # CONNECTOR PAGINATION
 *
 * Mock connectors must simulate realistic provider pagination.
 *
 * # Mock Google Cursor Progression
 *
 * ```text id="v1m8qk"
 * cursor_6
 * cursor_12
 * cursor_18
 * EOF
 * ```
 *
 * # Mock Notion Cursor Progression
 *
 * ```text id="t4n2pw"
 * cursor_4
 * cursor_8
 * cursor_12
 * EOF
 * ```
 *
 * EOF means:
 *
 * * provider exhausted
 * * no further continuation possible
 *
 * # LOGICAL PAGINATION
 *
 * UI requests:
 *
 * ```text id="x9r5zl"
 * pageNumber
 * pageSize
 * ```
 *
 * Engine computes:
 *
 * ```text id="y2v7qm"
 * startRow = pageNumber * pageSize
 * endRow = startRow + pageSize - 1
 * ```
 *
 * Engine must:
 *
 * * load rows across chunk files
 * * slice logical pages correctly
 *
 * IMPORTANT:
 * UI pagination must NEVER expose provider cursors.
 *
 * # SNAPSHOT EXHAUSTION
 *
 * When provider returns EOF:
 *
 * Chunk metadata should contain:
 *
 * ```json id="z6m1xn"
 * {
 *   "nextCursor": null,
 *   "exhausted": true
 * }
 * ```
 *
 * At this point:
 *
 * * entire logical query snapshot is materialized
 * * all future pagination becomes local-only
 *
 * # REQUIRED STORAGE INTERFACES
 *
 * Create:
 *
 * ```text id="b5r9qv"
 * SnapshotStorage
 * ```
 *
 * Responsibilities:
 *
 * * create query snapshot directories
 * * persist chunk files
 * * load chunk files
 * * load chunk metadata
 * * delete stale snapshots
 *
 * # REQUIRED ENGINE BEHAVIOR
 *
 * Engine responsibilities:
 *
 * * compute logical row ranges
 * * determine whether snapshot exists
 * * determine freshness validity
 * * determine whether continuation required
 * * continue provider pagination lazily
 * * slice logical UI pages
 *
 * Connector responsibilities:
 *
 * * provider pagination
 * * provider cursors
 * * transport batching
 *
 * # REQUIRED TESTS
 *
 * # Unit Tests
 *
 * ## ProviderPaginationTest
 *
 * Verify:
 *
 * * provider pagination progression
 * * cursor advancement
 * * EOF handling
 *
 * ## LogicalPaginationTest
 *
 * Verify:
 *
 * * logical page slicing works correctly
 * * UI pages independent of provider page size
 *
 * ## ChunkMetadataTest
 *
 * Verify:
 *
 * * latest chunk detection
 * * continuation state extraction
 * * freshness extraction
 *
 * # Integration Tests
 *
 * Use filesystem-backed snapshots.
 *
 * Tests MUST use:
 *
 * ```text id="d1m8zk"
 * data/test/
 * ```
 *
 * ONLY.
 *
 * # IncrementalSnapshotExpansionTest
 *
 * Scenario:
 *
 * 1. UI page size = 2
 * 2. provider fetch size = 6
 * 3. first chunk materialized
 * 4. deeper UI page requested
 * 5. continuation fetch occurs
 * 6. second chunk materialized
 *
 * Verify:
 *
 * * chunk files created
 * * connector called lazily
 * * continuation works correctly
 *
 * # LocalPageReuseTest
 *
 * Scenario:
 *
 * 1. provider fetch occurs once
 * 2. multiple UI pages served locally
 *
 * Verify:
 *
 * * no additional provider fetch occurs
 *
 * # FreshnessInvalidationTest
 *
 * Scenario:
 *
 * 1. snapshot created
 * 2. freshness expires
 * 3. next request invalidates snapshot
 * 4. query reruns from beginning
 *
 * Verify:
 *
 * * old snapshot deleted
 * * fresh snapshot recreated
 *
 * # ExhaustedSnapshotTest
 *
 * Scenario:
 *
 * 1. provider exhausted
 * 2. later UI pages served locally only
 *
 * Verify:
 *
 * * exhausted=true
 * * no continuation fetch attempted
 *
 * # MAIN.JAVA DEMO
 *
 * Demonstrate:
 *
 * # Mock Demo
 *
 * Provider fetch size:
 *
 * ```text id="r7v4mn"
 * 6
 * ```
 *
 * UI page size:
 *
 * ```text id="q2m8pk"
 * 2
 * ```
 *
 * Flow:
 *
 * 1. request page 0
 * 2. provider fetch occurs
 * 3. request page 1
 * 4. local cache reuse
 * 5. request page 3
 * 6. continuation fetch occurs
 * 7. EOF reached
 * 8. future pages local-only
 *
 * # Real Google Demo
 *
 * Provider fetch size:
 *
 * ```text id="n4x1zr"
 * 20
 * ```
 *
 * UI page size:
 *
 * ```text id="m8v2qp"
 * 10
 * ```
 *
 * Demonstrate:
 *
 * * filesystem snapshot creation
 * * lazy continuation
 * * freshness-aware reuse
 *
 * # IMPORTANT OUTPUT
 *
 * Print:
 *
 * * queryHash
 * * snapshot path
 * * provider fetches
 * * continuation fetches
 * * chunk files created
 * * cache reuse
 * * freshness decisions
 * * nextCursor
 * * exhausted state
 *
 * # IMPORTANT IMPLEMENTATION RULES
 *
 * * Keep code compileable
 * * Use Java 21
 * * Use records where appropriate
 * * Use Jackson for JSON only
 * * Keep filesystem structure human-readable
 * * No frameworks
 * * No distributed systems
 * * No DuckDB yet
 *
 * # IMPORTANT ARCHITECTURAL GOAL
 *
 * Clearly demonstrate:
 *
 * Provider pagination
 * ≠
 * logical pagination
 *
 * Connectors own:
 *
 * * transport batching
 * * cursors
 *
 * Engine owns:
 *
 * * logical pagination
 * * snapshot materialization
 * * freshness-aware invalidation
 * * cache reuse
 * * incremental expansion-circuits as soon as the cap is reached, which avoids unnecessary calls.
 * @implNote PROJECTION is not yet handled here; see {@link ResidualOps}'s @apiNote.
 */
public final class QueryExecutor {

    private final FilterExecutor filterExecutor = new FilterExecutor();
    private final SortExecutor sortExecutor = new SortExecutor();

    public ExecutionResult execute(
            UserContext user,
            Connector connector,
            ConnectorQuery initial,
            ResidualOps residual,
            Integer logicalLimit) {
        return execute(user, connector, initial, residual, logicalLimit, null);
    }

    /**
     * Applies optional {@link TagAccessPolicy} after residual ops and before logical LIMIT — rows
     * persisted to snapshots are therefore already role-filtered when this policy is non-empty.
     */
    public ExecutionResult execute(
            UserContext user,
            Connector connector,
            ConnectorQuery initial,
            ResidualOps residual,
            Integer logicalLimit,
            TagAccessPolicy tagPolicy) {

        boolean hasResidual = !residual.isEmpty();
        // NOTE: when residuals are present, do NOT short-circuit the page loop on logicalLimit —
        // we must see every row before we can sort/filter and pick the true top-N.
        Integer pageLoopCap = hasResidual ? null : logicalLimit;

        PageLoopOutcome loop = runPageLoop(user, connector, initial, pageLoopCap);

        List<EngineRow> rows = loop.rows;
        int rowsFromConnector = rows.size();

        if (hasResidual) {
            if (residual.where() != null) {
                rows = filterExecutor.apply(rows, residual.where());
            }
            if (!residual.orderBy().isEmpty()) {
                rows = sortExecutor.apply(rows, residual.orderBy());
            }
        }

        if (tagPolicy != null && !tagPolicy.allowedTags().isEmpty()) {
            rows = TagRowFilter.apply(rows, tagPolicy);
        }

        boolean limitStoppedAtCap = false;
        if (logicalLimit != null) {
            LimitExecutor.Limited<EngineRow> lim = LimitExecutor.apply(rows, logicalLimit);
            rows = lim.rows();
            limitStoppedAtCap = lim.stoppedAtLimit();
        }

        boolean stoppedAtLimit = loop.stoppedAtLimit || limitStoppedAtCap;
        return new ExecutionResult(
                loop.calls,
                List.copyOf(rows),
                loop.lastNextCursor,
                stoppedAtLimit,
                rowsFromConnector,
                hasResidual);
    }

    private PageLoopOutcome runPageLoop(
            UserContext user, Connector connector, ConnectorQuery initial, Integer pageLoopCap) {
        List<EngineRow> accumulated = new ArrayList<>();
        List<PageCall> calls = new ArrayList<>();
        ConnectorQuery current = initial;
        String lastNextCursor = null;
        boolean stoppedAtLimit = false;
        while (true) {
            SearchResult<EngineRow> page = connector.search(user, current);
            calls.add(new PageCall(current.cursor(), page.rows().size(), page.nextCursor()));

            for (EngineRow r : page.rows()) {
                if (pageLoopCap != null && accumulated.size() >= pageLoopCap) {
                    stoppedAtLimit = true;
                    break;
                }
                accumulated.add(r);
            }
            lastNextCursor = page.nextCursor();
            if (pageLoopCap != null && accumulated.size() >= pageLoopCap) {
                stoppedAtLimit = true;
                break;
            }
            if (lastNextCursor == null) {
                break;
            }
            current = withCursor(current, lastNextCursor);
        }
        return new PageLoopOutcome(calls, accumulated, lastNextCursor, stoppedAtLimit);
    }

    private static ConnectorQuery withCursor(ConnectorQuery cq, String cursor) {
        return new ConnectorQuery(
                cq.projection(),
                cq.where(),
                cq.orderBy(),
                cursor,
                cq.pageSize());
    }

    private record PageLoopOutcome(
            List<PageCall> calls,
            List<EngineRow> rows,
            String lastNextCursor,
            boolean stoppedAtLimit) {}

    /** One round-trip to a connector, captured for the demo's execution trace. */
    public record PageCall(String cursor, int rowsReturned, String nextCursor) {}

    /**
     * Outcome of a paginated execution. {@code finalNextCursor} is the cursor at the point we
     * stopped — non-null when more pages exist (we either capped or there are more rows the
     * connector has but we didn't fetch); null when the connector ran out of pages.
     *
     * @param rowsFromConnector total rows accumulated from the page loop, before residuals or
     *                          the LIMIT cap. Useful for showing how much work the engine did.
     * @param residualApplied whether the executor applied any in-memory WHERE / ORDER BY.
     */
    public record ExecutionResult(
            List<PageCall> calls,
            List<EngineRow> rows,
            String finalNextCursor,
            boolean stoppedAtLimit,
            int rowsFromConnector,
            boolean residualApplied) {

        public ExecutionResult {
            calls = calls == null ? List.of() : List.copyOf(calls);
            rows = rows == null ? List.of() : List.copyOf(rows);
        }
    }
}
