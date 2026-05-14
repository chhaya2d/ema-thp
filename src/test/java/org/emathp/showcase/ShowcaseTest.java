package org.emathp.showcase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.authz.demo.DemoPrincipalRegistry;
import org.emathp.cache.QueryCacheScope;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.connector.CountingConnector;
import org.emathp.connector.google.demo.DemoGoogleDriveConnector;
import org.emathp.connector.notion.demo.DemoNotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;
import org.emathp.model.Query;
import org.emathp.query.ErrorCode;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;
import org.emathp.ratelimit.HierarchicalRateLimiter;
import org.emathp.ratelimit.HierarchicalRateLimiterConfig;
import org.emathp.ratelimit.RateLimitPolicy;
import org.emathp.ratelimit.TokenBucketConfig;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.adapters.time.SystemClock;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.web.DefaultFederatedQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * <h2>Showcase — 10 narrated integration tests proving the system end-to-end.</h2>
 *
 * <p>Each test runs the full federated query path: parser → planner → engine → snapshot → service
 * response. Uses real {@code DefaultFederatedQueryService} composed with real engine, real planner,
 * real filesystem-backed snapshots ({@link FsSnapshotStore} in a {@code @TempDir}), real demo
 * connectors, and the real {@link DemoPrincipalRegistry}. Stops short of HTTP — assertions are
 * against {@link ResponseContext} directly (the same shape the HTTP layer would serialize).
 *
 * <p>Run: {@code gradlew test --tests "org.emathp.showcase.ShowcaseTest" -i}
 *
 * <p>OAuth + live Google Drive is covered by a design-doc screenshot, not a test (needs creds +
 * network; not CI-friendly).
 */
@DisplayName("Federated query layer — end-to-end showcase")
class ShowcaseTest {

    private static final String GOOGLE_DEMO_SOURCE = "google-drive";
    private static final String NOTION_DEMO_SOURCE = "notion";

    private SQLParserService parser;
    private DemoPrincipalRegistry principals;

    @BeforeEach
    void setUp() {
        parser = new SQLParserService();
        principals = new DemoPrincipalRegistry();
    }

    // ---------------------------------------------------------------------------------------
    // 1. Pushdown by capability — Google vs Notion
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Pushdown by capability: Google pushes WHERE+ORDER BY+pagination; Notion residual on ORDER BY / PROJECTION")
    void pushdown_capability_difference_googleDrive_vs_notion() {
        Planner planner = new Planner(true);
        Query query =
                (Query)
                        parser.parse(
                                "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01'"
                                        + " ORDER BY updatedAt DESC LIMIT 20");
        Connector google = new DemoGoogleDriveConnector();
        Connector notion = new DemoNotionConnector();

        PushdownPlan googlePlan = planner.plan(google, query);
        PushdownPlan notionPlan = planner.plan(notion, query);

        // Google supports filtering + sorting + projection — every operation pushes; no residual.
        assertTrue(googlePlan.pendingOperations().isEmpty(),
                "Google pending should be empty (full pushdown); got " + googlePlan.pendingOperations());
        assertTrue(googlePlan.residualOps().isEmpty(),
                "Google residual ops should be empty");

        // Notion supports filtering but NOT sorting / projection → those cascade to residual.
        // Pagination also cascades because ORDER BY went residual.
        List<String> notionPending = notionPlan.pendingOperations();
        assertTrue(notionPending.contains("ORDER BY"),
                "Notion must residual on ORDER BY; got " + notionPending);
        assertTrue(notionPending.contains("PROJECTION"),
                "Notion must residual on PROJECTION; got " + notionPending);
        assertFalse(notionPlan.residualOps().orderBy().isEmpty(),
                "Notion residual ORDER BY must carry the ORDER BY clause for the engine to apply");
    }

    // ---------------------------------------------------------------------------------------
    // 2. Incremental complexity
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Incremental complexity: bare SELECT → +WHERE → +ORDER BY → +LIMIT → JOIN all compose cleanly")
    void incremental_complexity_select_where_sort_limit_join(@TempDir Path snapshotBase) {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);
        RequestContext ctx = aliceCtx();

        // Step 1: bare SELECT — no WHERE / ORDER / LIMIT.
        JsonObject step1 = service.executeOrThrow(ctx, sql("SELECT title FROM google"));
        assertTrue(googleRowTotal(step1) > 0, "step1 must return rows");

        // Step 2: + WHERE (predicate pushdown territory).
        JsonObject step2 = service.executeOrThrow(
                ctx, sql("SELECT title FROM google WHERE updatedAt > '2020-01-01'"));
        assertTrue(googleRowTotal(step2) > 0, "step2 must return rows");

        // Step 3: + ORDER BY (sortable on Google).
        JsonObject step3 = service.executeOrThrow(
                ctx,
                sql("SELECT title FROM google WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC"));
        assertTrue(googleRowTotal(step3) > 0, "step3 must return rows");

        // Step 4: + LIMIT — capped to 3.
        JsonObject step4 = service.executeOrThrow(
                ctx,
                sql(
                        "SELECT title FROM google WHERE updatedAt > '2020-01-01'"
                                + " ORDER BY updatedAt DESC LIMIT 3"));
        assertEquals(3, googleRowTotal(step4), "step4 LIMIT 3 must materialize exactly 3 rows");

        // Step 5: JOIN with Notion.
        JsonObject step5 = service.executeOrThrow(
                ctx,
                sql("SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 10"));
        assertEquals("join", step5.get("kind").getAsString(), "step5 must be a join response");
        JsonArray pages = step5.getAsJsonArray("pages");
        assertTrue(pages.size() >= 1, "join must produce at least one page");
        assertTrue(pages.get(0).getAsJsonObject().getAsJsonArray("rows").size() > 0,
                "join must produce at least one matched row");
    }

    // ---------------------------------------------------------------------------------------
    // 3. RLS — same SQL, different roles, different rows
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. RLS: hr role sees 8 rows; engineering role sees 7 (1 hr-only doc filtered)")
    void rls_hr_sees_more_than_engineering(@TempDir Path snapshotBase) {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);

        // Same SQL run as alice (hr, allowedTags={hr,engineering}) and bob (engineering, allowedTags={engineering}).
        String sql = "SELECT title FROM google";

        JsonObject alice = service.executeOrThrow(ctxFor("alice"), sql(sql));
        JsonObject bob = service.executeOrThrow(ctxFor("bob"), sql(sql));

        int aliceRows = googleRowTotal(alice);
        int bobRows = googleRowTotal(bob);

        assertEquals(8, aliceRows, "hr (alice) sees every demo doc (any of {hr,engineering})");
        assertEquals(7, bobRows, "engineering (bob) loses 1 hr-only doc → 7 rows");
    }

    // ---------------------------------------------------------------------------------------
    // 4. Snapshot reuse — same role, second run cached
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Snapshot reuse: second identical request by same principal serves from disk, freshness_ms > 0")
    void snapshot_reuse_same_role_second_run_is_cached(@TempDir Path snapshotBase) throws Exception {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);
        RequestContext ctx = aliceCtx();
        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                        + " ORDER BY updatedAt DESC LIMIT 20";

        JsonObject first = service.executeOrThrow(ctx, sql(sql));
        assertFalse(serveModeCached(first, NOTION_DEMO_SOURCE),
                "first run should be live (no prior snapshot)");
        String path1 = first.get("snapshotPath").getAsString();

        // Tiny wait so freshness_ms is observably > 0 on the second call.
        Thread.sleep(50);

        JsonObject second = service.executeOrThrow(ctx, sql(sql));
        String path2 = second.get("snapshotPath").getAsString();

        assertTrue(serveModeCached(second, NOTION_DEMO_SOURCE),
                "second run must be cached for the Notion side");
        assertEquals(path1, path2, "both runs share the same snapshot directory");
        assertTrue(second.has("freshness_ms") && !second.get("freshness_ms").isJsonNull(),
                "freshness_ms must be present on second run");
        assertTrue(second.get("freshness_ms").getAsLong() >= 50L,
                "freshness_ms must reflect age since first materialization");
    }

    // ---------------------------------------------------------------------------------------
    // 5. Isolation matrix — role and tenant axes (3 principals, 3 distinct paths)
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Isolation matrix: alice (t1/hr), bob (t1/eng), carol (t2/hr) get three distinct snapshot paths; no cross-cache reads")
    void snapshot_isolation_matrix_role_and_tenant_axes(@TempDir Path snapshotBase) {
        CountingConnector notion = new CountingConnector(new DemoNotionConnector());
        FederatedQueryService service =
                newServiceWithConnectors(snapshotBase, RateLimitPolicy.UNLIMITED,
                        Map.of("google", new DemoGoogleDriveConnector(), "notion", notion));

        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                        + " ORDER BY updatedAt DESC LIMIT 20";

        JsonObject aliceRun = service.executeOrThrow(ctxFor("alice"), sql(sql));
        int callsAfterAlice = notion.searchCount();
        assertTrue(callsAfterAlice > 0, "alice's run must hit the provider at least once");

        JsonObject bobRun = service.executeOrThrow(ctxFor("bob"), sql(sql));
        int callsAfterBob = notion.searchCount();
        assertTrue(callsAfterBob > callsAfterAlice,
                "bob's run must be a cache MISS (different role → different snapshot dir, no chunk reuse)");

        JsonObject carolRun = service.executeOrThrow(ctxFor("carol"), sql(sql));
        int callsAfterCarol = notion.searchCount();
        assertTrue(callsAfterCarol > callsAfterBob,
                "carol's run must be a cache MISS (different tenant → different snapshot dir)");

        String alicePath = aliceRun.get("snapshotPath").getAsString();
        String bobPath = bobRun.get("snapshotPath").getAsString();
        String carolPath = carolRun.get("snapshotPath").getAsString();

        // All three paths are distinct.
        assertNotEquals(alicePath, bobPath, "alice vs bob: role differs → path differs");
        assertNotEquals(alicePath, carolPath, "alice vs carol: tenant differs → path differs");
        assertNotEquals(bobPath, carolPath, "bob vs carol: tenant + role both differ → path differs");

        // Path segments reflect the scope's (tenant, role, user) tuple.
        assertTrue(alicePath.contains("t_tenant-1"), "alice path tagged with tenant-1");
        assertTrue(alicePath.contains("r_hr"), "alice path tagged with role hr");
        assertTrue(bobPath.contains("t_tenant-1"), "bob path tagged with tenant-1");
        assertTrue(bobPath.contains("r_engineering"), "bob path tagged with role engineering");
        assertTrue(carolPath.contains("t_tenant-2"), "carol path tagged with tenant-2");
        assertTrue(carolPath.contains("r_hr"), "carol path tagged with role hr");
    }

    // ---------------------------------------------------------------------------------------
    // 6. Snapshot reuse across UI pages
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Snapshot reuse across UI pages: page 2 slices the materialized chunk with zero provider calls")
    void snapshot_reuse_subsequent_ui_pages_use_disk_only(@TempDir Path snapshotBase) {
        CountingConnector notion = new CountingConnector(new DemoNotionConnector());
        FederatedQueryService service =
                newServiceWithConnectors(snapshotBase, RateLimitPolicy.UNLIMITED,
                        Map.of("google", new DemoGoogleDriveConnector(), "notion", notion));
        RequestContext ctx = aliceCtx();
        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                        + " ORDER BY updatedAt DESC LIMIT 20";

        // Page 1 materializes the snapshot.
        service.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        int callsAfterPage1 = notion.searchCount();

        // Page 2 should slice the snapshot — zero new provider calls.
        JsonObject page2 = service.executeOrThrow(
                ctx, new FederatedQueryRequest(sql, null, "2", null, null));
        assertEquals(callsAfterPage1, notion.searchCount(),
                "page 2 must not hit the provider — snapshot reuse");
        assertTrue(serveModeCached(page2, NOTION_DEMO_SOURCE),
                "page 2 Notion side must report serveMode=cached");
    }

    // ---------------------------------------------------------------------------------------
    // 7. Join snapshot reuse
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Join snapshot reuse: second run of the same JOIN reads the materialized chunk, fullMaterializationReuse=true")
    void join_snapshot_reuse_second_run_uses_materialized_chunk(@TempDir Path snapshotBase) {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);
        RequestContext ctx = aliceCtx();
        String sql =
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 10";

        JsonObject first = service.executeOrThrow(ctx, sql(sql));
        assertEquals("join", first.get("kind").getAsString());
        assertFalse(first.get("fullMaterializationReuse").getAsBoolean(),
                "first run must materialize fresh — fullMaterializationReuse=false");

        JsonObject second = service.executeOrThrow(ctx, sql(sql));
        assertTrue(second.get("fullMaterializationReuse").getAsBoolean(),
                "second run must reuse the materialized chunk — fullMaterializationReuse=true");
        assertEquals(first.get("snapshotPath").getAsString(),
                second.get("snapshotPath").getAsString(),
                "both runs share the same join snapshot path");
        assertTrue(second.get("freshness_ms").getAsLong() >= 0L,
                "freshness_ms must reflect age of the materialized join");
    }

    // ---------------------------------------------------------------------------------------
    // 8. Rate limit — 429 envelope with Retry-After
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Rate limit: bursting past the per-user bucket returns Outcome.Failure(RATE_LIMIT_EXHAUSTED, retryAfterMs>0, scope=USER)")
    void rate_limit_429_with_retry_after_envelope(@TempDir Path snapshotBase) {
        // Tight buckets that won't refill during the test. Refill rate 0.01/s = 1 token per 100s,
        // so no meaningful recovery in the test's ~few-second lifetime. Burst 1 = first call OK,
        // every subsequent call denied at the USER bucket.
        //
        // Connector + tenant left high so the USER bucket (the tightest) is what trips. Order of
        // check in HierarchicalRateLimiter is connector → tenant → user, so we report the first
        // bucket that denies. With connector + tenant high, USER trips first.
        HierarchicalRateLimiterConfig tight =
                new HierarchicalRateLimiterConfig(
                        new TokenBucketConfig(100.0, 200.0), // connector
                        new TokenBucketConfig(100.0, 200.0), // tenant
                        new TokenBucketConfig(0.01, 1.0));   // user — tight, no refill during test
        FederatedQueryService service = newService(snapshotBase, new HierarchicalRateLimiter(tight));

        RequestContext ctx = aliceCtx();
        // Full pushdown on Google → exactly one provider call per query → one rate-limit tick.
        // Pure pushdown also bypasses the snapshot cache (chunks aren't persisted when residuals
        // are empty), so subsequent identical queries DO hit the page loop, not a cached chunk.
        FederatedQueryRequest req =
                new FederatedQueryRequest(
                        "SELECT title, updatedAt FROM google WHERE updatedAt > '2020-01-01'"
                                + " ORDER BY updatedAt DESC LIMIT 1",
                        null, null, null, Duration.ZERO);

        int denied = 0;
        ResponseContext.Outcome.Failure firstFailure = null;
        for (int i = 0; i < 5; i++) {
            ResponseContext rc = service.execute(ctx, req);
            if (rc.outcome() instanceof ResponseContext.Outcome.Failure f
                    && f.code() == ErrorCode.RATE_LIMIT_EXHAUSTED) {
                denied++;
                if (firstFailure == null) firstFailure = f;
                assertEquals("EXHAUSTED", rc.rateLimitStatus(),
                        "rate-limit failure must set rate_limit_status=EXHAUSTED");
            }
        }

        assertTrue(denied > 0, "at least one of 5 rapid requests must be denied; got " + denied);
        assertNotNull(firstFailure, "must have captured a RATE_LIMIT_EXHAUSTED failure");
        assertEquals(ErrorCode.RATE_LIMIT_EXHAUSTED, firstFailure.code());
        assertTrue(firstFailure.retryAfterMs() != null && firstFailure.retryAfterMs() > 0L,
                "retryAfterMs must be present and positive for client backoff");
        assertEquals("USER", firstFailure.violatedScope(),
                "USER bucket (the tightest of the three) trips first");
    }

    // ---------------------------------------------------------------------------------------
    // 9. Per-connector TTL contract
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Freshness contract: Notion uses 2-min default; client maxStaleness=30s overrides it")
    void freshness_contract_per_connector_ttl_with_client_override(@TempDir Path snapshotBase) {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);
        RequestContext ctx = aliceCtx();
        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                        + " ORDER BY updatedAt DESC LIMIT 5";

        // Run 1: no maxStaleness → connector default (Notion = 2 min).
        Instant runNotionAt = Instant.now();
        JsonObject defaultRun = service.executeOrThrow(
                ctx, new FederatedQueryRequest(sql, null, null, null, null));
        Instant notionFreshnessUntil =
                Instant.parse(notionAuthoritativeMeta(defaultRun).get("freshnessUntil").getAsString());
        long ttlSecondsDefault =
                Duration.between(runNotionAt, notionFreshnessUntil).getSeconds();
        // Notion default is 2 min = 120s. Test tolerance: 100-140s window (accounts for test runtime).
        assertTrue(ttlSecondsDefault >= 100 && ttlSecondsDefault <= 140,
                "Notion default TTL ~120s; observed " + ttlSecondsDefault + "s");

        // Fresh @TempDir each test → no state leak; bump the query to differ from run 1 so we
        // don't hit the prior snapshot.
        Instant runOverrideAt = Instant.now();
        JsonObject overrideRun = service.executeOrThrow(
                ctx,
                new FederatedQueryRequest(
                        "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-02' ORDER BY updatedAt DESC LIMIT 5",
                        null, null, null, Duration.ofSeconds(30)));
        Instant overrideFreshnessUntil =
                Instant.parse(notionAuthoritativeMeta(overrideRun).get("freshnessUntil").getAsString());
        long ttlSecondsOverride =
                Duration.between(runOverrideAt, overrideFreshnessUntil).getSeconds();
        assertTrue(ttlSecondsOverride >= 25 && ttlSecondsOverride <= 35,
                "client maxStaleness=30s wins over connector default; observed "
                        + ttlSecondsOverride + "s");
    }

    // ---------------------------------------------------------------------------------------
    // 10. Error envelope shape — BAD_QUERY for malformed SQL
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Error envelope: malformed SQL returns Outcome.Failure(BAD_QUERY, message, null retryAfter), traceId preserved")
    void error_envelope_shape_bad_query(@TempDir Path snapshotBase) {
        FederatedQueryService service = newService(snapshotBase, RateLimitPolicy.UNLIMITED);
        RequestContext ctx = aliceCtx();
        FederatedQueryRequest req =
                new FederatedQueryRequest("DROP TABLE foo", null, null, null, null);

        ResponseContext rc = service.execute(ctx, req);

        assertFalse(rc.isSuccess(), "DROP TABLE must fail (only SELECT supported)");
        assertEquals(ctx.traceId(), rc.traceId(), "traceId echoes through the failure path");
        assertEquals("OK", rc.rateLimitStatus(), "non-rate-limit failure has rate_limit_status=OK");

        ResponseContext.Outcome.Failure f = (ResponseContext.Outcome.Failure) rc.outcome();
        assertEquals(ErrorCode.BAD_QUERY, f.code());
        assertNotNull(f.message(), "failure must carry a human-readable message");
        assertEquals(null, f.retryAfterMs(), "BAD_QUERY has no retry hint");
        assertEquals(null, f.violatedScope(), "BAD_QUERY has no rate-limit scope");
    }

    // ---------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------

    private FederatedQueryService newService(Path snapshotBase, RateLimitPolicy rateLimit) {
        Map<String, Connector> connectors = new LinkedHashMap<>();
        connectors.put("google", new DemoGoogleDriveConnector());
        connectors.put("notion", new DemoNotionConnector());
        return newServiceWithConnectors(snapshotBase, rateLimit, connectors);
    }

    private FederatedQueryService newServiceWithConnectors(
            Path snapshotBase, RateLimitPolicy rateLimit, Map<String, Connector> connectors) {
        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor(rateLimit);
        JoinExecutor joinExecutor = new JoinExecutor(planner, executor);
        SnapshotQueryService snapshots =
                new SnapshotQueryService(
                        planner,
                        executor,
                        new FsSnapshotStore(snapshotBase),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        return new DefaultFederatedQueryService(
                parser,
                planner,
                executor,
                joinExecutor,
                List.copyOf(connectors.values()),
                connectors,
                UserContext.anonymous(),
                snapshots,
                SnapshotEnvironment.TEST,
                2,
                principals);
    }

    private RequestContext ctxFor(String userId) {
        QueryCacheScope scope = principals.cacheScopeFor(userId);
        return RequestContext.forCli(new UserContext(userId), scope);
    }

    private RequestContext aliceCtx() {
        return ctxFor("alice");
    }

    private static FederatedQueryRequest sql(String text) {
        return new FederatedQueryRequest(text, null, null, null, null);
    }

    /**
     * Returns the materialized total (pre-UI-window) for the named connector — that's
     * {@code execution.uiRowTotal}. Falls back to row-array size when uiRowTotal isn't set
     * (e.g. non-paged responses).
     */
    private static int googleRowTotal(JsonObject response) {
        return sideRowTotal(response, GOOGLE_DEMO_SOURCE);
    }

    private static int sideRowTotal(JsonObject response, String connectorSource) {
        JsonArray sides = response.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject side = el.getAsJsonObject();
            if (connectorSource.equals(side.get("connector").getAsString())) {
                JsonObject exec = side.getAsJsonObject("execution");
                if (exec.has("uiRowTotal") && !exec.get("uiRowTotal").isJsonNull()) {
                    return exec.get("uiRowTotal").getAsInt();
                }
                return exec.getAsJsonArray("rows").size();
            }
        }
        throw new AssertionError("no side for connector " + connectorSource);
    }

    private static boolean serveModeCached(JsonObject response, String connectorSource) {
        JsonArray sides = response.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject side = el.getAsJsonObject();
            if (connectorSource.equals(side.get("connector").getAsString())) {
                return "cached".equals(side.get("serveMode").getAsString());
            }
        }
        throw new AssertionError("no side for connector " + connectorSource);
    }

    private static JsonObject notionAuthoritativeMeta(JsonObject response) {
        JsonArray sides = response.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject side = el.getAsJsonObject();
            if (NOTION_DEMO_SOURCE.equals(side.get("connector").getAsString())) {
                JsonElement meta = side.get("authoritativeChunkMeta");
                assertNotNull(meta, "Notion side must carry authoritativeChunkMeta");
                assertFalse(meta.isJsonNull(), "Notion authoritativeChunkMeta must be non-null");
                return meta.getAsJsonObject();
            }
        }
        throw new AssertionError("no Notion side in response");
    }
}
