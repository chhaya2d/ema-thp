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

        // Use execute() to get the full ResponseContext (wire-contract surface) — body-detail
        // assertions still come from rc.body(); header-shaped assertions go through the typed fields.
        org.emathp.query.ResponseContext firstRc = service.execute(ctx, sql(sql));
        assertTrue(firstRc.isSuccess(), "first run must succeed");
        JsonObject first = firstRc.body();
        assertEquals("MISS", firstRc.cacheStatus(),
                "first run is a top-level cache MISS (no prior snapshot)");
        assertFalse(serveModeCached(first, NOTION_DEMO_SOURCE),
                "first run should be live (no prior snapshot)");
        String path1 = firstRc.debug().snapshotPath();
        assertEquals(path1, first.get("snapshotPath").getAsString(),
                "ResponseContext.debug.snapshotPath mirrors the body field exactly");

        // Tiny wait so freshness_ms is observably > 0 on the second call.
        Thread.sleep(50);

        org.emathp.query.ResponseContext secondRc = service.execute(ctx, sql(sql));
        assertTrue(secondRc.isSuccess(), "second run must succeed");
        JsonObject second = secondRc.body();
        String path2 = secondRc.debug().snapshotPath();

        assertEquals("HIT", secondRc.cacheStatus(),
                "second run is a top-level cache HIT (chunks reused)");
        assertTrue(serveModeCached(second, NOTION_DEMO_SOURCE),
                "second run must be cached for the Notion side");
        assertEquals(path1, path2, "both runs share the same snapshot directory");
        assertNotNull(secondRc.freshnessMs(),
                "ResponseContext.freshnessMs must be present on second run");
        assertTrue(secondRc.freshnessMs() >= 50L,
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

        org.emathp.query.ResponseContext aliceRc = service.execute(ctxFor("alice"), sql(sql));
        assertTrue(aliceRc.isSuccess());
        int callsAfterAlice = notion.searchCount();
        assertTrue(callsAfterAlice > 0, "alice's run must hit the provider at least once");
        assertEquals("MISS", aliceRc.cacheStatus(), "alice (first run, cold cache) is a top-level MISS");

        org.emathp.query.ResponseContext bobRc = service.execute(ctxFor("bob"), sql(sql));
        assertTrue(bobRc.isSuccess());
        int callsAfterBob = notion.searchCount();
        assertTrue(callsAfterBob > callsAfterAlice,
                "bob's run must be a cache MISS (different role → different snapshot dir, no chunk reuse)");
        assertEquals("MISS", bobRc.cacheStatus(), "bob is a top-level MISS (separate dir from alice)");

        org.emathp.query.ResponseContext carolRc = service.execute(ctxFor("carol"), sql(sql));
        assertTrue(carolRc.isSuccess());
        int callsAfterCarol = notion.searchCount();
        assertTrue(callsAfterCarol > callsAfterBob,
                "carol's run must be a cache MISS (different tenant → different snapshot dir)");
        assertEquals("MISS", carolRc.cacheStatus(), "carol is a top-level MISS (separate tenant from alice/bob)");

        // Wire-contract assertions use ResponseContext.debug.snapshotPath (typed). The body
        // mirror is kept for backward compat; both should match.
        String alicePath = aliceRc.debug().snapshotPath();
        String bobPath = bobRc.debug().snapshotPath();
        String carolPath = carolRc.debug().snapshotPath();
        assertEquals(alicePath, aliceRc.body().get("snapshotPath").getAsString());
        assertEquals(bobPath, bobRc.body().get("snapshotPath").getAsString());
        assertEquals(carolPath, carolRc.body().get("snapshotPath").getAsString());
        // Tenant/role debug fields confirm principal resolution per request.
        assertEquals("tenant-1", aliceRc.debug().tenantId());
        assertEquals("hr", aliceRc.debug().roleSlug());
        assertEquals("tenant-1", bobRc.debug().tenantId());
        assertEquals("engineering", bobRc.debug().roleSlug());
        assertEquals("tenant-2", carolRc.debug().tenantId());
        assertEquals("hr", carolRc.debug().roleSlug());

        // All three paths are distinct.
        assertNotEquals(alicePath, bobPath, "alice vs bob: role differs → path differs");
        assertNotEquals(alicePath, carolPath, "alice vs carol: tenant differs → path differs");
        assertNotEquals(bobPath, carolPath, "bob vs carol: tenant + role both differ → path differs");

        // Demo connectors declare TENANT_ROLE scope, so the snapshot path excludes userId — two
        // users with the same (tenant, role) would share the directory (see Test #11). The
        // segments still reflect tenant + role.
        assertTrue(alicePath.contains("t_tenant-1") && alicePath.contains("r_hr"),
                "alice path tagged with tenant-1 + hr");
        assertTrue(bobPath.contains("t_tenant-1") && bobPath.contains("r_engineering"),
                "bob path tagged with tenant-1 + engineering");
        assertTrue(carolPath.contains("t_tenant-2") && carolPath.contains("r_hr"),
                "carol path tagged with tenant-2 + hr");
        // Demo connectors → snapshotKeyedByUser is false. No userId in path.
        assertFalse(alicePath.contains("u_alice"),
                "demo connectors are TENANT_ROLE-scoped; path must not include userId");
    }

    // ---------------------------------------------------------------------------------------
    // 11. Cache sharing — same tenant + role, different users
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. Cache sharing for TENANT_ROLE connectors: alice and dan (both t1/hr) share the snapshot path; dan's run is a cache HIT")
    void snapshot_cache_sharing_across_users_with_same_tenant_and_role(@TempDir Path snapshotBase) {
        CountingConnector notion = new CountingConnector(new DemoNotionConnector());
        FederatedQueryService service =
                newServiceWithConnectors(snapshotBase, RateLimitPolicy.UNLIMITED,
                        Map.of("google", new DemoGoogleDriveConnector(), "notion", notion));
        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                        + " ORDER BY updatedAt DESC LIMIT 20";

        org.emathp.query.ResponseContext aliceRc = service.execute(ctxFor("alice"), sql(sql));
        assertTrue(aliceRc.isSuccess());
        JsonObject aliceRun = aliceRc.body();
        int callsAfterAlice = notion.searchCount();
        assertTrue(callsAfterAlice > 0, "alice (first run) must hit the provider");
        assertEquals("MISS", aliceRc.cacheStatus(), "alice first run is a top-level MISS");
        assertFalse(serveModeCached(aliceRun, NOTION_DEMO_SOURCE),
                "alice's first run is live, not cached");

        org.emathp.query.ResponseContext danRc = service.execute(ctxFor("dan"), sql(sql));
        assertTrue(danRc.isSuccess());
        JsonObject danRun = danRc.body();
        int callsAfterDan = notion.searchCount();

        // Dan has the same (tenant-1, hr) as alice. With DemoNotionConnector declaring
        // TENANT_ROLE scope, the snapshot path is invariant under userId — dan reads alice's
        // chunks.
        assertEquals(callsAfterAlice, callsAfterDan,
                "dan's run must be a cache HIT (no extra provider calls)");
        assertEquals("HIT", danRc.cacheStatus(), "dan's run is a top-level cache HIT");
        assertTrue(serveModeCached(danRun, NOTION_DEMO_SOURCE),
                "dan's Notion side must report serveMode=cached");
        assertEquals(aliceRc.debug().snapshotPath(), danRc.debug().snapshotPath(),
                "alice and dan share the snapshot path (same tenant + role; TENANT_ROLE connector)");
        // Sanity: response flag reflects the keying decision.
        assertFalse(danRun.get("snapshotKeyedByUser").getAsBoolean(),
                "with TENANT_ROLE connectors, snapshotKeyedByUser must be false");
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

        org.emathp.query.ResponseContext firstRc = service.execute(ctx, sql(sql));
        assertTrue(firstRc.isSuccess());
        JsonObject first = firstRc.body();
        assertEquals("join", first.get("kind").getAsString());
        assertEquals("MISS", firstRc.cacheStatus(),
                "first run is a top-level MISS (must materialize the join)");
        assertFalse(first.get("fullMaterializationReuse").getAsBoolean(),
                "first run must materialize fresh — fullMaterializationReuse=false");

        org.emathp.query.ResponseContext secondRc = service.execute(ctx, sql(sql));
        assertTrue(secondRc.isSuccess());
        JsonObject second = secondRc.body();
        assertEquals("HIT", secondRc.cacheStatus(),
                "second run is a top-level HIT (full materialization reused)");
        assertTrue(second.get("fullMaterializationReuse").getAsBoolean(),
                "second run must reuse the materialized chunk — fullMaterializationReuse=true");
        assertEquals(firstRc.debug().snapshotPath(), secondRc.debug().snapshotPath(),
                "both runs share the same join snapshot path");
        assertNotNull(secondRc.freshnessMs(),
                "ResponseContext.freshnessMs must be present after the join is materialized");
        assertTrue(secondRc.freshnessMs() >= 0L,
                "freshness_ms must reflect age of the materialized join");
    }

    // ---------------------------------------------------------------------------------------
    // 8. Connector-layer rate limit — upstream provider quota protection
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Connector-layer rate limit: bursting fresh provider calls trips CONNECTOR bucket (upstream protection)")
    void connector_rate_limit_trips_on_provider_calls(@TempDir Path snapshotBase) {
        // Connector-layer limiter sits at the engine page loop, fires only when a provider call
        // actually happens (cache miss). Tight connector bucket so bursts of fresh requests trip.
        HierarchicalRateLimiterConfig tightConnector =
                HierarchicalRateLimiterConfig.forConnector(
                        new TokenBucketConfig(0.01, 1.0)); // 1 burst, ~no refill
        FederatedQueryService service =
                newService(snapshotBase, new HierarchicalRateLimiter(tightConnector));

        RequestContext ctx = aliceCtx();
        // Same query each call, but maxStaleness=Duration.ZERO forces a fresh fetch every time,
        // bypassing the cache. So each call enters the page loop and debits the connector
        // bucket. First call succeeds (burst=1); subsequent calls denied at CONNECTOR scope.
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
        assertEquals("CONNECTOR", firstFailure.violatedScope(),
                "CONNECTOR bucket (the only one configured at this layer) trips");
    }

    // ---------------------------------------------------------------------------------------
    // 12. Service-layer rate limit — Ema honors its own SLO even on cache hits
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Service-layer rate limit: bursting CACHED requests as one user still trips USER bucket (Ema's own SLO)")
    void service_rate_limit_trips_on_cache_hits(@TempDir Path snapshotBase) throws Exception {
        // Service-layer limiter sits at FederatedQueryService.execute entry. Tight user bucket
        // so the 2nd request denies — regardless of cache hit/miss. This proves Ema's per-user
        // fairness is honored even when the request never reaches an upstream provider.
        HierarchicalRateLimiterConfig tightService =
                HierarchicalRateLimiterConfig.forService(
                        new TokenBucketConfig(10.0, 20.0), // tenant — generous
                        new TokenBucketConfig(0.01, 1.0)); // user — burst=1, no refill
        FederatedQueryService service =
                newServiceWithServiceLimiter(
                        snapshotBase,
                        RateLimitPolicy.UNLIMITED, // connector layer UNLIMITED
                        new HierarchicalRateLimiter(tightService));

        RequestContext ctx = aliceCtx();
        // Identical query each call — second+ calls would be cache hits (cache exists from #1).
        // The service limiter fires BEFORE the cache check, so cache hits don't bypass the
        // limiter — that's the whole point of the service layer.
        FederatedQueryRequest req =
                new FederatedQueryRequest(
                        "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01'"
                                + " ORDER BY updatedAt DESC LIMIT 5",
                        null, null, null, null);

        // Call 1: service-layer OK (burst=1), executes, caches result.
        ResponseContext first = service.execute(ctx, req);
        assertTrue(first.isSuccess(), "first call must succeed");

        // Call 2: service-layer DENIES at USER scope — even though this would have been a cache
        // hit. The request never reaches the snapshot/engine layer.
        ResponseContext second = service.execute(ctx, req);
        assertFalse(second.isSuccess(), "second call must be denied at service layer");
        ResponseContext.Outcome.Failure f = (ResponseContext.Outcome.Failure) second.outcome();
        assertEquals(ErrorCode.RATE_LIMIT_EXHAUSTED, f.code());
        assertEquals("USER", f.violatedScope(),
                "service-layer denial reports USER scope (the tightest configured bucket)");
        assertEquals("EXHAUSTED", second.rateLimitStatus());
        assertTrue(f.retryAfterMs() != null && f.retryAfterMs() > 0L);
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

    private FederatedQueryService newService(Path snapshotBase, RateLimitPolicy connectorLimit) {
        Map<String, Connector> connectors = new LinkedHashMap<>();
        connectors.put("google", new DemoGoogleDriveConnector());
        connectors.put("notion", new DemoNotionConnector());
        return newServiceWithConnectors(snapshotBase, connectorLimit, connectors);
    }

    private FederatedQueryService newServiceWithConnectors(
            Path snapshotBase, RateLimitPolicy connectorLimit, Map<String, Connector> connectors) {
        return newServiceFull(snapshotBase, connectorLimit, RateLimitPolicy.UNLIMITED, connectors);
    }

    /** Builds a service with both limiters explicit — for the service-layer test (#12). */
    private FederatedQueryService newServiceWithServiceLimiter(
            Path snapshotBase, RateLimitPolicy connectorLimit, RateLimitPolicy serviceLimit) {
        Map<String, Connector> connectors = new LinkedHashMap<>();
        connectors.put("google", new DemoGoogleDriveConnector());
        connectors.put("notion", new DemoNotionConnector());
        return newServiceFull(snapshotBase, connectorLimit, serviceLimit, connectors);
    }

    private FederatedQueryService newServiceFull(
            Path snapshotBase,
            RateLimitPolicy connectorLimit,
            RateLimitPolicy serviceLimit,
            Map<String, Connector> connectors) {
        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor(connectorLimit);
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
                principals,
                serviceLimit);
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
