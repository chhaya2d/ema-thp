package org.emathp.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.authz.demo.DemoPrincipalRegistry;
import org.emathp.cache.QueryCacheScope;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.adapters.time.SystemClock;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Mock fixtures expose the full static corpus for every {@link UserContext#userId()} (no auth).
 * Snapshot paths must not alias across principals.
 */
class MockUserDataIsolationTest {

    private FederatedQueryService runner;
    private PrincipalRegistry principals;

    @TempDir Path snapshotBase;

    @BeforeEach
    void setUp() {
        Map<String, Connector> byName = new LinkedHashMap<>();
        byName.put("google", new GoogleDriveConnector());
        byName.put("notion", new NotionConnector());
        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor();
        JoinExecutor joinExecutor = new JoinExecutor(planner, executor);
        SnapshotQueryService snapshots =
                new SnapshotQueryService(
                        planner,
                        executor,
                        new FsSnapshotStore(snapshotBase),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        principals = new DemoPrincipalRegistry();
        runner =
                new DefaultFederatedQueryService(
                        new SQLParserService(),
                        planner,
                        executor,
                        joinExecutor,
                        List.copyOf(byName.values()),
                        byName,
                        UserContext.anonymous(),
                        snapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_TESTS,
                        principals);
    }

    private JsonObject runFor(String userId, String sql) {
        QueryCacheScope scope = principals.cacheScopeFor(userId);
        UserContext user = (userId == null || userId.isBlank())
                ? UserContext.anonymous() : new UserContext(userId);
        RequestContext ctx = RequestContext.forCli(user, scope);
        return runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
    }

    @Test
    void namedUsersSeeSameMockGoogleFirstRowTitle() {
        String sql =
                "SELECT title FROM resources WHERE updatedAt > '2026-01-01' ORDER BY updatedAt DESC LIMIT 5";
        JsonObject alice = runFor("alice", sql);
        JsonObject bob = runFor("bob", sql);
        assertEquals(firstGoogleTitle(alice), firstGoogleTitle(bob));
    }

    @Test
    void snapshotPathsDifferPerUserForSameQueryHash() {
        String sql = "SELECT title FROM resources ORDER BY updatedAt DESC LIMIT 3";
        JsonObject alice = runFor("alice", sql);
        JsonObject bob = runFor("bob", sql);
        assertNotEquals(alice.get("snapshotPath").getAsString(), bob.get("snapshotPath").getAsString());
        assertTrue(alice.get("snapshotPath").getAsString().contains("u_alice"));
        assertTrue(bob.get("snapshotPath").getAsString().contains("u_bob"));
    }

    @Test
    void anonymousSnapshotUsesGuestScopeSegment() {
        String sql = "SELECT title FROM resources ORDER BY updatedAt DESC LIMIT 1";
        QueryCacheScope anonScope = QueryCacheScope.from(UserContext.anonymous());
        RequestContext ctx = RequestContext.forCli(UserContext.anonymous(), anonScope);
        JsonObject anon = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        Path p = Path.of(anon.get("snapshotPath").getAsString()).normalize();
        assertEquals(anonScope.snapshotScopeDirectoryName(), p.getParent().getFileName().toString());
    }

    private static String firstGoogleTitle(JsonObject root) {
        JsonArray sides = root.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject o = el.getAsJsonObject();
            if ("google-drive".equals(o.get("connector").getAsString())) {
                JsonObject ex = o.getAsJsonObject("execution");
                JsonArray rows = ex.getAsJsonArray("rows");
                JsonObject row0 = rows.get(0).getAsJsonObject();
                return row0.getAsJsonObject("fields").get("title").getAsString();
            }
        }
        throw new AssertionError("no google-drive side");
    }
}
