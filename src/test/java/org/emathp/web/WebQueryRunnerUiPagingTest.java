package org.emathp.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.emathp.auth.UserContext;
import org.emathp.cache.QueryCacheScope;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.adapters.time.SystemClock;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WebQueryRunnerUiPagingTest {

    private WebQueryRunner runner;
    private QueryCacheScope scope;

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
        runner =
                new WebQueryRunner(
                        new SQLParserService(),
                        planner,
                        executor,
                        joinExecutor,
                        List.copyOf(byName.values()),
                        byName,
                        UserContext.anonymous(),
                        snapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_TESTS);
        scope = DemoPrincipalRegistry.cacheScope("alice");
    }

    private static JsonObject googleDriveSide(JsonObject root) {
        JsonArray sides = root.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject o = el.getAsJsonObject();
            if ("google-drive".equals(o.get("connector").getAsString())) {
                return o;
            }
        }
        throw new AssertionError("no google-drive side");
    }

    private static String firstGoogleDriveTitle(JsonObject root) {
        JsonArray rows = googleDriveSide(root).getAsJsonObject("execution").getAsJsonArray("rows");
        return rows.get(0).getAsJsonObject().getAsJsonObject("fields").get("title").getAsString();
    }

    private static JsonObject notionSide(JsonObject root) {
        JsonArray sides = root.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject o = el.getAsJsonObject();
            if ("notion".equals(o.get("connector").getAsString())) {
                return o;
            }
        }
        throw new AssertionError("no notion side");
    }

    private static String firstNotionTitle(JsonObject root) {
        JsonArray rows = notionSide(root).getAsJsonObject("execution").getAsJsonArray("rows");
        return rows.get(0).getAsJsonObject().getAsJsonObject("fields").get("title").getAsString();
    }

    /** Same logical query shape for every connector side (see {@link FederatedDemosTest} demos). */
    private static void assertNotion_sqlWide_residualSortAndSnapshots(
            JsonObject root, boolean expectSnapshotReuse, boolean expectRecordedProviderCalls) {
        JsonObject n = notionSide(root);
        assertEquals("[ORDER BY, PROJECTION, PAGINATION]", n.get("pending").getAsString());
        assertEquals("[WHERE]", n.get("pushedSummary").getAsString());
        assertEquals("ORDER BY updatedAt DESC", n.get("residual").getAsString());

        JsonObject ex = n.getAsJsonObject("execution");
        assertTrue(ex.get("residualApplied").getAsBoolean());
        assertEquals(8, ex.get("uiRowTotal").getAsInt());

        boolean reuse = n.get("snapshotReuseNoProviderCall").getAsBoolean();
        assertEquals(expectSnapshotReuse, reuse);
        int callCount = ex.getAsJsonArray("calls").size();
        if (expectRecordedProviderCalls) {
            assertTrue(callCount >= 1, "First materialization must call the Notion mock at least once");
            assertTrue(
                    n.get("providerFetchesThisRequest").getAsInt() >= 1,
                    "Provider fetches counted on snapshot miss");
            // Pagination is residual for Notion; pushed ConnectorQuery leaves pageSize null and
            // MockNotionApi treats null as unlimited — so often one batch, unlike multi-page scans.
        } else {
            assertEquals(0, callCount);
            assertEquals(0, n.get("providerFetchesThisRequest").getAsInt());
        }
    }

    private static String sqlWide() {
        return "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
    }

    @Test
    void parseUiOffset_trimsAndClamps() {
        assertEquals(0, WebQueryRunner.parseUiOffset(null));
        assertEquals(0, WebQueryRunner.parseUiOffset(""));
        assertEquals(0, WebQueryRunner.parseUiOffset("   "));
        assertEquals(3, WebQueryRunner.parseUiOffset(" 3 "));
        assertEquals(0, WebQueryRunner.parseUiOffset("-5"));
    }

    @Test
    void invalidCursor_rejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> runner.run(sqlWide(), null, "not-a-number", null, null, scope));
    }

    @Test
    void singleSource_fromNotion_runsNotionSideOnly() {
        String sql =
                "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        JsonObject root = runner.run(sql, null, null, null, null, scope);
        assertEquals("single", root.get("kind").getAsString());
        assertEquals(1, root.getAsJsonArray("targetConnectors").size());
        assertEquals("notion", root.getAsJsonArray("targetConnectors").get(0).getAsString());
        assertEquals(1, root.getAsJsonArray("sides").size());
        assertEquals("notion", notionSide(root).get("connector").getAsString());
        assertTrue(root.has("serverElapsedMs"));
    }

    @Test
    void singleSource_fromGoogle_runsGoogleSideOnly() {
        String sql =
                "SELECT title, updatedAt FROM google WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        JsonObject root = runner.run(sql, null, null, null, null, scope);
        assertEquals(1, root.getAsJsonArray("sides").size());
        assertEquals("google-drive", googleDriveSide(root).get("connector").getAsString());
    }

    @Test
    void secondWindow_slicesRows_andReusesSnapshotWithoutProviderCall() {
        JsonObject p1 = runner.run(sqlWide(), null, null, null, null, scope);
        assertEquals("fresh", p1.get("freshnessDecision").getAsString());
        assertTrue(p1.get("uiPagingSupported").getAsBoolean());
        assertFalse(googleDriveSide(p1).get("snapshotReuseNoProviderCall").getAsBoolean());
        assertEquals(2, googleDriveSide(p1).getAsJsonObject("execution").getAsJsonArray("rows").size());
        assertEquals(8, googleDriveSide(p1).getAsJsonObject("execution").get("uiRowTotal").getAsInt());
        assertEquals("2", p1.get("uiNextCursor").getAsString());
        assertNotion_sqlWide_residualSortAndSnapshots(p1, false, true);

        JsonObject p2 = runner.run(sqlWide(), null, "2", null, null, scope);
        assertEquals("fresh", p2.get("freshnessDecision").getAsString());
        assertFalse(
                googleDriveSide(p2).get("snapshotReuseNoProviderCall").getAsBoolean(),
                "Pure pushdown leg has no residual → connector chunks not cached");
        assertEquals(2, googleDriveSide(p2).getAsJsonObject("execution").getAsJsonArray("rows").size());
        assertNotEquals(firstGoogleDriveTitle(p1), firstGoogleDriveTitle(p2));
        assertNotion_sqlWide_residualSortAndSnapshots(p2, true, false);
        assertNotEquals(firstNotionTitle(p1), firstNotionTitle(p2));

        JsonObject pLast = runner.run(sqlWide(), null, "6", null, null, scope);
        assertFalse(googleDriveSide(pLast).get("snapshotReuseNoProviderCall").getAsBoolean());
        assertEquals(2, googleDriveSide(pLast).getAsJsonObject("execution").getAsJsonArray("rows").size());
        assertEquals(8, googleDriveSide(pLast).getAsJsonObject("execution").get("uiRowTotal").getAsInt());
        assertTrue(pLast.get("uiNextCursor").isJsonNull());
        assertNotion_sqlWide_residualSortAndSnapshots(pLast, true, false);

        JsonObject totals = pLast.getAsJsonObject("uiRowTotalsByConnector");
        assertEquals(8, totals.get("google-drive").getAsInt());
        assertEquals(8, totals.get("notion").getAsInt());
        assertFalse(Objects.equals(firstNotionTitle(p1), firstNotionTitle(pLast)));
    }

    @Test
    void join_snapshotsRows_onlySecondPageReusesDisk() {
        String sql =
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 10";
        JsonObject r1 = runner.run(sql, null, null, null, null, scope);
        assertEquals("join", r1.get("kind").getAsString());
        assertTrue(r1.get("snapshotBacked").getAsBoolean());
        assertFalse(r1.get("fullMaterializationReuse").getAsBoolean());
        JsonObject page0 = r1.getAsJsonArray("pages").get(0).getAsJsonObject();
        assertFalse(page0.has("left"));
        assertFalse(page0.has("right"));

        JsonObject r2 = runner.run(sql, null, "2", null, null, scope);
        assertTrue(r2.get("fullMaterializationReuse").getAsBoolean());
    }
}
