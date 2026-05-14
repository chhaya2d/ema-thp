package org.emathp.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.connector.CountingConnector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.adapters.time.SystemClock;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.web.DefaultFederatedQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * After the first request, {@link QueryExecutor} has materialized a full logical result into one
 * snapshot chunk; deeper UI pages reuse it without additional connector calls.
 */
class IncrementalSnapshotExpansionTest {

    @Test
    void firstRequest_runsExecutor_deeperUiPage_reusesSnapshotWithoutConnector(@TempDir Path base)
            throws Exception {
        CountingConnector notion = new CountingConnector(new NotionConnector());
        Map<String, Connector> byName = new LinkedHashMap<>();
        byName.put("google", new GoogleDriveConnector());
        byName.put("notion", notion);

        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor();
        JoinExecutor joinExecutor = new JoinExecutor(planner, executor);

        SnapshotQueryService snapshots =
                new SnapshotQueryService(
                        planner,
                        executor,
                        new FsSnapshotStore(base),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        FederatedQueryService runner =
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
                        2,
                        PrincipalRegistry.UNRESTRICTED);

        String sql =
                "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        var scope = runner.defaultCacheScope();
        RequestContext ctx = RequestContext.forCli(UserContext.anonymous(), scope);
        JsonObject p0 = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        int searchesAfterFirst = notion.searchCount();
        assertTrue(searchesAfterFirst >= 1);
        assertFalse(sideNotion(p0).get("snapshotReuseNoProviderCall").getAsBoolean());

        JsonObject pDeep = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, "6", null, null));
        assertEquals(searchesAfterFirst, notion.searchCount());
        assertTrue(sideNotion(pDeep).get("snapshotReuseNoProviderCall").getAsBoolean());

        Path snap = Path.of(pDeep.get("snapshotPath").getAsString());
        assertTrue(Files.exists(snap));
        assertTrue(Files.exists(snap.resolve("notion")));
    }

    private static JsonObject sideNotion(JsonObject root) {
        var sides = root.getAsJsonArray("sides");
        for (var el : sides) {
            JsonObject o = el.getAsJsonObject();
            if ("notion".equals(o.get("connector").getAsString())) {
                return o;
            }
        }
        throw new AssertionError();
    }
}
