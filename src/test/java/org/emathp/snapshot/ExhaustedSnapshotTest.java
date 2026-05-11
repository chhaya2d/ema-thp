package org.emathp.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
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
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.web.WebQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExhaustedSnapshotTest {

    @Test
    void afterProviderExhausted_extraUiPages_areLocalOnly(@TempDir Path base) {
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
        WebQueryRunner runner =
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
                        2);

        String sql =
                "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        runner.run(sql, null, null, null, null, runner.cacheScope());
        runner.run(sql, null, "6", null, null, runner.cacheScope());

        JsonObject loaded = runner.run(sql, null, "6", null, null, runner.cacheScope());
        JsonObject nmeta = sideNotion(loaded).getAsJsonObject("authoritativeChunkMeta");
        assertNotNull(nmeta);
        assertTrue(nmeta.get("exhausted").getAsBoolean());

        int searchesAfterMaterialized = notion.searchCount();
        JsonObject again = runner.run(sql, null, "6", null, null, runner.cacheScope());
        assertTrue(sideNotion(again).get("snapshotReuseNoProviderCall").getAsBoolean());
        assertEquals(searchesAfterMaterialized, notion.searchCount());
    }

    private static JsonObject sideNotion(JsonObject root) {
        JsonArray sides = root.getAsJsonArray("sides");
        for (JsonElement el : sides) {
            JsonObject o = el.getAsJsonObject();
            if ("notion".equals(o.get("connector").getAsString())) {
                return o;
            }
        }
        throw new AssertionError();
    }
}
