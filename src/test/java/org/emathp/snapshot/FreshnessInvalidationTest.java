package org.emathp.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
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
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.serde.SnapshotJson;
import org.emathp.web.DefaultFederatedQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FreshnessInvalidationTest {

    private static FederatedQueryService newRunner(Path base) {
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
                        new FsSnapshotStore(base),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        return new DefaultFederatedQueryService(
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
    }

    @Test
    void staleChunkFreshness_deletesSnapshotAndRestarts(@TempDir Path base) throws Exception {
        FederatedQueryService runner = newRunner(base);

        String sql =
                "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        var scope = runner.defaultCacheScope();
        RequestContext ctx = RequestContext.forCli(UserContext.anonymous(), scope);
        JsonObject first = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        Path snap = Path.of(first.get("snapshotPath").getAsString());
        assertTrue(Files.exists(snap));

        Path notionDir = snap.resolve("notion");
        try (Stream<Path> metas = Files.list(notionDir)) {
            Path meta =
                    metas.filter(p -> p.toString().endsWith("_meta.json")).findFirst().orElseThrow();
            ChunkMetadata cm =
                    SnapshotJson.mapper().readValue(meta.toFile(), ChunkMetadata.class);
            ChunkMetadata stale =
                    new ChunkMetadata(
                            cm.startRow(),
                            cm.endRow(),
                            cm.createdAt(),
                            Instant.EPOCH.toString(),
                            cm.nextCursor(),
                            cm.exhausted(),
                            cm.providerFetchSize());
            SnapshotJson.mapper().writeValue(meta.toFile(), stale);
        }

        JsonObject second = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        assertEquals("stale_restarted", second.get("freshnessDecision").getAsString());
        assertTrue(Files.exists(snap));
    }

    @Test
    void joinStaleMaterialized_restarts(@TempDir Path base) throws Exception {
        FederatedQueryService runner = newRunner(base);
        var scope = runner.defaultCacheScope();
        RequestContext ctx = RequestContext.forCli(UserContext.anonymous(), scope);
        String joinSql =
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 10";
        JsonObject first = runner.executeOrThrow(ctx, new FederatedQueryRequest(joinSql, null, null, null, null));
        Path snap = Path.of(first.get("snapshotPath").getAsString());
        Path mat = snap.resolve("_materialized");
        assertTrue(Files.exists(mat));
        try (Stream<Path> metas = Files.list(mat)) {
            Path metaPath =
                    metas.filter(p -> p.toString().endsWith("_meta.json")).findFirst().orElseThrow();
            ChunkMetadata cm =
                    SnapshotJson.mapper().readValue(metaPath.toFile(), ChunkMetadata.class);
            ChunkMetadata stale =
                    new ChunkMetadata(
                            cm.startRow(),
                            cm.endRow(),
                            cm.createdAt(),
                            Instant.EPOCH.toString(),
                            cm.nextCursor(),
                            cm.exhausted(),
                            cm.providerFetchSize());
            SnapshotJson.mapper().writeValue(metaPath.toFile(), stale);
        }
        JsonObject second = runner.executeOrThrow(ctx, new FederatedQueryRequest(joinSql, null, null, null, null));
        assertEquals("stale_restarted", second.get("freshnessDecision").getAsString());
    }
}
