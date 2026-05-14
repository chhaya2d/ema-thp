package org.emathp.demo;

import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.config.RuntimeEnv;
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
import org.emathp.web.DefaultFederatedQueryService;

/** CLI demonstration of filesystem snapshots (prints queryHash, paths, fetch counts, freshness). */
public final class SnapshotFileDemo {

    private SnapshotFileDemo() {}

    public static void runMockDemo() {
        RuntimeEnv.loadDotEnv();
        System.out.println("=== Snapshot demo: MOCK connectors (provider batch 6 / 4, UI page 2) ===\n");
        Map<String, Connector> byName = new LinkedHashMap<>();
        byName.put("google", new GoogleDriveConnector());
        byName.put("notion", new NotionConnector());

        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor();
        JoinExecutor joinExecutor = new JoinExecutor(planner, executor);

        Path base = Path.of("data");
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
                        WebDefaults.UI_QUERY_PAGE_SIZE_MOCK,
                        PrincipalRegistry.UNRESTRICTED);

        String sql =
                "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20";
        var scope = runner.defaultCacheScope();
        RequestContext ctx = RequestContext.forCli(UserContext.anonymous(), scope);

        printStep("1) Page 0 — QueryExecutor runs; residual legs (Notion here) persist connector chunks");
        JsonObject p0 = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, null, null, null));
        printSnapshotSummary(p0);

        printStep("2) Page 1 — residual leg may reuse chunks; pure pushdown legs re-run live");
        JsonObject p1 = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, "2", null, null));
        printSnapshotSummary(p1);

        printStep("3) Page offset 6 — same pattern (Notion snapshot vs Google live)");
        JsonObject p3 = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, "6", null, null));
        printSnapshotSummary(p3);

        printStep("4) Repeat offset 6");
        JsonObject p3b = runner.executeOrThrow(ctx, new FederatedQueryRequest(sql, null, "6", null, null));
        printSnapshotSummary(p3b);

        System.out.println("\nInspect chunk files under: " + p0.get("snapshotPath").getAsString());
    }

    private static void printStep(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    private static void printSnapshotSummary(JsonObject root) {
        System.out.println("queryHash=" + root.get("queryHash").getAsString());
        System.out.println("snapshotPath=" + root.get("snapshotPath").getAsString());
        System.out.println("freshnessDecision=" + root.get("freshnessDecision").getAsString());
        System.out.println("environment=" + root.get("snapshotEnvironment").getAsString());
        var sides = root.getAsJsonArray("sides");
        for (var el : sides) {
            JsonObject s = el.getAsJsonObject();
            String name = s.get("connector").getAsString();
            System.out.println(
                    "  "
                            + name
                            + ": providerFetches="
                            + s.get("providerFetchesThisRequest").getAsInt()
                            + ", continuation="
                            + s.get("continuationFetchesThisRequest").getAsInt()
                            + ", reuseNoFetch="
                            + s.get("snapshotReuseNoProviderCall").getAsBoolean());
            if (s.has("authoritativeChunkMeta") && !s.get("authoritativeChunkMeta").isJsonNull()) {
                JsonObject m = s.getAsJsonObject("authoritativeChunkMeta");
                System.out.println(
                        "    authoritative: exhausted="
                                + m.get("exhausted")
                                + ", nextCursor="
                                + m.get("nextCursor"));
            }
        }
    }
}
