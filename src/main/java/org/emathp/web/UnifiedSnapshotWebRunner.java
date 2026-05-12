package org.emathp.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.emathp.auth.UserContext;
import org.emathp.cache.ParsedQueryNormalizer;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;
import org.emathp.model.ResidualOps;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;
import org.emathp.snapshot.api.SidePageRequest;
import org.emathp.snapshot.api.SidePageResult;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.layout.QuerySnapshotHasher;
import org.emathp.federation.MaterializedPage;
import org.emathp.snapshot.model.LogicalPagination;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.pagination.UiResponsePaging;
import org.emathp.snapshot.pipeline.FullMaterializationCoordinator;
import org.emathp.snapshot.policy.SnapshotMaterializationPolicy;

/**
 * HTTP JSON over parsed SQL: single-source uses per-connector snapshot policy; engine-composed
 * queries use {@link FullMaterializationCoordinator} (same “full materialise” bucket as residual).
 */
final class UnifiedSnapshotWebRunner {

    private final Planner planner;
    private final JoinExecutor joinExecutor;
    private final List<Connector> connectors;
    private final Map<String, Connector> connectorsByName;
    private final SnapshotQueryService snapshotQueryService;
    private final SnapshotEnvironment snapshotEnv;
    private final int uiPageSize;

    UnifiedSnapshotWebRunner(
            Planner planner,
            JoinExecutor joinExecutor,
            List<Connector> connectors,
            Map<String, Connector> connectorsByName,
            SnapshotQueryService snapshotQueryService,
            SnapshotEnvironment snapshotEnv,
            int uiPageSize) {
        this.planner = planner;
        this.joinExecutor = joinExecutor;
        this.connectors = connectors;
        this.connectorsByName = connectorsByName;
        this.snapshotQueryService = snapshotQueryService;
        this.snapshotEnv = snapshotEnv;
        this.uiPageSize = uiPageSize;
    }

    JsonObject runParsed(
            ParsedQuery parsed,
            Integer pageNumber,
            String uiCursorOffset,
            Integer requestPageSize,
            Duration maxStaleness,
            UserContext requestUser)
            throws IOException {

        int effectivePageSize = effectiveUiPageSize(requestPageSize);
        int startRow = computeStartRow(pageNumber, uiCursorOffset, effectivePageSize);

        if (SnapshotMaterializationPolicy.requiresFullMaterialization(parsed)) {
            return renderFullMaterializationResponse(
                    parsed, startRow, effectivePageSize, maxStaleness, requestUser);
        }
        return runSingle((Query) parsed, startRow, effectivePageSize, maxStaleness, requestUser);
    }

    private JsonObject runSingle(
            Query rawQuery, int startRow, int effectivePageSize, Duration maxStaleness, UserContext requestUser)
            throws IOException {
        Query plannerQuery = rawQuery.withPagination(null, 1);

        String normalized = ParsedQueryNormalizer.canonicalSnapshotIdentity(rawQuery);
        String queryHash = QuerySnapshotHasher.hashForPath(normalized);
        Path queryRoot = snapshotQueryService.queryRoot(snapshotEnv, requestUser.userId(), queryHash);

        Instant now = Instant.now();
        boolean staleRestarted =
                snapshotQueryService.pruneStaleQueryTreeIfNeeded(queryRoot, maxStaleness);
        snapshotQueryService.ensureQueryInfo(
                queryRoot, queryHash, requestUser.userId(), normalized, now.toString());

        String freshnessDecision = staleRestarted ? "stale_restarted" : "fresh";

        JsonObject root = new JsonObject();
        root.addProperty("kind", "single");
        root.addProperty("ok", true);
        root.addProperty("queryHash", queryHash);
        root.addProperty("snapshotPath", queryRoot.toString());
        root.addProperty("freshnessDecision", freshnessDecision);
        root.addProperty("snapshotEnvironment", snapshotEnv.name());
        root.addProperty("snapshotBacked", snapshotQueryService.persistSnapshotMaterialization());
        if (maxStaleness != null) {
            root.addProperty("maxStaleness", maxStaleness.toString());
        } else {
            root.add("maxStaleness", JsonNull.INSTANCE);
        }

        JsonArray sides = new JsonArray();
        for (Connector c : connectors) {
            sides.add(executeSideSnapshot(plannerQuery, c, queryRoot, maxStaleness, requestUser));
        }
        root.add("sides", sides);

        enrichSingleSourceResumeCursors(root);
        attachSnapshotAudit(root, now);
        UiResponsePaging.applyToSingleConnectorSides(root, startRow, effectivePageSize);
        return root;
    }

    private JsonObject renderFullMaterializationResponse(
            ParsedQuery pq,
            int startRow,
            int effectivePageSize,
            Duration maxStaleness,
            UserContext requestUser)
            throws IOException {
        JoinQuery jq = (JoinQuery) pq;
        boolean snap = snapshotQueryService.persistSnapshotMaterialization();
        String normalized = ParsedQueryNormalizer.canonicalSnapshotIdentity(jq);
        String queryHash = QuerySnapshotHasher.hashForPath(normalized);
        Path queryRoot = snapshotQueryService.queryRoot(snapshotEnv, requestUser.userId(), queryHash);

        Instant now = snapshotQueryService.clock().now();
        boolean staleRestarted =
                snapshotQueryService.pruneStaleQueryTreeIfNeeded(queryRoot, maxStaleness);
        snapshotQueryService.ensureQueryInfo(
                queryRoot, queryHash, requestUser.userId(), normalized, now.toString());

        FullMaterializationCoordinator.Outcome out =
                FullMaterializationCoordinator.run(
                        jq,
                        snap,
                        queryRoot,
                        maxStaleness,
                        requestUser,
                        joinExecutor,
                        connectorsByName,
                        snapshotQueryService.store(),
                        snapshotQueryService.clock());

        JsonArray pages = materializedPageArray(out.paged());

        JsonObject root = new JsonObject();
        root.addProperty("kind", "join");
        root.addProperty("ok", true);
        root.addProperty("queryHash", queryHash);
        root.addProperty("snapshotPath", queryRoot.toString());
        root.addProperty("freshnessDecision", staleRestarted ? "stale_restarted" : "fresh");
        root.addProperty("snapshotEnvironment", snapshotEnv.name());
        root.addProperty("snapshotBacked", snap);
        root.addProperty(QueryResponseJsonKeys.FULL_MATERIALIZATION_REUSE, out.reusedFromDisk());
        if (maxStaleness != null) {
            root.addProperty("maxStaleness", maxStaleness.toString());
        } else {
            root.add("maxStaleness", JsonNull.INSTANCE);
        }
        root.add("pages", pages);
        attachSnapshotAudit(root, now);
        UiResponsePaging.applyToFirstPageRows(
                root, QueryResponseJsonKeys.PAGE_ROWS, startRow, effectivePageSize);
        return root;
    }

    private JsonArray materializedPageArray(MaterializedPage jr) {
        JsonArray pages = new JsonArray();
        pages.add(materializedPageBody(1, jr));
        return pages;
    }

    private JsonObject materializedPageBody(int pageNum, MaterializedPage result) {
        JsonObject o = new JsonObject();
        o.addProperty("page", pageNum);
        o.addProperty("upstreamRowCount", result.upstreamRowCount());
        o.addProperty("stoppedAtLimit", result.stoppedAtLimit());
        o.addProperty("nextCursor", result.nextCursor());
        JsonArray rows = new JsonArray();
        for (EngineRow row : result.rows()) {
            rows.add(engineRowJson(row));
        }
        o.add(QueryResponseJsonKeys.PAGE_ROWS, rows);
        return o;
    }

    private int computeStartRow(Integer pageNumber, String uiCursorOffset, int pageSize) {
        if (pageNumber != null) {
            return LogicalPagination.startRow(pageNumber, pageSize);
        }
        return UiResponsePaging.parseUiOffset(uiCursorOffset);
    }

    private int effectiveUiPageSize(Integer requestPageSize) {
        int ps = requestPageSize != null ? requestPageSize : uiPageSize;
        if (ps < 1) {
            throw new IllegalArgumentException("pageSize must be >= 1");
        }
        return UiResponsePaging.clampClientPageSize(ps);
    }

    private JsonObject executeSideSnapshot(
            Query plannerQuery,
            Connector connector,
            Path queryRoot,
            Duration maxStaleness,
            UserContext requestUser)
            throws IOException {

        PushdownPlan plan = planner.plan(connector, plannerQuery);
        SidePageResult out =
                snapshotQueryService.resolveSide(
                        new SidePageRequest(
                                requestUser,
                                connector,
                                plannerQuery,
                                queryRoot,
                                maxStaleness,
                                SnapshotMaterializationPolicy.persistConnectorSideChunks(plan)));

        QueryExecutor.ExecutionResult exec = out.execution();

        JsonObject o = new JsonObject();
        o.addProperty("connector", connector.source());
        o.addProperty("pushedSummary", pushedOps(plan.pushedQuery()).toString());
        o.addProperty("pending", plan.pendingOperations().toString());
        o.addProperty("residual", formatResidual(plan.residualOps()));

        JsonObject ex = new JsonObject();
        ex.add("calls", callsJson(exec));
        ex.addProperty("finalNextCursor", exec.finalNextCursor());
        ex.addProperty("stoppedAtLimit", exec.stoppedAtLimit());
        ex.addProperty("rowsFromConnector", exec.rowsFromConnector());
        ex.addProperty("residualApplied", exec.residualApplied());
        ex.add("rows", rowsJson(exec.rows()));

        o.add("execution", ex);

        o.addProperty("providerFetchesThisRequest", out.providerFetches());
        o.addProperty("continuationFetchesThisRequest", out.continuationFetches());
        o.addProperty("snapshotReuseNoProviderCall", out.snapshotReuseNoProviderCall());

        JsonArray chunkCreated = new JsonArray();
        for (String f : out.chunkFilesCreatedThisRequest()) {
            chunkCreated.add(f);
        }
        o.add("chunkFilesCreatedThisRequest", chunkCreated);
        o.addProperty("connectorSnapshotDir", out.connectorSnapshotDir().toString());

        if (out.authoritativeChunkMeta() != null) {
            JsonObject meta = new JsonObject();
            meta.addProperty("startRow", out.authoritativeChunkMeta().startRow());
            meta.addProperty("endRow", out.authoritativeChunkMeta().endRow());
            meta.addProperty("freshnessUntil", out.authoritativeChunkMeta().freshnessUntil());
            meta.addProperty("nextCursor", out.authoritativeChunkMeta().nextCursor());
            meta.addProperty("exhausted", out.authoritativeChunkMeta().exhausted());
            meta.addProperty("providerFetchSize", out.authoritativeChunkMeta().providerFetchSize());
            o.add("authoritativeChunkMeta", meta);
        } else {
            o.add("authoritativeChunkMeta", JsonNull.INSTANCE);
        }

        return o;
    }

    private static void attachSnapshotAudit(JsonObject root, Instant runAt) {
        root.addProperty("snapshotMaterializationAt", runAt.toString());
    }

    private static void enrichSingleSourceResumeCursors(JsonObject root) {
        JsonObject map = new JsonObject();
        String kind = root.get("kind").getAsString();
        if ("single".equals(kind) && root.has("sides")) {
            for (JsonElement el : root.getAsJsonArray("sides")) {
                JsonObject side = el.getAsJsonObject();
                String connector = side.get("connector").getAsString();
                JsonObject exec = side.getAsJsonObject("execution");
                if (exec.has("finalNextCursor") && !exec.get("finalNextCursor").isJsonNull()) {
                    map.addProperty(connector, exec.get("finalNextCursor").getAsString());
                }
            }
        }
        root.add("resumeCursors", map);
    }

    private static JsonArray callsJson(QueryExecutor.ExecutionResult exec) {
        JsonArray a = new JsonArray();
        for (int i = 0; i < exec.calls().size(); i++) {
            QueryExecutor.PageCall call = exec.calls().get(i);
            JsonObject c = new JsonObject();
            c.addProperty("page", i + 1);
            c.addProperty("cursor", call.cursor());
            c.addProperty("rowsReturned", call.rowsReturned());
            c.addProperty("nextCursor", call.nextCursor());
            a.add(c);
        }
        return a;
    }

    private static JsonArray rowsJson(List<EngineRow> rows) {
        JsonArray a = new JsonArray();
        for (EngineRow r : rows) {
            a.add(engineRowJson(r));
        }
        return a;
    }

    private static JsonObject engineRowJson(EngineRow row) {
        JsonObject o = new JsonObject();
        o.add("fields", fieldMapToJson(row.fields()));
        return o;
    }

    private static JsonObject fieldMapToJson(Map<String, Object> fields) {
        JsonObject m = new JsonObject();
        for (var e : fields.entrySet()) {
            m.add(e.getKey(), valueToJsonElement(e.getValue()));
        }
        return m;
    }

    private static JsonElement valueToJsonElement(Object v) {
        if (v == null) {
            return JsonNull.INSTANCE;
        }
        if (v instanceof Instant i) {
            return new JsonPrimitive(i.toString());
        }
        if (v instanceof Number n) {
            return new JsonPrimitive(n);
        }
        if (v instanceof Boolean b) {
            return new JsonPrimitive(b);
        }
        if (v instanceof List<?> list) {
            JsonArray arr = new JsonArray();
            for (Object x : list) {
                arr.add(valueToJsonElement(x));
            }
            return arr;
        }
        return new JsonPrimitive(v.toString());
    }

    private static JsonArray planPushedArray(ConnectorQuery cq) {
        JsonArray a = new JsonArray();
        for (String s : pushedOps(cq)) {
            a.add(s);
        }
        return a;
    }

    private static List<String> pushedOps(ConnectorQuery cq) {
        List<String> ops = new ArrayList<>();
        if (cq.where() != null) {
            ops.add("WHERE");
        }
        if (!cq.orderBy().isEmpty()) {
            ops.add("ORDER BY");
        }
        if (!cq.projection().isEmpty()) {
            ops.add("PROJECTION");
        }
        if (cq.pageSize() != null || cq.cursor() != null) {
            ops.add("PAGINATION");
        }
        return ops;
    }

    private static String formatResidual(ResidualOps res) {
        if (res.isEmpty()) {
            return "none";
        }
        List<String> parts = new ArrayList<>();
        if (res.where() != null) {
            ComparisonExpr w = res.where();
            parts.add("WHERE " + w.field() + " " + w.operator() + " " + w.value());
        }
        if (!res.orderBy().isEmpty()) {
            parts.add(
                    "ORDER BY " + res.orderBy().stream()
                            .map(o -> o.field() + " " + o.direction())
                            .collect(Collectors.joining(", ")));
        }
        return String.join("; ", parts);
    }
}
