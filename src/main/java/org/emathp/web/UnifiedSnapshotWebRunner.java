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
import org.emathp.authz.Principal;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.authz.ScopeAndPolicy;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.cache.ParsedQueryNormalizer;
import org.emathp.cache.QueryCacheScope;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.federation.MaterializedPage;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;
import org.emathp.model.ResidualOps;
import org.emathp.pagination.UiResponsePaging;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.api.SidePageRequest;
import org.emathp.snapshot.api.SidePageResult;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.layout.QuerySnapshotHasher;
import org.emathp.snapshot.model.LogicalPagination;
import org.emathp.snapshot.model.SnapshotEnvironment;
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
    private final PrincipalRegistry principals;

    UnifiedSnapshotWebRunner(
            Planner planner,
            JoinExecutor joinExecutor,
            List<Connector> connectors,
            Map<String, Connector> connectorsByName,
            SnapshotQueryService snapshotQueryService,
            SnapshotEnvironment snapshotEnv,
            int uiPageSize,
            PrincipalRegistry principals) {
        this.planner = planner;
        this.joinExecutor = joinExecutor;
        this.connectors = connectors;
        this.connectorsByName = connectorsByName;
        this.snapshotQueryService = snapshotQueryService;
        this.snapshotEnv = snapshotEnv;
        this.uiPageSize = uiPageSize;
        this.principals = principals;
    }

    private TagAccessPolicy tagPolicyFor(QueryCacheScope scope) {
        Principal p = principals.lookup(new UserContext(scope.userId()));
        return ScopeAndPolicy.tagPolicy(p, scope.roleSlug());
    }

    JsonObject runParsed(RequestContext ctx, ParsedQuery parsed, FederatedQueryRequest req)
            throws IOException {

        int effectivePageSize = effectiveUiPageSize(req.requestPageSize());
        int startRow = computeStartRow(req.pageNumber(), req.logicalCursorOffset(), effectivePageSize);

        if (SnapshotMaterializationPolicy.requiresFullMaterialization(parsed)) {
            return renderFullMaterializationResponse(
                    ctx, parsed, startRow, effectivePageSize, req.maxStaleness());
        }
        return runSingle(ctx, (Query) parsed, startRow, effectivePageSize, req.maxStaleness());
    }

    private JsonObject runSingle(
            RequestContext ctx, Query rawQuery, int startRow, int effectivePageSize, Duration maxStaleness)
            throws IOException {
        Query plannerQuery = rawQuery.withPagination(null, 1);
        QueryCacheScope scope = ctx.scope();
        TagAccessPolicy tagPolicy = tagPolicyFor(scope);

        String normalized = ParsedQueryNormalizer.canonicalSnapshotIdentity(rawQuery);
        String queryHash = QuerySnapshotHasher.hashForPath(normalized);
        Path queryRoot =
                snapshotQueryService.queryRoot(snapshotEnv, scope.snapshotScopeDirectoryName(), queryHash);

        Instant now = Instant.now();
        boolean staleRestarted =
                snapshotQueryService.pruneStaleQueryTreeIfNeeded(queryRoot, maxStaleness);
        snapshotQueryService.ensureQueryInfo(
                queryRoot, queryHash, scope.snapshotScopeDirectoryName(), normalized, now.toString());

        String freshnessDecision = staleRestarted ? "stale_restarted" : "fresh";

        JsonObject root = new JsonObject();
        root.addProperty("kind", "single");
        root.addProperty("ok", true);
        root.addProperty("queryHash", queryHash);
        root.addProperty("snapshotPath", queryRoot.toString());
        root.addProperty("freshnessDecision", freshnessDecision);
        root.addProperty(
                "snapshotTreeNote",
                "freshnessDecision=stale_restarted means the on-disk query tree was deleted before this run; "
                        + "per-side cached vs live is serveMode / snapshotReuseNoProviderCall.");
        root.addProperty("snapshotEnvironment", snapshotEnv.name());
        root.addProperty("snapshotBacked", snapshotQueryService.persistSnapshotMaterialization());
        if (maxStaleness != null) {
            root.addProperty("maxStaleness", maxStaleness.toString());
        } else {
            root.add("maxStaleness", JsonNull.INSTANCE);
        }

        root.addProperty("tenantId", scope.tenantId());
        root.addProperty("roleSlug", scope.roleSlug());

        List<Connector> activeConnectors =
                SingleSourceConnectorSelector.connectorsForSingleSource(
                        rawQuery.fromTable(), connectors, connectorsByName);

        JsonArray targets = new JsonArray();
        for (Connector c : activeConnectors) {
            targets.add(new JsonPrimitive(c.source()));
        }
        root.add("targetConnectors", targets);

        JsonArray sides = new JsonArray();
        for (Connector c : activeConnectors) {
            sides.add(executeSideSnapshot(ctx, plannerQuery, c, queryRoot, maxStaleness, tagPolicy));
        }
        root.add("sides", sides);
        root.addProperty("providerFetchSummary", summarizeSidesFetchMode(sides));
        attachFreshnessMs(root, sides, now);

        enrichSingleSourceResumeCursors(root);
        attachSnapshotAudit(root, now);
        UiResponsePaging.applyToSingleConnectorSides(root, startRow, effectivePageSize);
        return root;
    }

    /**
     * Aggregates per-side {@code freshnessMs} (already attached by {@link #executeSideSnapshot})
     * into a root-level {@code freshness_ms} = age of the oldest used chunk across sides. Null
     * when no side carried a chunk (e.g., zero-row result).
     */
    private static void attachFreshnessMs(JsonObject root, JsonArray sides, Instant now) {
        Long oldest = null;
        for (JsonElement el : sides) {
            JsonObject side = el.getAsJsonObject();
            if (!side.has("freshnessMs") || side.get("freshnessMs").isJsonNull()) {
                continue;
            }
            long ms = side.get("freshnessMs").getAsLong();
            if (oldest == null || ms > oldest) {
                oldest = ms;
            }
        }
        if (oldest != null) {
            root.addProperty("freshness_ms", oldest);
        } else {
            root.add("freshness_ms", JsonNull.INSTANCE);
        }
    }

    private JsonObject renderFullMaterializationResponse(
            RequestContext ctx,
            ParsedQuery pq,
            int startRow,
            int effectivePageSize,
            Duration maxStaleness)
            throws IOException {
        JoinQuery jq = (JoinQuery) pq;
        QueryCacheScope scope = ctx.scope();
        TagAccessPolicy tagPolicy = tagPolicyFor(scope);
        boolean snap = snapshotQueryService.persistSnapshotMaterialization();
        String normalized = ParsedQueryNormalizer.canonicalSnapshotIdentity(jq);
        String queryHash = QuerySnapshotHasher.hashForPath(normalized);
        Path queryRoot =
                snapshotQueryService.queryRoot(snapshotEnv, scope.snapshotScopeDirectoryName(), queryHash);

        Instant now = snapshotQueryService.clock().now();
        boolean staleRestarted =
                snapshotQueryService.pruneStaleQueryTreeIfNeeded(queryRoot, maxStaleness);
        snapshotQueryService.ensureQueryInfo(
                queryRoot, queryHash, scope.snapshotScopeDirectoryName(), normalized, now.toString());

        FullMaterializationCoordinator.Outcome out =
                FullMaterializationCoordinator.run(
                        ctx,
                        jq,
                        snap,
                        queryRoot,
                        maxStaleness,
                        joinExecutor,
                        connectorsByName,
                        snapshotQueryService.store(),
                        snapshotQueryService.clock(),
                        tagPolicy);

        JsonArray pages = materializedPageArray(out.paged());

        JsonObject root = new JsonObject();
        root.addProperty("kind", "join");
        root.addProperty("ok", true);
        root.addProperty("queryHash", queryHash);
        root.addProperty("snapshotPath", queryRoot.toString());
        root.addProperty("tenantId", scope.tenantId());
        root.addProperty("roleSlug", scope.roleSlug());
        root.addProperty("freshnessDecision", staleRestarted ? "stale_restarted" : "fresh");
        root.addProperty("snapshotEnvironment", snapshotEnv.name());
        root.addProperty("snapshotBacked", snap);
        root.addProperty(QueryResponseJsonKeys.FULL_MATERIALIZATION_REUSE, out.reusedFromDisk());
        root.addProperty("freshness_ms", out.freshnessMs());
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
            RequestContext ctx,
            Query plannerQuery,
            Connector connector,
            Path queryRoot,
            Duration maxStaleness,
            TagAccessPolicy tagPolicy)
            throws IOException {

        PushdownPlan plan = planner.plan(connector, plannerQuery);
        SidePageResult out =
                snapshotQueryService.resolveSide(
                        new SidePageRequest(
                                ctx,
                                connector,
                                plannerQuery,
                                queryRoot,
                                maxStaleness,
                                SnapshotMaterializationPolicy.persistConnectorSideChunks(plan),
                                tagPolicy));

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
        o.addProperty("serveMode", out.snapshotReuseNoProviderCall() ? "cached" : "live");

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
            meta.addProperty("createdAt", out.authoritativeChunkMeta().createdAt());
            meta.addProperty("freshnessUntil", out.authoritativeChunkMeta().freshnessUntil());
            meta.addProperty("nextCursor", out.authoritativeChunkMeta().nextCursor());
            meta.addProperty("exhausted", out.authoritativeChunkMeta().exhausted());
            meta.addProperty("providerFetchSize", out.authoritativeChunkMeta().providerFetchSize());
            o.add("authoritativeChunkMeta", meta);
            long ageMs =
                    Math.max(
                            0L,
                            Instant.now().toEpochMilli()
                                    - Instant.parse(out.authoritativeChunkMeta().createdAt())
                                            .toEpochMilli());
            o.addProperty("freshnessMs", ageMs);
        } else {
            o.add("authoritativeChunkMeta", JsonNull.INSTANCE);
            o.add("freshnessMs", JsonNull.INSTANCE);
        }

        return o;
    }

    private static String summarizeSidesFetchMode(JsonArray sides) {
        boolean anyCached = false;
        boolean anyLive = false;
        for (JsonElement el : sides) {
            JsonObject s = el.getAsJsonObject();
            boolean reuse =
                    s.has("snapshotReuseNoProviderCall")
                            && !s.get("snapshotReuseNoProviderCall").isJsonNull()
                            && s.get("snapshotReuseNoProviderCall").getAsBoolean();
            if (reuse) {
                anyCached = true;
            } else {
                anyLive = true;
            }
        }
        if (anyCached && !anyLive) {
            return "all_cached";
        }
        if (!anyCached && anyLive) {
            return "all_live";
        }
        if (anyCached && anyLive) {
            return "mixed";
        }
        return "all_live";
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
