package org.emathp.snapshot.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.connector.Connector;
import org.emathp.engine.QueryExecutor;
import org.emathp.model.EngineRow;
import org.emathp.model.Query;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.api.SidePageRequest;
import org.emathp.snapshot.api.SidePageResult;
import org.emathp.snapshot.layout.ChunkNaming;
import org.emathp.snapshot.layout.SnapshotPaths;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.ports.Clock;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Pipeline for one connector side: optionally try fresh chunks → {@link QueryExecutor} → optionally
 * persist. Whether connector chunk IO runs is delegated to callers via {@link
 * org.emathp.snapshot.api.SidePageRequest#persistConnectorSnapshot()} (see {@link
 * org.emathp.snapshot.policy.SnapshotMaterializationPolicy}).
 */
public final class SingleSourceSidePipeline {

    private SingleSourceSidePipeline() {}

    public static SidePageResult execute(
            SidePageRequest request,
            Planner planner,
            QueryExecutor executor,
            SnapshotStore store,
            Clock clock,
            Duration defaultWriteTtl)
            throws IOException {

        RequestContext ctx = request.ctx();
        Connector connector = request.connector();
        Query plannerQuery = request.plannerQuery();
        Path queryRoot = request.queryRoot();
        Duration maxStaleness = request.maxStaleness();
        Instant now = clock.now();

        PushdownPlan plan = planner.plan(connector, plannerQuery);
        Path connectorDir = queryRoot.resolve(SnapshotPaths.safeConnectorDirSegment(connector.source()));
        Files.createDirectories(queryRoot);

        boolean persistThisSide =
                plan.persistSnapshotMaterialization() && request.persistConnectorSnapshot();

        if (persistThisSide) {
            Optional<ChunkMetadata> latest = store.latestChunkMeta(connectorDir);
            if (latest.isPresent() && FreshnessPolicy.isFresh(latest.get(), maxStaleness, now)) {
                List<EngineRow> rows = loadAllRowsFromChunks(store, connectorDir);
                Integer lim = plannerQuery.limit();
                if (lim != null && rows.size() > lim) {
                    rows = List.copyOf(rows.subList(0, lim));
                }
                ChunkMetadata m = latest.get();
                String finalCursor = m.exhausted() ? null : m.nextCursor();
                QueryExecutor.ExecutionResult cached =
                        new QueryExecutor.ExecutionResult(
                                List.of(),
                                rows,
                                finalCursor,
                                false,
                                rows.size(),
                                !plan.residualOps().isEmpty());
                return new SidePageResult(
                        cached,
                        0,
                        0,
                        true,
                        List.of(),
                        m,
                        connectorDir);
            }
        }

        TagAccessPolicy tagPolicy =
                request.tagPolicy() == null ? TagAccessPolicy.unrestricted() : request.tagPolicy();
        QueryExecutor.ExecutionResult er =
                executor.execute(
                        ctx,
                        connector,
                        plan.pushedQuery(),
                        plan.residualOps(),
                        plannerQuery.limit(),
                        tagPolicy);

        Duration ttl = resolveTtl(maxStaleness, connector.defaultFreshnessTtl(), defaultWriteTtl);
        Instant freshnessUntil = now.plus(ttl);

        List<EngineRow> rows = er.rows();
        int n = rows.size();
        int lastIdx = n == 0 ? -1 : n - 1;
        boolean exhausted = er.finalNextCursor() == null;
        ChunkMetadata meta =
                n > 0
                        ? new ChunkMetadata(
                                0,
                                lastIdx,
                                now.toString(),
                                freshnessUntil.toString(),
                                er.finalNextCursor(),
                                exhausted,
                                connector.defaultFetchPageSize())
                        : null;

        if (!plan.persistSnapshotMaterialization()) {
            return new SidePageResult(
                    er,
                    er.calls().size(),
                    Math.max(0, er.calls().size() - 1),
                    false,
                    List.of(),
                    meta,
                    connectorDir);
        }

        if (!request.persistConnectorSnapshot()) {
            return new SidePageResult(
                    er,
                    er.calls().size(),
                    Math.max(0, er.calls().size() - 1),
                    false,
                    List.of(),
                    meta,
                    connectorDir);
        }

        if (store.exists(connectorDir)) {
            store.deleteRecursively(connectorDir);
        }
        Files.createDirectories(connectorDir);

        List<String> written = new ArrayList<>();
        if (n > 0 && meta != null) {
            store.writeChunk(connectorDir, 0, lastIdx, rows, meta);
            String prefix = ChunkNaming.prefix(0, lastIdx);
            written.add(ChunkNaming.dataFile(prefix));
            written.add(ChunkNaming.metaFile(prefix));
        }

        return new SidePageResult(
                er,
                er.calls().size(),
                Math.max(0, er.calls().size() - 1),
                false,
                written,
                meta,
                connectorDir);
    }

    private static List<EngineRow> loadAllRowsFromChunks(SnapshotStore store, Path connectorDir)
            throws IOException {
        List<ChunkMetadata> metas = store.listChunkMetasOrdered(connectorDir);
        List<EngineRow> all = new ArrayList<>();
        for (ChunkMetadata m : metas) {
            String prefix = ChunkNaming.prefix(m.startRow(), m.endRow());
            all.addAll(store.readChunkData(connectorDir, prefix));
        }
        return all;
    }

    /**
     * Resolution order: client {@code maxStaleness} wins; else the connector's own
     * {@link org.emathp.connector.Connector#defaultFreshnessTtl()}; else the system-wide floor.
     */
    static Duration resolveTtl(Duration clientMaxStaleness, Duration connectorTtl, Duration floor) {
        if (clientMaxStaleness != null) {
            return clientMaxStaleness;
        }
        if (connectorTtl != null) {
            return connectorTtl;
        }
        return floor;
    }
}
