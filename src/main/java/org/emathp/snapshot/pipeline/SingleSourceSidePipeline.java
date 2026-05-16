package org.emathp.snapshot.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.connector.Connector;
import org.emathp.engine.QueryExecutor;
import org.emathp.metrics.Metrics;
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

    /**
     * In-flight cache fills, keyed by connector directory. The first thread to miss for a given
     * {@code connectorDir} installs a {@link CompletableFuture} here and runs the fetch + write;
     * other threads that miss while it's running find the same future and await its result. This
     * collapses thundering-herd misses into a single provider call + single write.
     */
    private static final ConcurrentHashMap<Path, CompletableFuture<SidePageResult>> inFlight =
            new ConcurrentHashMap<>();

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

        // Reuse the caller's plan when provided; the web runner already computes it for the
        // response JSON shape, so re-planning here would double the work for no benefit.
        PushdownPlan plan = request.plan() != null ? request.plan() : planner.plan(connector, plannerQuery);
        Path connectorDir = queryRoot.resolve(SnapshotPaths.safeConnectorDirSegment(connector.source()));
        Files.createDirectories(queryRoot);

        boolean persistThisSide =
                plan.persistSnapshotMaterialization() && request.persistConnectorSnapshot();

        if (persistThisSide) {
            Optional<ChunkMetadata> latest = store.latestChunkMeta(connectorDir);
            if (latest.isPresent() && FreshnessPolicy.isFresh(latest.get(), maxStaleness, now)) {
                Metrics.SNAPSHOT_CACHE_HITS.inc(connector.source());
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
            // Don't bump miss counter here — we don't yet know if we're the thread that will
            // actually fetch + write. Concurrent missers collapse onto one in-flight future
            // (see singleFlightFetchAndWrite); only the winner is a true miss, waiters are hits.
        }

        // Single-flight: when this side persists, exactly one thread fetches + writes for a
        // given connectorDir; concurrent missers wait on the same future and reuse its result.
        // Without persistence nothing is written, so there's no shared resource to contend over.
        if (persistThisSide) {
            return singleFlightFetchAndWrite(
                    request, executor, store, plan, connectorDir, now, defaultWriteTtl);
        }
        // Non-persist path: no cache to share, every request goes to provider.
        Metrics.SNAPSHOT_CACHE_MISSES.inc(connector.source());
        return fetchAndMaybeWrite(
                request, executor, store, plan, connectorDir, now, defaultWriteTtl);
    }

    private static SidePageResult singleFlightFetchAndWrite(
            SidePageRequest request,
            QueryExecutor executor,
            SnapshotStore store,
            PushdownPlan plan,
            Path connectorDir,
            Instant now,
            Duration defaultWriteTtl)
            throws IOException {
        CompletableFuture<SidePageResult> mine = new CompletableFuture<>();
        CompletableFuture<SidePageResult> winner = inFlight.computeIfAbsent(connectorDir, k -> mine);

        if (winner == mine) {
            // We installed the future — we're the one thread that actually fetches + writes.
            // Concurrent missers for the same connectorDir get this same future and wait below.
            Metrics.SNAPSHOT_CACHE_MISSES.inc(request.connector().source());
            try {
                SidePageResult result =
                        fetchAndMaybeWrite(
                                request, executor, store, plan, connectorDir, now, defaultWriteTtl);
                mine.complete(result);
            } catch (Throwable t) {
                mine.completeExceptionally(t);
            } finally {
                // Remove only if still ours — guards against a future leak if some other thread
                // somehow installed a fresh slot (it can't today, but the contract is "remove
                // only what I installed").
                inFlight.remove(connectorDir, mine);
            }
        } else {
            // Waiter: the winner is doing the fetch + write; we'll read the result via the
            // future. From the caller's perspective this is a cache hit — no provider call,
            // no disk write.
            Metrics.SNAPSHOT_CACHE_HITS.inc(request.connector().source());
        }

        try {
            return winner.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioe) throw ioe;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for cache fill", e);
        }
    }

    private static SidePageResult fetchAndMaybeWrite(
            SidePageRequest request,
            QueryExecutor executor,
            SnapshotStore store,
            PushdownPlan plan,
            Path connectorDir,
            Instant now,
            Duration defaultWriteTtl)
            throws IOException {
        RequestContext ctx = request.ctx();
        Connector connector = request.connector();
        Query plannerQuery = request.plannerQuery();
        Duration maxStaleness = request.maxStaleness();

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

        Duration ttl = resolveTtl(maxStaleness, connector.maxFreshnessTtl(), defaultWriteTtl);
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
     * Resolves the effective chunk-write TTL.
     *
     * <p>Both the client's {@code maxStaleness} and the connector's
     * {@link org.emathp.connector.Connector#maxFreshnessTtl()} are <em>upper bounds</em> on cache
     * age — the connector advertises how long its source's representation stays valid (e.g. a
     * search cursor's expiry), and the client expresses how stale they're willing to tolerate.
     * The stamped TTL is the {@code min} of whichever are non-null; the floor applies only when
     * both are absent.
     */
    static Duration resolveTtl(Duration clientMaxStaleness, Duration connectorTtl, Duration floor) {
        if (clientMaxStaleness == null && connectorTtl == null) {
            return floor;
        }
        if (clientMaxStaleness == null) {
            return connectorTtl;
        }
        if (connectorTtl == null) {
            return clientMaxStaleness;
        }
        return clientMaxStaleness.compareTo(connectorTtl) <= 0 ? clientMaxStaleness : connectorTtl;
    }
}
