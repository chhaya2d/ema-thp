package org.emathp.snapshot.api;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.emathp.engine.QueryExecutor;
import org.emathp.planner.Planner;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.snapshot.pipeline.SingleSourceSidePipeline;
import org.emathp.snapshot.pipeline.StaleQueryTreeInspector;
import org.emathp.snapshot.ports.Clock;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Public façade for the snapshot subsystem: freshness pruning, query-info bootstrap, and per-side
 * resolution via {@link SingleSourceSidePipeline}.
 */
public final class SnapshotQueryService {

    private final Planner planner;
    private final QueryExecutor executor;
    private final SnapshotStore store;
    private final Clock clock;
    private final Duration defaultWriteTtl;
    private final boolean persistSnapshotMaterialization;

    public SnapshotQueryService(
            Planner planner,
            QueryExecutor executor,
            SnapshotStore store,
            Clock clock,
            Duration defaultWriteTtl) {
        this.planner = planner;
        this.executor = executor;
        this.store = store;
        this.clock = clock;
        this.defaultWriteTtl = defaultWriteTtl;
        this.persistSnapshotMaterialization = planner.persistSnapshotMaterialization();
    }

    public boolean persistSnapshotMaterialization() {
        return persistSnapshotMaterialization;
    }

    public Clock clock() {
        return clock;
    }

    public SnapshotStore store() {
        return store;
    }

    public Path queryRoot(SnapshotEnvironment env, String userId, String queryHash) {
        return store.querySnapshotDir(env, userId, queryHash);
    }

    /** @return {@code true} if the tree was deleted due to staleness */
    public boolean pruneStaleQueryTreeIfNeeded(Path queryRoot, Duration maxStaleness) throws IOException {
        if (!persistSnapshotMaterialization) {
            return false;
        }
        Instant now = clock.now();
        if (StaleQueryTreeInspector.isQuerySnapshotStale(queryRoot, store, now, maxStaleness)) {
            store.deleteRecursively(queryRoot);
            return true;
        }
        return false;
    }

    public void ensureQueryInfo(
            Path queryRoot, String queryHash, String userId, String normalizedQuery, String createdAt)
            throws IOException {
        if (!persistSnapshotMaterialization) {
            return;
        }
        store.ensureQueryInfo(queryRoot, queryHash, userId, normalizedQuery, createdAt);
    }

    public SidePageResult resolveSide(SidePageRequest request) throws IOException {
        return SingleSourceSidePipeline.execute(
                request, planner, executor, store, clock, defaultWriteTtl);
    }
}
