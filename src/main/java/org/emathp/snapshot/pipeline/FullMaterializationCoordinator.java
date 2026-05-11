package org.emathp.snapshot.pipeline;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.emathp.auth.UserContext;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.federation.MaterializedPage;
import org.emathp.federation.MaterializedRowSet;
import org.emathp.federation.OffsetCursorPager;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.ports.Clock;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Runs engine-composed queries with the same snapshot semantics as residual single-source work:
 * always fully materialise when persistence is enabled (see {@link org.emathp.snapshot.policy.SnapshotMaterializationPolicy#requiresFullMaterialization}).
 */
public final class FullMaterializationCoordinator {

    private FullMaterializationCoordinator() {}

    public record Outcome(MaterializedPage paged, boolean reusedFromDisk) {}

    public static Outcome run(
            JoinQuery jq,
            boolean persistenceEnabled,
            Path queryRoot,
            Duration maxStaleness,
            UserContext user,
            JoinExecutor joinExecutor,
            Map<String, Connector> connectorsByName,
            SnapshotStore store,
            Clock clock)
            throws IOException {

        if (!persistenceEnabled) {
            return new Outcome(
                    materializeAndPage(joinExecutor, user, connectorsByName, jq), false);
        }

        Instant now = clock.now();
        Optional<FullMaterializedResultSnapshot.CacheHit> hit =
                FullMaterializedResultSnapshot.tryHit(queryRoot, maxStaleness, store, clock);
        if (hit.isPresent()) {
            FullMaterializedResultSnapshot.CacheHit h = hit.get();
            MaterializedPage paged =
                    OffsetCursorPager.page(
                            h.cappedRows(),
                            h.upstreamRowCount(),
                            h.stoppedAtLimit(),
                            jq.cursor(),
                            jq.pageSize());
            return new Outcome(paged, true);
        }

        List<EngineRow> combined = joinExecutor.materialize(user, connectorsByName, jq);
        MaterializedRowSet rowSet = MaterializedRowSet.limitedFrom(combined, jq.limit());
        MaterializedPage paged = OffsetCursorPager.page(rowSet, jq.cursor(), jq.pageSize());

        Duration ttl = maxStaleness != null ? maxStaleness : WebDefaults.snapshotChunkFreshness();
        Instant freshnessUntil = now.plus(ttl);
        int n = rowSet.limitedRows().size();
        int lastIdx = n == 0 ? -1 : n - 1;
        ChunkMetadata meta =
                new ChunkMetadata(
                        0,
                        lastIdx,
                        now.toString(),
                        freshnessUntil.toString(),
                        null,
                        true,
                        0);
        JsonObject manifest = new JsonObject();
        manifest.addProperty("upstreamRowCount", rowSet.totalBeforeLimit());
        manifest.addProperty("stoppedAtLimit", rowSet.stoppedAtLimit());
        FullMaterializedResultSnapshot.writeIfNonEmpty(
                queryRoot, store, rowSet.limitedRows(), meta, manifest);

        return new Outcome(paged, false);
    }

    private static MaterializedPage materializeAndPage(
            JoinExecutor joinExecutor,
            UserContext user,
            Map<String, Connector> connectorsByName,
            JoinQuery jq) {
        List<EngineRow> combined = joinExecutor.materialize(user, connectorsByName, jq);
        MaterializedRowSet rowSet = MaterializedRowSet.limitedFrom(combined, jq.limit());
        return OffsetCursorPager.page(rowSet, jq.cursor(), jq.pageSize());
    }
}
