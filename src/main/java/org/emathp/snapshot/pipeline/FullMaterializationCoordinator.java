package org.emathp.snapshot.pipeline;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.policy.TagRowFilter;
import org.emathp.federation.MaterializedPage;
import org.emathp.federation.MaterializedRowSet;
import org.emathp.federation.OffsetCursorPager;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.query.RequestContext;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.ports.Clock;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Runs engine-composed queries with the same snapshot semantics as residual single-source work:
 * always fully materialise when persistence is enabled (see {@link
 * org.emathp.snapshot.policy.SnapshotMaterializationPolicy#requiresFullMaterialization}).
 */
public final class FullMaterializationCoordinator {

    private FullMaterializationCoordinator() {}

    /**
     * @param freshnessMs age of the materialized result in milliseconds (now − chunk.createdAt).
     *                   {@code 0L} when a fresh materialization just ran; positive when served
     *                   from disk cache.
     */
    public record Outcome(MaterializedPage paged, boolean reusedFromDisk, long freshnessMs) {}

    public static Outcome run(
            RequestContext ctx,
            JoinQuery jq,
            boolean persistenceEnabled,
            Path queryRoot,
            Duration maxStaleness,
            JoinExecutor joinExecutor,
            Map<String, Connector> connectorsByName,
            SnapshotStore store,
            Clock clock,
            TagAccessPolicy tagPolicy)
            throws IOException {

        if (!persistenceEnabled) {
            return new Outcome(
                    materializeAndPage(ctx, joinExecutor, connectorsByName, jq, tagPolicy), false, 0L);
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
            long ageMs = Math.max(0L, now.toEpochMilli() - Instant.parse(h.createdAt()).toEpochMilli());
            return new Outcome(paged, true, ageMs);
        }

        List<EngineRow> combined = joinExecutor.materialize(ctx, connectorsByName, jq);
        combined = applyTagPolicy(combined, tagPolicy);
        MaterializedRowSet rowSet = MaterializedRowSet.limitedFrom(combined, jq.limit());
        MaterializedPage paged = OffsetCursorPager.page(rowSet, jq.cursor(), jq.pageSize());

        Duration ttl = resolveJoinTtl(maxStaleness, jq, connectorsByName);
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

        return new Outcome(paged, false, 0L);
    }

    private static MaterializedPage materializeAndPage(
            RequestContext ctx,
            JoinExecutor joinExecutor,
            Map<String, Connector> connectorsByName,
            JoinQuery jq,
            TagAccessPolicy tagPolicy) {
        List<EngineRow> combined = joinExecutor.materialize(ctx, connectorsByName, jq);
        combined = applyTagPolicy(combined, tagPolicy);
        MaterializedRowSet rowSet = MaterializedRowSet.limitedFrom(combined, jq.limit());
        return OffsetCursorPager.page(rowSet, jq.cursor(), jq.pageSize());
    }

    private static List<EngineRow> applyTagPolicy(List<EngineRow> rows, TagAccessPolicy tagPolicy) {
        if (tagPolicy == null || tagPolicy.allowedTags().isEmpty()) {
            return rows;
        }
        return TagRowFilter.apply(rows, tagPolicy);
    }

    /**
     * Resolution order for join TTL: client {@code maxStaleness} wins; else the <strong>min</strong>
     * of the two side connectors' {@link
     * org.emathp.connector.Connector#defaultFreshnessTtl()} — the materialized result is only as
     * fresh as its tightest side; else the system-wide floor.
     */
    private static Duration resolveJoinTtl(
            Duration clientMaxStaleness, JoinQuery jq, Map<String, Connector> connectorsByName) {
        if (clientMaxStaleness != null) {
            return clientMaxStaleness;
        }
        Connector leftConn = connectorsByName.get(jq.left().connectorName());
        Connector rightConn = connectorsByName.get(jq.right().connectorName());
        Duration left = leftConn != null ? leftConn.defaultFreshnessTtl() : null;
        Duration right = rightConn != null ? rightConn.defaultFreshnessTtl() : null;
        if (left != null && right != null) {
            return left.compareTo(right) <= 0 ? left : right;
        }
        if (left != null) return left;
        if (right != null) return right;
        return WebDefaults.snapshotChunkFreshness();
    }
}
