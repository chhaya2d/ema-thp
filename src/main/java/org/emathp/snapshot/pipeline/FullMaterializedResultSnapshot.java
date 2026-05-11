package org.emathp.snapshot.pipeline;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.emathp.model.EngineRow;
import org.emathp.snapshot.layout.ChunkNaming;
import org.emathp.snapshot.layout.SnapshotPaths;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.ports.Clock;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Single-directory snapshot for engine-composed results (see {@link
 * org.emathp.snapshot.policy.SnapshotMaterializationPolicy#requiresFullMaterialization}). Today that
 * is join only. No per-connector trees under the same {@code queryRoot}.
 */
public final class FullMaterializedResultSnapshot {

    private FullMaterializedResultSnapshot() {}

    public static Path materializedDir(Path queryRoot) {
        return queryRoot.resolve(SnapshotPaths.MATERIALIZED_QUERY_SEGMENT);
    }

    public record CacheHit(List<EngineRow> cappedRows, int upstreamRowCount, boolean stoppedAtLimit) {}

    public static Optional<CacheHit> tryHit(
            Path queryRoot, Duration maxStaleness, SnapshotStore store, Clock clock) throws IOException {

        Path dir = materializedDir(queryRoot);
        if (!store.exists(dir)) {
            return Optional.empty();
        }
        Instant now = clock.now();
        Optional<ChunkMetadata> meta = store.latestChunkMeta(dir);
        if (meta.isEmpty() || !FreshnessPolicy.isFresh(meta.get(), maxStaleness, now)) {
            return Optional.empty();
        }
        String mf = store.readMaterializedManifest(dir);
        if (mf == null || mf.isBlank()) {
            return Optional.empty();
        }
        JsonObject parsed = JsonParser.parseString(mf).getAsJsonObject();
        int upstreamRowCount =
                parsed.has("upstreamRowCount")
                        ? parsed.get("upstreamRowCount").getAsInt()
                        : parsed.get("totalJoinedRows").getAsInt();
        boolean stoppedAtLimit =
                parsed.has("stoppedAtLimit") && parsed.get("stoppedAtLimit").getAsBoolean();

        ChunkMetadata m = meta.get();
        String prefix = ChunkNaming.prefix(m.startRow(), m.endRow());
        List<EngineRow> rows = store.readMaterializedRows(dir, prefix);
        return Optional.of(new CacheHit(rows, upstreamRowCount, stoppedAtLimit));
    }

    public static void writeIfNonEmpty(
            Path queryRoot,
            SnapshotStore store,
            List<EngineRow> cappedRows,
            ChunkMetadata meta,
            JsonObject manifest)
            throws IOException {
        if (cappedRows.isEmpty()) {
            return;
        }
        Path dir = materializedDir(queryRoot);
        if (store.exists(dir)) {
            store.deleteRecursively(dir);
        }
        store.writeMaterializedSnapshot(dir, cappedRows, meta, manifest.toString());
    }
}
