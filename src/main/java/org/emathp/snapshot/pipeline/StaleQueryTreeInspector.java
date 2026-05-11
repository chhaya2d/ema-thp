package org.emathp.snapshot.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.ports.SnapshotStore;

/**
 * Uses latest chunk metadata in each subdirectory of a query tree (connector legs and/or full
 * materialisation). Stale when {@link FreshnessPolicy} rejects any child’s authoritative chunk.
 */
public final class StaleQueryTreeInspector {

    private StaleQueryTreeInspector() {}

    public static boolean isQuerySnapshotStale(
            Path queryRoot, SnapshotStore store, Instant now, Duration requestMaxStaleness)
            throws IOException {
        if (!store.exists(queryRoot)) {
            return false;
        }
        try (var stream = Files.list(queryRoot)) {
            for (Path child : stream.toList()) {
                if (!Files.isDirectory(child)) {
                    continue;
                }
                Optional<ChunkMetadata> m = store.latestChunkMeta(child);
                if (m.isPresent() && !FreshnessPolicy.isFresh(m.get(), requestMaxStaleness, now)) {
                    return true;
                }
            }
        }
        return false;
    }
}
