package org.emathp.snapshot.pipeline;

import java.time.Duration;
import java.time.Instant;
import org.emathp.snapshot.model.ChunkMetadata;

/** Rules for whether on-disk chunk metadata still satisfies a client freshness requirement. */
public final class FreshnessPolicy {

    private FreshnessPolicy() {}

    /** Fresh if {@code now} is strictly before the applicable expiry instant. */
    public static boolean isFresh(ChunkMetadata meta, Duration requestMaxStaleness, Instant now) {
        Instant created = Instant.parse(meta.createdAt());
        Instant limit =
                requestMaxStaleness != null
                        ? created.plus(requestMaxStaleness)
                        : Instant.parse(meta.freshnessUntil());
        return now.isBefore(limit);
    }
}
