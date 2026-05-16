package org.emathp.snapshot.pipeline;

import java.time.Duration;
import java.time.Instant;
import org.emathp.snapshot.model.ChunkMetadata;

/** Rules for whether on-disk chunk metadata still satisfies a client freshness requirement. */
public final class FreshnessPolicy {

    private FreshnessPolicy() {}

    /**
     * Fresh if {@code now} is strictly before the applicable expiry instant.
     *
     * <p>Two upper bounds apply: (1) the client's {@code requestMaxStaleness} — how old this
     * specific caller will tolerate — and (2) the chunk's stamped {@code freshnessUntil} — the
     * ceiling the connector advertised when the chunk was written. A chunk is fresh only if
     * {@code now} is before <em>both</em>. The connector ceiling is non-negotiable (a client
     * cannot opt to read past it), matching the cache-control pattern where origin's max-age
     * bounds any downstream cache's reuse.
     */
    public static boolean isFresh(ChunkMetadata meta, Duration requestMaxStaleness, Instant now) {
        Instant created = Instant.parse(meta.createdAt());
        Instant stampedLimit = Instant.parse(meta.freshnessUntil());
        Instant queryLimit =
                requestMaxStaleness != null ? created.plus(requestMaxStaleness) : stampedLimit;
        Instant effectiveLimit = queryLimit.isBefore(stampedLimit) ? queryLimit : stampedLimit;
        return now.isBefore(effectiveLimit);
    }
}
