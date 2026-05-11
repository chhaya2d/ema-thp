package org.emathp.snapshot.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.emathp.snapshot.model.ChunkMetadata;
import org.junit.jupiter.api.Test;

class FreshnessPolicyTest {

    @Test
    void requestMaxStaleness_measuredFromCreatedAt() {
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        ChunkMetadata m =
                new ChunkMetadata(
                        0,
                        5,
                        created.toString(),
                        "2099-01-01T00:00:00Z",
                        null,
                        true,
                        6);
        assertTrue(FreshnessPolicy.isFresh(m, Duration.ofHours(1), created));
        assertFalse(FreshnessPolicy.isFresh(m, Duration.ofHours(1), created.plus(Duration.ofHours(2))));
    }

    @Test
    void nullMaxStaleness_usesDiskFreshnessUntil() {
        Instant now = Instant.parse("2026-06-01T12:00:00Z");
        ChunkMetadata m =
                new ChunkMetadata(
                        0,
                        5,
                        "2026-01-01T00:00:00Z",
                        "2026-06-02T00:00:00Z",
                        null,
                        true,
                        6);
        assertTrue(FreshnessPolicy.isFresh(m, null, now));
        assertFalse(FreshnessPolicy.isFresh(m, null, Instant.parse("2026-06-03T00:00:00Z")));
    }
}
