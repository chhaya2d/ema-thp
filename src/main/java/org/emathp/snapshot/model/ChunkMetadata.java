package org.emathp.snapshot.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Per-chunk continuation + freshness. The chunk with the greatest {@code endRow} in a connector
 * subdirectory is the authoritative continuation anchor for that provider stream.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ChunkMetadata(
        int startRow,
        int endRow,
        String createdAt,
        String freshnessUntil,
        String nextCursor,
        boolean exhausted,
        int providerFetchSize) {}
