package org.emathp.query;

import java.time.Duration;

/**
 * Query-specific payload — what data to ask for and how to slice it. Identity / session metadata
 * (user, scope, tenantId, traceId) lives in the orthogonal {@link RequestContext}.
 *
 * <p>Rule for adding fields here: would this value change on "next page" of the same logical
 * query? If yes → here. If no (same on retry / next page) → {@link RequestContext}.
 *
 * @param sql                  SELECT subset; non-blank
 * @param pageNumber           1-based logical page, or {@code null} when using {@code
 *                             logicalCursorOffset}
 * @param logicalCursorOffset  row-offset string (e.g. {@code "6"}), alternative to pageNumber
 * @param requestPageSize      UI page-size override, or {@code null} for server default
 * @param maxStaleness         per-call freshness hint (ISO duration), or {@code null} for default
 *                             chunk TTL
 */
public record FederatedQueryRequest(
        String sql,
        Integer pageNumber,
        String logicalCursorOffset,
        Integer requestPageSize,
        Duration maxStaleness) {

    public FederatedQueryRequest {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be blank");
        }
    }
}
