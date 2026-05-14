package org.emathp.query;

/**
 * Domain-level error vocabulary, transport-agnostic. CLI and tests can branch on these codes
 * without depending on {@code org.emathp.web}; the HTTP layer maps each to a status code via
 * {@link #httpStatus()}.
 *
 * <p>Mapped to PDF reference vocabulary: {@code RATE_LIMIT_EXHAUSTED}, {@code STALE_DATA},
 * {@code ENTITLEMENT_DENIED}, {@code SOURCE_TIMEOUT}.
 */
public enum ErrorCode {

    /** Hierarchical rate limiter denied the request (any of connector/tenant/user bucket). */
    RATE_LIMIT_EXHAUSTED(429),

    /** Tag policy or future RLS/CLS rule denied the row/column. */
    ENTITLEMENT_DENIED(403),

    /** Snapshot freshness contract could not be satisfied within {@code maxStaleness}. */
    STALE_DATA(409),

    /** Upstream provider timed out. */
    SOURCE_TIMEOUT(504),

    /** Upstream provider returned its own 429. */
    SOURCE_RATE_LIMITED(503),

    /** Upstream provider unreachable or 5xx. */
    SOURCE_UNAVAILABLE(503),

    /** Client-side problem: parser, invalid pagination, unknown connector mode, etc. */
    BAD_QUERY(400),

    /** Catch-all for unexpected exceptions. */
    INTERNAL(500);

    private final int httpStatus;

    ErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}
