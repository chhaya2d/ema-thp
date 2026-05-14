package org.emathp.ratelimit;

/**
 * Outcome of {@link HierarchicalRateLimiter#tryAcquire(RequestContext)}.
 *
 * @param allowed when {@code true}, the request may proceed and {@code retryAfterMs} is {@code 0}.
 * @param retryAfterMs client-facing hint: minimum wait before a retry is likely to succeed for the
 *     {@link #violatedScope} bucket (first failure in connector → tenant → user order).
 * @param violatedScope {@code null} when {@link #allowed}; otherwise which bucket rejected the
 *     request.
 */
public record RateLimitResult(boolean allowed, long retryAfterMs, RateLimitScope violatedScope) {

    private static final RateLimitResult OK = new RateLimitResult(true, 0L, null);

    public static RateLimitResult ok() {
        return OK;
    }

    public static RateLimitResult denied(long retryAfterMs, RateLimitScope violatedScope) {
        if (retryAfterMs < 0) {
            throw new IllegalArgumentException("retryAfterMs must be non-negative");
        }
        return new RateLimitResult(false, retryAfterMs, violatedScope);
    }
}
