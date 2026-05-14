package org.emathp.ratelimit;

/**
 * Seam between the engine and rate-limit enforcement. Engine code holds a {@link RateLimitPolicy};
 * the runtime injects {@link HierarchicalRateLimiter} and tests inject {@link #UNLIMITED}.
 *
 * <p>Implementations return {@link RateLimitResult} rather than throwing so callers can decide the
 * propagation strategy (engine page loop wraps denials in {@link RateLimitedException}).
 */
public interface RateLimitPolicy {

    RateLimitResult tryAcquire(RequestContext ctx);

    /**
     * No-op policy: every request is allowed. Use for snapshot / pagination / web tests that don't
     * exercise rate limiting, and for codepaths that have already gated callers separately.
     */
    RateLimitPolicy UNLIMITED = ctx -> RateLimitResult.ok();
}
