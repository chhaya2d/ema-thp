package org.emathp.ratelimit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hierarchical rate limiter: each request must pass <strong>connector</strong>, <strong>tenant</strong>,
 * and <strong>user</strong> {@link TokenBucket}s (AND semantics).
 *
 * <p>Storage is {@link ConcurrentHashMap}; each {@link TokenBucket} uses {@code synchronized} for
 * lazy refill + debit. Failed partial acquires {@linkplain TokenBucket#refundOne() refund} earlier
 * buckets so a rejected request does not consume upstream quota (see {@link TokenBucket} on
 * refund tradeoffs).
 *
 * <p><strong>Single-node prototype:</strong> no cross-process coordination; keys are not replicated.
 */
public final class HierarchicalRateLimiter implements RateLimitPolicy {

    private final ConcurrentHashMap<RateLimitKey, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final TimeSource time;
    private final HierarchicalRateLimiterConfig config;

    public HierarchicalRateLimiter(HierarchicalRateLimiterConfig config) {
        this(TimeSource.system(), config);
    }

    public HierarchicalRateLimiter(TimeSource time, HierarchicalRateLimiterConfig config) {
        this.time = Objects.requireNonNull(time, "time");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Acquires one token from each configured scope; scopes with {@code null} config are skipped
     * (supports the two-layer model — see {@link HierarchicalRateLimiterConfig}).
     *
     * @return full {@link RateLimitResult}; use {@link #allow(RequestContext)} when only a
     *     boolean is needed.
     */
    @Override
    public RateLimitResult tryAcquire(RequestContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        List<TokenBucket> committed = new ArrayList<>(3);

        if (config.connector() != null) {
            TokenBucket connector =
                    bucket(RateLimitKey.connector(ctx.connectorName()), config.connector());
            TokenBucket.TryOutcome c = connector.tryConsumeOne();
            if (!c.allowed()) {
                return RateLimitResult.denied(c.retryAfterMs(), RateLimitScope.CONNECTOR);
            }
            committed.add(connector);
        }

        if (config.tenant() != null) {
            TokenBucket tenant = bucket(RateLimitKey.tenant(ctx.tenantId()), config.tenant());
            TokenBucket.TryOutcome t = tenant.tryConsumeOne();
            if (!t.allowed()) {
                rollback(committed);
                return RateLimitResult.denied(t.retryAfterMs(), RateLimitScope.TENANT);
            }
            committed.add(tenant);
        }

        if (config.user() != null) {
            TokenBucket user =
                    bucket(RateLimitKey.user(ctx.tenantId(), ctx.userId()), config.user());
            TokenBucket.TryOutcome u = user.tryConsumeOne();
            if (!u.allowed()) {
                rollback(committed);
                return RateLimitResult.denied(u.retryAfterMs(), RateLimitScope.USER);
            }
        }
        return RateLimitResult.ok();
    }

    /** @see #tryAcquire(RequestContext) */
    public boolean allow(RequestContext ctx) {
        return tryAcquire(ctx).allowed();
    }

    private void rollback(List<TokenBucket> committed) {
        for (int i = committed.size() - 1; i >= 0; i--) {
            committed.get(i).refundOne();
        }
    }

    private TokenBucket bucket(RateLimitKey key, TokenBucketConfig bucketConfig) {
        return buckets.computeIfAbsent(key, k -> new TokenBucket(time, bucketConfig));
    }
}
