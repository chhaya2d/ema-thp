package org.emathp.ratelimit;

import java.util.Objects;

/**
 * Per-scope {@link TokenBucketConfig}. Scopes are evaluated independently: a request must pass
 * connector, tenant, and user buckets (AND semantics).
 */
public record HierarchicalRateLimiterConfig(
        TokenBucketConfig connector, TokenBucketConfig tenant, TokenBucketConfig user) {

    public HierarchicalRateLimiterConfig {
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(user, "user");
    }
}
