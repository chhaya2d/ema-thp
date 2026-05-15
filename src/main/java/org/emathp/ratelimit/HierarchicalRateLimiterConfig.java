package org.emathp.ratelimit;

/**
 * Per-scope {@link TokenBucketConfig}. Scopes are evaluated independently: a request must pass
 * every configured bucket (AND semantics across non-null scopes).
 *
 * <p>Any scope may be {@code null} — the limiter skips it. This supports the two-layer model:
 *
 * <ul>
 *   <li><b>Service limiter</b> at the HTTP / service entry — debits {@code tenant} + {@code user}
 *       per request, including cache hits. Honors Ema's own SLO and per-user fairness. Configured
 *       with {@code connector = null}.</li>
 *   <li><b>Connector limiter</b> at the engine page-loop boundary — debits {@code connector} per
 *       outbound provider call. Honors upstream APIs' published rate limits. Configured with
 *       {@code tenant = null} and {@code user = null}.</li>
 * </ul>
 *
 * <p>At least one scope must be non-null; otherwise the limiter does nothing.
 */
public record HierarchicalRateLimiterConfig(
        TokenBucketConfig connector, TokenBucketConfig tenant, TokenBucketConfig user) {

    public HierarchicalRateLimiterConfig {
        if (connector == null && tenant == null && user == null) {
            throw new IllegalArgumentException(
                    "at least one of connector/tenant/user must be configured");
        }
    }

    /** Convenience: service-layer config (tenant + user; connector skipped). */
    public static HierarchicalRateLimiterConfig forService(
            TokenBucketConfig tenant, TokenBucketConfig user) {
        return new HierarchicalRateLimiterConfig(null, tenant, user);
    }

    /** Convenience: connector-layer config (connector only; tenant + user skipped). */
    public static HierarchicalRateLimiterConfig forConnector(TokenBucketConfig connector) {
        return new HierarchicalRateLimiterConfig(connector, null, null);
    }
}
