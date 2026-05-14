package org.emathp.ratelimit;

import java.util.Objects;

/**
 * Stable map key for {@link java.util.concurrent.ConcurrentHashMap}. Equality is by scope + logical
 * dimension string (connector name, tenant id, or tenant-scoped user id).
 */
public record RateLimitKey(RateLimitScope scope, String dimension) {

    public RateLimitKey {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(dimension, "dimension");
        if (dimension.isBlank()) {
            throw new IllegalArgumentException("dimension must not be blank");
        }
    }

    static RateLimitKey connector(String connectorName) {
        return new RateLimitKey(RateLimitScope.CONNECTOR, connectorName);
    }

    static RateLimitKey tenant(String tenantId) {
        return new RateLimitKey(RateLimitScope.TENANT, tenantId);
    }

    /** User buckets are namespaced by tenant so the same user id cannot bleed across tenants. */
    static RateLimitKey user(String tenantId, String userId) {
        return new RateLimitKey(RateLimitScope.USER, tenantId + "|" + userId);
    }
}
