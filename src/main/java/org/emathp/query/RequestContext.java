package org.emathp.query;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.emathp.auth.UserContext;
import org.emathp.cache.QueryCacheScope;

/**
 * Cross-cutting per-request facts. Stable across pagination / continuation of the same logical
 * query; carries identity (user, scope, tenant) and observability (traceId, startedAt) for every
 * downstream layer that needs to log, audit, or branch on the caller.
 *
 * <p>What belongs here vs in {@link FederatedQueryRequest}: anything that would be the same on the
 * "next page" of the same logical query — that's context. Anything that varies per page (sql,
 * pagination, page size, maxStaleness) — that's request.
 *
 * @param traceId    UUID per request, echoed in responses and structured log lines
 * @param user       caller identity for connector calls; {@code UserContext.anonymous()} when
 *                   unauthenticated
 * @param scope      tenant/role/user namespace for snapshot paths and cache keys
 * @param tenantId   tenantId used for rate-limit bucket keys; {@code null}/blank ⇒ anon, skip the
 *                   rate-limit check (the page loop honors this)
 * @param startedAt  when the request was admitted at the boundary; used to compute elapsed times
 */
public record RequestContext(
        String traceId,
        UserContext user,
        QueryCacheScope scope,
        String tenantId,
        Instant startedAt) {

    public RequestContext {
        Objects.requireNonNull(traceId, "traceId");
        if (traceId.isBlank()) {
            throw new IllegalArgumentException("traceId must not be blank");
        }
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(startedAt, "startedAt");
    }

    /** True when the rate-limit check should be skipped (no tenant or no userId). */
    public boolean isAnonymous() {
        return tenantId == null
                || tenantId.isBlank()
                || user.userId() == null
                || user.userId().isBlank();
    }

    /**
     * Convenience factory for CLI / test paths. Generates a fresh traceId and uses {@code
     * Instant.now()} as start time; tenantId is taken from the scope unless the user is anonymous.
     */
    public static RequestContext forCli(UserContext user, QueryCacheScope scope) {
        String tenant =
                (user != null && user.userId() != null && !user.userId().isBlank())
                        ? scope.tenantId()
                        : null;
        return new RequestContext(UUID.randomUUID().toString(), user, scope, tenant, Instant.now());
    }

    /**
     * Convenience for engine-level tests that don't need a real principal — synthesizes an
     * anonymous scope from {@code user}. Rate limiting is bypassed (no tenant).
     */
    public static RequestContext forEngine(UserContext user) {
        return forCli(user, QueryCacheScope.from(user));
    }
}
