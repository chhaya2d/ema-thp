package org.emathp.ratelimit;

import java.util.Objects;
import org.emathp.auth.UserContext;
import org.emathp.authz.Principal;
import org.emathp.connector.Connector;

/**
 * Identity slice for hierarchical limiting. All three fields participate in keying different
 * buckets; a request must satisfy connector, tenant, and user limits.
 */
public record RequestContext(String tenantId, String userId, String connectorName) {

    public RequestContext {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(connectorName, "connectorName");
        if (tenantId.isBlank() || userId.isBlank() || connectorName.isBlank()) {
            throw new IllegalArgumentException("tenantId, userId, and connectorName must be non-blank");
        }
    }

    /**
     * Builds a context from the engine's per-request types. Callers should short-circuit (not
     * build a context at all) when the caller is anonymous — see {@link
     * org.emathp.engine.QueryExecutor}'s page loop.
     */
    public static RequestContext of(Principal principal, UserContext user, Connector connector) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(connector, "connector");
        return new RequestContext(principal.tenantId(), user.userId(), connector.source());
    }
}
