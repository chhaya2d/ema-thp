package org.emathp.auth;

/**
 * Caller identity for connector-scoped work (e.g. loading OAuth tokens per user). The planner and
 * query model stay authentication-agnostic; only {@link org.emathp.connector.Connector}
 * implementations that need credentials observe this type.
 *
 * @param userId stable id in the host application, or {@code null} for anonymous / mock-only
 *               execution paths
 */
public record UserContext(String userId) {

    public static UserContext anonymous() {
        return new UserContext(null);
    }
}
