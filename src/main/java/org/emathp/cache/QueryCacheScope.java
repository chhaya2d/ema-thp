package org.emathp.cache;

import org.emathp.auth.UserContext;

/**
 * Identity slice for snapshot paths and cache keys so results never leak across tenants / roles.
 *
 * <p>Filesystem layout uses {@link #snapshotScopeDirectoryName()} under each snapshot environment.
 *
 * @param userId stable principal id for connector calls (mock corpus / OAuth identity), or {@code
 *     _} when absent
 * @param tenantId logical tenant for isolation
 * @param roleSlug active role for this request (demo uses primary role from {@link
 *     org.emathp.web.DemoPrincipalRegistry})
 * @param keySchemaVersion bump when serialization of snapshot identity changes
 */
public record QueryCacheScope(String userId, String tenantId, String roleSlug, int keySchemaVersion) {

    public static final int CURRENT_KEY_SCHEMA = 2;

    /**
     * Anonymous / unspecified tenant snapshot lane — suitable only for single-user demos where no
     * tenant metadata exists.
     */
    public static QueryCacheScope from(UserContext ctx) {
        String uid = ctx.userId();
        if (uid == null || uid.isBlank()) {
            return new QueryCacheScope("_", "_anon", "guest", CURRENT_KEY_SCHEMA);
        }
        return new QueryCacheScope(uid, "_default", "_guest", CURRENT_KEY_SCHEMA);
    }

    /** Demo playground tenants / roles (see {@link org.emathp.web.DemoPrincipalRegistry}). */
    public static QueryCacheScope forDemo(String userId, String tenantId, String roleSlug) {
        String uid = userId == null || userId.isBlank() ? "_" : userId.trim();
        String t = tenantId == null || tenantId.isBlank() ? "_" : tenantId.trim();
        String r = roleSlug == null || roleSlug.isBlank() ? "guest" : roleSlug.trim();
        return new QueryCacheScope(uid, t, r, CURRENT_KEY_SCHEMA);
    }

    String keySegment() {
        return "u:"
                + (userId == null ? "_" : userId)
                + "|t:"
                + (tenantId == null ? "_" : tenantId)
                + "|r:"
                + (roleSlug == null ? "_" : roleSlug)
                + "|ksv:"
                + keySchemaVersion;
    }

    /**
     * Single path segment for snapshot roots (tenant + role + user + schema version). Snapshot rows
     * are stored <strong>after</strong> role tag filtering so different roles do not share cached row
     * bytes.
     */
    public String snapshotScopeDirectoryName() {
        return "t_"
                + safeSegment(tenantId)
                + "_r_"
                + safeSegment(roleSlug)
                + "_u_"
                + safeSegment(userId)
                + "_ksv"
                + keySchemaVersion;
    }

    private static String safeSegment(String s) {
        if (s == null || s.isBlank()) {
            return "_";
        }
        return s.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
