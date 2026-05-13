package org.emathp.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.emathp.cache.QueryCacheScope;
import org.emathp.engine.policy.TagAccessPolicy;

/**
 * Demo-only principal metadata: tenant, roles, primary role, and per-role allowed document tags.
 * Used when only {@code userId} is selected in the browser playground.
 */
public final class DemoPrincipalRegistry {

    private static final Map<String, DemoPrincipal> BY_USER = new LinkedHashMap<>();

    static {
        BY_USER.put(
                "alice",
                new DemoPrincipal(
                        "tenant-1",
                        "hr",
                        List.of("hr"),
                        Map.of("hr", Set.of("hr", "engineering"))));
        BY_USER.put(
                "bob",
                new DemoPrincipal(
                        "tenant-1",
                        "engineering",
                        List.of("engineering"),
                        Map.of("engineering", Set.of("engineering"))));
    }

    private DemoPrincipalRegistry() {}

    public static DemoPrincipal principal(String userId) {
        if (userId == null || userId.isBlank()) {
            return anonymous();
        }
        DemoPrincipal p = BY_USER.get(userId.trim().toLowerCase(Locale.ROOT));
        return p != null ? p : anonymous();
    }

    public static QueryCacheScope cacheScope(String userId) {
        DemoPrincipal p = principal(userId);
        String uid = userId == null || userId.isBlank() ? "_" : userId.trim();
        return QueryCacheScope.forDemo(uid, p.tenantId(), p.primaryRole());
    }

    /** Tag filter derived from the user's roles + {@link QueryCacheScope#roleSlug()} (primary role). */
    public static TagAccessPolicy tagPolicyFor(QueryCacheScope scope) {
        DemoPrincipal p = principal(scope.userId());
        return TagAccessPolicy.forAllowedTags(p.allowedTagsForRole(scope.roleSlug()));
    }

    private static DemoPrincipal anonymous() {
        return new DemoPrincipal("_anon", "guest", List.of("guest"), Map.of("guest", Set.of()));
    }

    public record DemoPrincipal(
            String tenantId,
            String primaryRole,
            List<String> roles,
            Map<String, Set<String>> allowedTagsByRole) {

        public Set<String> allowedTagsForRole(String roleSlug) {
            return allowedTagsByRole.getOrDefault(roleSlug, Set.of());
        }

        public Set<String> allowedTagsForPrimaryRole() {
            return allowedTagsForRole(primaryRole);
        }
    }
}
