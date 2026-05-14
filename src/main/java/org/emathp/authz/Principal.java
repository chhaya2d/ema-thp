package org.emathp.authz;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Authorization view of a caller: tenant, roles, and per-role tag visibility.
 *
 * <p>Produced by a {@link PrincipalRegistry} from a {@link org.emathp.auth.UserContext}. The query
 * pipeline derives {@link org.emathp.cache.QueryCacheScope} and {@link TagAccessPolicy} from a
 * {@code Principal} (see {@link ScopeAndPolicy}); engine/snapshot code never observes this type
 * directly.
 *
 * @param tenantId logical tenant for snapshot isolation; {@code "_anon"} for unknown callers
 * @param primaryRole role used for cache scoping and the default tag filter
 * @param roles all roles the caller holds (informational; not consumed by the engine today)
 * @param allowedTagsByRole per-role allowed tag set; an empty / missing entry means "no
 *                          tag-based restriction" via {@link TagAccessPolicy#unrestricted()}
 */
public record Principal(
        String tenantId,
        String primaryRole,
        List<String> roles,
        Map<String, Set<String>> allowedTagsByRole) {

    public Principal {
        roles = roles == null ? List.of() : List.copyOf(roles);
        allowedTagsByRole = allowedTagsByRole == null ? Map.of() : Map.copyOf(allowedTagsByRole);
    }

    public Set<String> allowedTagsForRole(String roleSlug) {
        return allowedTagsByRole.getOrDefault(roleSlug, Set.of());
    }

    public Set<String> allowedTagsForPrimaryRole() {
        return allowedTagsForRole(primaryRole);
    }

    public static Principal anonymous() {
        return new Principal("_anon", "guest", List.of("guest"), Map.of("guest", Set.of()));
    }
}
