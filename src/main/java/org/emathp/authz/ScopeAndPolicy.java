package org.emathp.authz;

import org.emathp.cache.QueryCacheScope;

/**
 * Derives {@link QueryCacheScope} and {@link TagAccessPolicy} from a {@link Principal}.
 *
 * <p>Single place to keep the two derivations consistent: the cache scope (used for snapshot path
 * isolation) and the tag policy (used for the engine's role-based row filter) share the principal's
 * primary role. A future production {@link PrincipalRegistry} can return a different shape of
 * {@code Principal} without forcing callers to repeat this glue.
 */
public final class ScopeAndPolicy {

    private ScopeAndPolicy() {}

    public static QueryCacheScope cacheScope(String userId, Principal principal) {
        String uid = userId == null || userId.isBlank() ? "_" : userId.trim();
        return QueryCacheScope.forDemo(uid, principal.tenantId(), principal.primaryRole());
    }

    public static TagAccessPolicy tagPolicy(Principal principal, String roleSlug) {
        return TagAccessPolicy.forAllowedTags(principal.allowedTagsForRole(roleSlug));
    }
}
