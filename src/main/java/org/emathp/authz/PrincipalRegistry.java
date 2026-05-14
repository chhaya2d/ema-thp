package org.emathp.authz;

import org.emathp.auth.UserContext;
import org.emathp.cache.QueryCacheScope;

/**
 * Resolves a caller's {@link Principal}. The only authz seam the rest of the system depends on:
 * inject one implementation at server boot, derive {@link QueryCacheScope} and {@link
 * TagAccessPolicy} from the resulting principal via {@link ScopeAndPolicy}, and hand those values
 * to the engine.
 *
 * <p>Implementations return {@link Principal#anonymous()} for unknown callers rather than throwing
 * — snapshot keying and tag filtering must remain well-defined for every request.
 */
public interface PrincipalRegistry {

    Principal lookup(UserContext user);

    /** Convenience: lookup + scope derivation in one step. */
    default QueryCacheScope cacheScopeFor(String userId) {
        Principal p = lookup(new UserContext(userId));
        return ScopeAndPolicy.cacheScope(userId, p);
    }

    /**
     * Registry that returns {@link Principal#anonymous()} for every caller — every request is
     * tenant {@code _anon}, role {@code guest}, no tag-based restriction. Useful for snapshot /
     * pagination tests that don't exercise authorization.
     */
    PrincipalRegistry UNRESTRICTED = user -> Principal.anonymous();
}
