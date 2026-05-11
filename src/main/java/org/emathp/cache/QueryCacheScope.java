package org.emathp.cache;

import org.emathp.auth.UserContext;

/**
 * Identity slice for snapshot paths and future cache keys so results never leak across callers.
 *
 * <p><b>Tradeoffs</b>
 *
 * <ul>
 *   <li><b>{@code userId} only (v1)</b> — Simple and safe: same SQL for two users never shares an
 *       entry. Lower hit rate when many users have identical connector data (rare in OAuth-backed
 *       flows but common in mocks).</li>
 *   <li><b>Future: tenant / account-set scope</b> — If you can prove two users resolve to the same
 *       backing accounts for every connector in the query, you may widen scope to a {@code
 *       dataScopeId} and share entries — higher hit rate, but the scope model must track token /
 *       ACL changes and invalidate or version keys when access changes.</li>
 *   <li><b>Anonymous ({@code userId == null})</b> — All anonymous callers share one lane; fine for
 *       mock demos, wrong for multi-tenant anonymous APIs.</li>
 * </ul>
 *
 * @param userId stable principal id, or {@code null} for anonymous
 * @param keySchemaVersion bump when serialization of {@link ParsedQueryNormalizer} changes to
 *                         avoid reading structurally incompatible cache entries
 */
public record QueryCacheScope(String userId, int keySchemaVersion) {

    public static final int CURRENT_KEY_SCHEMA = 1;

    public static QueryCacheScope from(UserContext ctx) {
        return new QueryCacheScope(ctx.userId(), CURRENT_KEY_SCHEMA);
    }

    String keySegment() {
        return "u:" + (userId == null ? "_" : userId) + "|ksv:" + keySchemaVersion;
    }
}
