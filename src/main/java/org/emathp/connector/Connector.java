package org.emathp.connector;

import java.time.Duration;
import org.emathp.auth.UserContext;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;

public interface Connector {

    String source();

    CapabilitySet capabilities();

    /**
     * Rows to request per underlying API page when the planner pushes pagination. Independent of
     * UI/federated {@link org.emathp.model.Query#pageSize()} — that value only signals that
     * paging is in play; the connector batch size follows this method.
     */
    default int defaultFetchPageSize() {
        return 20;
    }

    /**
     * Hard upper bound on how long this connector's cached data is reusable. Different sources
     * have different intrinsic invalidation semantics — a search-cursor's expiry, a webhook's
     * delivery window, a documents API's typical change rate — so each connector advertises its
     * own ceiling rather than relying on a single global value.
     *
     * <p>Both this and the client's {@code maxStaleness} are upper bounds at the snapshot layer.
     * The effective TTL is their {@code min}: clients can request <em>tighter</em> freshness than
     * the connector advertises (just becomes a cache miss), but cannot request <em>looser</em>
     * (the source's validity contract is non-negotiable). When both are absent,
     * {@link org.emathp.config.WebDefaults#snapshotChunkFreshness()} applies as the system-wide
     * floor.
     */
    default Duration maxFreshnessTtl() {
        return Duration.ofMinutes(5);
    }

    /**
     * How this connector's returned data varies with caller identity. Drives snapshot cache
     * keying — see {@link DataScope}. Default {@link DataScope#USER} (conservative: each user
     * isolated).
     *
     * <p>Today only {@link #isUserScopedData()} is consumed by the snapshot pipeline; the full
     * enum is declared as the production architecture target. Connectors that genuinely share
     * data across users should override (e.g. Salesforce / Jira / HRIS-style → {@code
     * TENANT_ROLE}; the demo fixtures → {@code TENANT_ROLE}).
     */
    default DataScope dataScope() {
        return DataScope.USER;
    }

    /**
     * Binary view of {@link #dataScope()} used by snapshot keying today. When {@code true}, the
     * snapshot path includes {@code userId}; when {@code false}, it does not, and users sharing
     * the same {@code (tenant, role)} share the cache.
     *
     * <p>Real OAuth connectors must return {@code true} — sharing across users would cross-serve
     * private data.
     */
    default boolean isUserScopedData() {
        return dataScope() == DataScope.USER;
    }

    SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query);
}
