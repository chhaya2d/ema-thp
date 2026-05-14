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
     * How long this connector considers its own data fresh — the snapshot TTL used when the
     * client does not pass {@code maxStaleness}. Different sources have different intrinsic
     * change rates (a documents API ≠ a real-time messaging API), so each connector should
     * advertise its own default rather than relying on a single global value.
     *
     * <p>Resolution order at the snapshot layer: client's {@code maxStaleness} → this method →
     * {@link org.emathp.config.WebDefaults#snapshotChunkFreshness()} as the system-wide floor.
     */
    default Duration defaultFreshnessTtl() {
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
