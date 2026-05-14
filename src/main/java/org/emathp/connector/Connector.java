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

    SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query);
}
