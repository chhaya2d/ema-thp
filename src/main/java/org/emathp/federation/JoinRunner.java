package org.emathp.federation;

import java.util.List;
import java.util.Map;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.query.RequestContext;

/**
 * Executes a two-source join in memory. Full-disk materialisation for joins is handled by {@link
 * org.emathp.snapshot.pipeline.FullMaterializationCoordinator} — not here.
 */
public final class JoinRunner {

    private JoinRunner() {}

    public static MaterializedPage run(
            JoinExecutor joinExecutor,
            RequestContext ctx,
            Map<String, Connector> connectorsByName,
            JoinQuery jq) {
        List<EngineRow> combined = joinExecutor.materialize(ctx, connectorsByName, jq);
        MaterializedRowSet rowSet = MaterializedRowSet.limitedFrom(combined, jq.limit());
        return OffsetCursorPager.page(rowSet, jq.cursor(), jq.pageSize());
    }
}
