package org.emathp.planner;

import java.util.List;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.ResidualOps;

/**
 * Result of decomposing a {@link org.emathp.model.Query} against a connector's capabilities.
 *
 * <p>{@code pushedQuery} is what the connector will be invoked with; {@code pendingOperations}
 * lists human-readable labels of clauses left for residual execution; {@code residualOps} is
 * the structured form of those same clauses, ready to feed to
 * {@link org.emathp.engine.QueryExecutor}.
 *
 * @apiNote {@code pendingOperations} and {@code residualOps} are two views of the same fact
 *          (modulo PROJECTION + LIMIT, which appear in {@code pendingOperations} only — the
 *          former is deferred from {@code residualOps}, the latter has its own engine path).
 *          Display callers can use {@code pendingOperations}; execution callers should use
 *          {@code residualOps}.
 */
public record PushdownPlan(
        ConnectorQuery pushedQuery,
        List<String> pendingOperations,
        ResidualOps residualOps,
        /**
         * When true, snapshot IO (read-through cache + chunk writes) may run for this plan. When false,
         * the engine still evaluates pushdown/residual the same way, but skips disk. Controlled by
         * {@code EMA_PUSHDOWN_SNAPSHOT_RUN} / {@link org.emathp.config.WebDefaults#persistSnapshotMaterialization()}.
         */
        boolean persistSnapshotMaterialization) {

    public PushdownPlan {
        pendingOperations = pendingOperations == null ? List.of() : List.copyOf(pendingOperations);
    }
}
