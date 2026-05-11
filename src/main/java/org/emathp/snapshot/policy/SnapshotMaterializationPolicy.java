package org.emathp.snapshot.policy;

import org.emathp.model.JoinQuery;
import org.emathp.model.ParsedQuery;
import org.emathp.planner.PushdownPlan;

/**
 * Central policy for snapshot persistence shape.
 *
 * <ul>
 *   <li><b>Incremental</b> (single-source connector leg, full pushdown): no residual work on that
 *       leg → no connector chunk persistence; provider runs incrementally each request.</li>
 *   <li><b>Fully materialised</b>: engine must finish semantics before the answer is stable. That is
 *       {@link org.emathp.model.ResidualOps} non-empty on a persisted connector leg, <em>or</em> any
 *       query for which {@link #requiresFullMaterialization(ParsedQuery)} is true (today:
 *       {@link JoinQuery} — treated like residual for snapshot: always one full row list under
 *       {@link org.emathp.snapshot.layout.SnapshotPaths#MATERIALIZED_QUERY_SEGMENT}).</li>
 * </ul>
 *
 * <p><b>Assumption (TODO):</b> no {@link org.emathp.connector.Connector} may accept federated
 * <em>join</em> pushdown; join composition stays engine-only. If that ever changes, revisit {@link
 * #requiresFullMaterialization} and full-materialisation layout.
 */
public final class SnapshotMaterializationPolicy {

    private SnapshotMaterializationPolicy() {}

    /**
     * Engine-composed queries always take the fully materialised snapshot path when persistence is
     * on (same bucket as residual work), never incremental per-connector trees.
     */
    public static boolean requiresFullMaterialization(ParsedQuery pq) {
        return pq instanceof JoinQuery;
    }

    /** Whether a standalone single-source connector run may read/write filesystem connector chunks. */
    public static boolean persistConnectorSideChunks(PushdownPlan plan) {
        return plan.persistSnapshotMaterialization() && !plan.residualOps().isEmpty();
    }
}
