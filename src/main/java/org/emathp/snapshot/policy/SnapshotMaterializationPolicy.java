package org.emathp.snapshot.policy;

import org.emathp.model.JoinQuery;
import org.emathp.model.ParsedQuery;
import org.emathp.planner.PushdownPlan;

/**
 * Central policy for snapshot persistence shape.
 *
 * <ul>
 *   <li><b>Single-source connector legs</b>: every leg persists chunks when global persistence is
 *       on — both residual paths (engine over-fetched and filtered locally) and pure-pushdown
 *       paths (connector returned exactly the requested rows). The cache saves the provider call
 *       in both cases; pure-pushdown was excluded earlier on the rationale that "engine did no
 *       work," but the network round-trip and the upstream's rate-limit quota are also work
 *       worth caching.</li>
 *   <li><b>Joins</b>: {@link #requiresFullMaterialization(ParsedQuery)} is true; the engine must
 *       complete the join semantics before the answer is stable, so persistence follows the
 *       fully-materialised layout under {@link
 *       org.emathp.snapshot.layout.SnapshotPaths#MATERIALIZED_QUERY_SEGMENT}.</li>
 * </ul>
 *
 * <p><b>Bypass:</b> clients can opt out of the cache per request via {@code maxStaleness=PT0S} —
 * any persisted chunk older than 0s is stale, so the engine re-fetches.
 *
 * <p><b>Assumption (TODO):</b> no {@link org.emathp.connector.Connector} may accept federated
 * <em>join</em> pushdown; join composition stays engine-only. If that ever changes, revisit
 * {@link #requiresFullMaterialization} and full-materialisation layout.
 */
public final class SnapshotMaterializationPolicy {

    private SnapshotMaterializationPolicy() {}

    /**
     * Engine-composed queries always take the fully materialised snapshot path when persistence
     * is on (same bucket as residual work), never incremental per-connector trees.
     */
    public static boolean requiresFullMaterialization(ParsedQuery pq) {
        return pq instanceof JoinQuery;
    }

    /**
     * Whether a standalone single-source connector run may read/write filesystem connector
     * chunks. Today: gated only by the global persistence flag — residual and pure-pushdown
     * paths both persist.
     */
    public static boolean persistConnectorSideChunks(PushdownPlan plan) {
        return plan.persistSnapshotMaterialization();
    }
}
