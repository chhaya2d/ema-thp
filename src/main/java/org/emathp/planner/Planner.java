package org.emathp.planner;

import java.util.ArrayList;
import java.util.List;
import org.emathp.config.WebDefaults;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.metrics.Metrics;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.OrderBy;
import org.emathp.model.Query;
import org.emathp.model.ResidualOps;

/**
 * Rule-based planner. Decides which slice of a {@link Query} can be pushed to a connector and
 * which slice stays pending for residual execution.
 *
 * <p>Rules:
 * <ul>
 *   <li>WHERE: pushed iff the connector supports filtering AND the predicate's operator is in
 *       {@link CapabilitySet#supportedOperators()} AND the predicate's field is in
 *       {@link CapabilitySet#supportedFields()}.</li>
 *   <li>ORDER BY: pushed iff the connector supports sorting AND WHERE is ok (no WHERE in the
 *       query, or WHERE was pushed).</li>
 *   <li>PROJECTION: pushed iff the connector supports projection.</li>
 *   <li>PAGINATION: pushed iff the connector supports pagination AND WHERE is ok AND ORDER BY is
 *       ok. {@link ConnectorQuery#pageSize()} is set to {@link Connector#defaultFetchPageSize()} —
 *       the engine query's {@link Query#pageSize()} only signals that federation paging is enabled.
 *       {@link ConnectorQuery#cursor()} carries the user's opaque resume token.</li>
 * </ul>
 *
 * <p>LIMIT is not a planner decision — it is always engine-enforced (see ADR-0003) and never
 * appears in the pushed query or in {@code pendingOperations}. The engine's
 * {@code QueryExecutor.execute} caps the final result at {@code Query.limit()}.
 *
 * @implNote The pushdown rules encode SQL's logical evaluation order
 *           (WHERE → ORDER BY → PAGINATION): an operation can be pushed only when every
 *           logically-earlier operation is also pushed (or absent from the query). The
 *           "WHERE is ok" / "ORDER BY is ok" gates prevent a connector from, e.g., paginating
 *           an unfiltered universe while WHERE is residual. This composes with the predicate
 *           operator/field gates: if WHERE falls residual because the operator or field is
 *           unsupported, ORDER BY and PAGINATION cascade to residual too.
 * @implNote The PAGINATION rule deliberately allows pushdown when the query has no ORDER BY at
 *           all, in addition to "ORDER BY was pushed". A strict reading requires a pushed
 *           ORDER BY; if the user never asked for an order there is no consistency to violate.
 *           Tradeoff: a connector returning rows in arbitrary order produces a stable but
 *           unspecified top-N. Drop the {@code query.orderBy().isEmpty() ||} clause to revert
 *           to strict semantics; the demo output is unaffected because the demo query always
 *           has an explicit ORDER BY.
 * @implNote {@link CapabilitySet#supportedFields()} is intentionally consulted only for the
 *           WHERE clause today — its scope is predicate pushdown. ORDER BY field validity is
 *           NOT gated here: a pushed sort on a field the connector can't actually order by will
 *           reach the translator, where it may throw or silently fall through. The
 *           filterable / sortable / projectable field sets are commonly different on real
 *           SaaS APIs (Drive, Notion); the field set will likely split into per-clause sets
 *           when a connector with diverging support lands. Until then, sort/project field
 *           validation is the translator's responsibility.
 */
public final class Planner {

    private final boolean persistSnapshotMaterialization;

    public Planner() {
        this(WebDefaults.persistSnapshotMaterialization());
    }

    public Planner(boolean persistSnapshotMaterialization) {
        this.persistSnapshotMaterialization = persistSnapshotMaterialization;
    }

    public boolean persistSnapshotMaterialization() {
        return persistSnapshotMaterialization;
    }

    public PushdownPlan plan(Connector connector, Query query) {
        CapabilitySet caps = connector.capabilities();
        List<String> pending = new ArrayList<>();

        ComparisonExpr pushedWhere = null;
        if (query.where() != null) {
            ComparisonExpr w = query.where();
            boolean operatorOk = caps.supportedOperators().contains(w.operator());
            boolean fieldOk = caps.supportedFields().contains(w.field());
            if (caps.supportsFiltering() && operatorOk && fieldOk) {
                pushedWhere = w;
                Metrics.PLANNER_PUSHED.inc(connector.source(), "WHERE");
            } else {
                pending.add("WHERE");
                Metrics.PLANNER_RESIDUAL.inc(connector.source(), "WHERE");
            }
        }
        boolean whereOk = query.where() == null || pushedWhere != null;

        List<OrderBy> pushedOrderBy = List.of();
        boolean orderByPushed = false;
        if (!query.orderBy().isEmpty()) {
            if (caps.supportsSorting() && whereOk) {
                pushedOrderBy = query.orderBy();
                orderByPushed = true;
                Metrics.PLANNER_PUSHED.inc(connector.source(), "ORDER_BY");
            } else {
                pending.add("ORDER BY");
                Metrics.PLANNER_RESIDUAL.inc(connector.source(), "ORDER_BY");
            }
        }
        boolean orderByOk = query.orderBy().isEmpty() || orderByPushed;

        List<String> pushedProjection = List.of();
        if (!query.select().isEmpty()) {
            if (caps.supportsProjection()) {
                pushedProjection = query.select();
                Metrics.PLANNER_PUSHED.inc(connector.source(), "PROJECTION");
            } else {
                pending.add("PROJECTION");
                Metrics.PLANNER_RESIDUAL.inc(connector.source(), "PROJECTION");
            }
        }

        String pushedCursor = null;
        Integer pushedPageSize = null;
        boolean queryHasPagination = query.pageSize() != null || query.cursor() != null;
        if (queryHasPagination) {
            if (caps.supportsPagination() && whereOk && orderByOk) {
                pushedCursor = query.cursor();
                // UI query pageSize only enables pagination; connector batch size comes from the
                // connector (see Connector#defaultFetchPageSize).
                pushedPageSize = connector.defaultFetchPageSize();
                Metrics.PLANNER_PUSHED.inc(connector.source(), "PAGINATION");
            } else {
                pending.add("PAGINATION");
                Metrics.PLANNER_RESIDUAL.inc(connector.source(), "PAGINATION");
            }
        }

        ConnectorQuery pushedQuery = new ConnectorQuery(
                pushedProjection,
                pushedWhere,
                pushedOrderBy,
                pushedCursor,
                pushedPageSize);
        ResidualOps residualOps = ResidualOps.from(query, pushedQuery);
        return new PushdownPlan(pushedQuery, pending, residualOps, persistSnapshotMaterialization);
    }
}
