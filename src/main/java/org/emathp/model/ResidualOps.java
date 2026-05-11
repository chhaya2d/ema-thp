package org.emathp.model;

import java.util.List;

/**
 * Operations the engine must apply in-memory because the connector did not push them.
 * Derived by diffing the original {@link Query} against the connector's pushed
 * {@link ConnectorQuery}: anything present in the original but missing from the pushed query
 * lands here.
 *
 * @apiNote {@code limit} is intentionally NOT modeled here. Engine-level LIMIT is always applied
 *          (regardless of whether the connector advertised support — see ADR-0002), so it has
 *          its own dedicated path through {@code QueryExecutor.execute}'s {@code logicalLimit}
 *          parameter rather than being treated as a "residual" operation.
 * @apiNote PROJECTION is also intentionally NOT modeled here yet. Projection changes {@link
 *          EngineRow} field sets, not just which rows survive — deferred.
 */
public record ResidualOps(ComparisonExpr where, List<OrderBy> orderBy) {

    public static final ResidualOps NONE = new ResidualOps(null, List.of());

    public ResidualOps {
        orderBy = orderBy == null ? List.of() : List.copyOf(orderBy);
    }

    /**
     * Compute residuals as the diff between the user's original {@link Query} and the
     * connector-bound {@link ConnectorQuery} the planner produced.
     *
     * <p>Boolean-capability semantics (ADR-0001) make each clause all-or-nothing: an operation
     * is either fully pushed (present on {@code pushed}) or fully residual (present on
     * {@code original} only). Partial pushdown is not currently representable.
     */
    public static ResidualOps from(Query original, ConnectorQuery pushed) {
        ComparisonExpr resWhere =
                pushed.where() == null && original.where() != null ? original.where() : null;
        List<OrderBy> resOrderBy =
                pushed.orderBy().isEmpty() && !original.orderBy().isEmpty()
                        ? original.orderBy()
                        : List.of();
        return new ResidualOps(resWhere, resOrderBy);
    }

    public boolean isEmpty() {
        return where == null && orderBy.isEmpty();
    }
}
