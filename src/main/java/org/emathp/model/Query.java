package org.emathp.model;

import java.util.List;

/**
 * User-facing query the planner consumes. {@code cursor} and {@code pageSize} are the normalized
 * pagination contract; the SQL parser leaves them null and the caller layers them on top before
 * planning.
 */
public record Query(
        List<String> select,
        ComparisonExpr where,
        List<OrderBy> orderBy,
        Integer limit,
        String cursor,
        Integer pageSize) implements ParsedQuery {

    public Query {
        select = select == null ? List.of() : List.copyOf(select);
        orderBy = orderBy == null ? List.of() : List.copyOf(orderBy);
    }

    public Query withPagination(String cursor, Integer pageSize) {
        return new Query(select, where, orderBy, limit, cursor, pageSize);
    }
}
