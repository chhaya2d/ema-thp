package org.emathp.model;

import java.util.List;

/**
 * Normalized query handed to a {@link org.emathp.connector.Connector}. Pagination is represented
 * via {@code cursor} (opaque, normalized — never a provider-native token) and {@code pageSize}.
 *
 * <p>{@code limit} is intentionally absent from this type. The connector contract is
 * cursor-pagination only; total-result capping is always engine-side. See ADR-0003.
 */
public record ConnectorQuery(
        List<String> projection,
        ComparisonExpr where,
        List<OrderBy> orderBy,
        String cursor,
        Integer pageSize) {

    public ConnectorQuery {
        projection = projection == null ? List.of() : List.copyOf(projection);
        orderBy = orderBy == null ? List.of() : List.copyOf(orderBy);
    }
}
