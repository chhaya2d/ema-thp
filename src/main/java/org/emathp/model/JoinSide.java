package org.emathp.model;

import java.util.Objects;

/**
 * One side of a {@link JoinQuery}.
 *
 * @param connectorName logical SQL identifier the user wrote in {@code FROM}/{@code JOIN}
 *                      (e.g. {@code "google"}). The executor receives a registry mapping these
 *                      logical names to concrete {@link org.emathp.connector.Connector}s.
 *                      Distinct from {@code Connector.source()}, which is the connector's
 *                      self-describing identity (e.g. {@code "google-drive"}).
 * @param alias the qualifier used in column references (e.g. {@code "g"} in {@code g.title}).
 *              Falls back to {@code connectorName} when the user wrote no AS clause, so the
 *              column-to-side mapping rule is uniform.
 */
public record JoinSide(String connectorName, String alias) {

    public JoinSide {
        Objects.requireNonNull(connectorName, "connectorName");
        Objects.requireNonNull(alias, "alias");
    }
}
