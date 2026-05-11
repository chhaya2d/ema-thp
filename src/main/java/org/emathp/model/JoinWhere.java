package org.emathp.model;

import java.util.Objects;

/**
 * Single-side WHERE clause on a {@link JoinQuery}: the predicate references exactly one of the
 * two joined sides, identified by {@code alias}.
 *
 * <p>The {@link ComparisonExpr#field()} stored here is the <em>unqualified</em> column name (the
 * parser strips the {@code alias.} prefix before constructing this record). The executor routes
 * the predicate to the matching side's per-side {@link Query}, where it goes through the
 * existing {@code Planner} - so it gets pushed or residualized exactly like any single-source
 * WHERE.
 *
 * @apiNote v1 supports only single-side WHERE on JOIN queries. Cross-side predicates
 *          (e.g. {@code g.updatedAt > n.lastEditedTime}) require qualified-field semantics in
 *          the residual evaluator and a post-join filtering pass; deferred.
 */
public record JoinWhere(String alias, ComparisonExpr predicate) {

    public JoinWhere {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(predicate, "predicate");
    }
}
