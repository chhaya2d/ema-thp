package org.emathp.model;

import java.util.List;
import java.util.Objects;

/**
 * Two-source INNER JOIN with a single equi-predicate.
 *
 * <p>v1 surface:
 * {@code SELECT [* | qual.col, ...] FROM x a JOIN y b ON a.f = b.g [WHERE a.col op v] [LIMIT n]}.
 *
 * <ul>
 *   <li>WHERE may reference only one side - the parser routes it to that side's per-side
 *       {@link Query} via {@link JoinWhere}.</li>
 *   <li>{@code cursor} / {@code pageSize} paginate the joined output (see {@link #withPagination}).
 *       The user-facing pagination contract; not in SQL, layered on top before execution.</li>
 *   <li>Post-join WHERE / ORDER BY are NOT yet supported - they need qualified-field semantics
 *       across two sides (deferred).</li>
 * </ul>
 *
 * @param select dotted strings (e.g. {@code "g.title"}) when the user named columns; empty
 *               list when the user wrote {@code SELECT *}. Parsed-and-stored only; projection
 *               isn't applied to the result yet.
 */
public record JoinQuery(
        JoinSide left,
        JoinSide right,
        JoinPredicate on,
        JoinWhere where,
        List<String> select,
        Integer limit,
        String cursor,
        Integer pageSize) implements ParsedQuery {

    public JoinQuery {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(on, "on");
        select = select == null ? List.of() : List.copyOf(select);
    }

    public JoinQuery withPagination(String cursor, Integer pageSize) {
        return new JoinQuery(left, right, on, where, select, limit, cursor, pageSize);
    }
}
