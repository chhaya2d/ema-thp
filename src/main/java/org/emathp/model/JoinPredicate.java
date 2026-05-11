package org.emathp.model;

import java.util.Objects;

/**
 * Equi-join predicate: {@code leftAlias.leftField = rightAlias.rightField}.
 *
 * <p>The parser <em>canonicalizes</em> the order so that {@code leftAlias} always matches the
 * {@link JoinQuery#left()} side's alias, regardless of which order the user wrote the equality
 * in. That lets the executor build the right side's hash table blindly without re-checking
 * orientation.
 *
 * @apiNote v1 is equi-only (single equality between two qualified columns). Theta joins
 *          (range, inequality, multi-column compound) are out of scope until a use case
 *          motivates them.
 */
public record JoinPredicate(
        String leftAlias,
        String leftField,
        String rightAlias,
        String rightField) {

    public JoinPredicate {
        Objects.requireNonNull(leftAlias, "leftAlias");
        Objects.requireNonNull(leftField, "leftField");
        Objects.requireNonNull(rightAlias, "rightAlias");
        Objects.requireNonNull(rightField, "rightField");
    }
}
