package org.emathp.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.emathp.engine.internal.FilterValueOps;
import org.emathp.engine.internal.RowFields;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.EngineRow;

/**
 * Applies a residual WHERE (not pushed to the connector) to rows in memory. Field resolution reads
 * {@link EngineRow#fields()}.
 *
 * @implNote LIKE is case-insensitive substring containment, with leading and trailing {@code %}
 *           stripped. Internal {@code %} or {@code _} wildcards are NOT honored.
 */
public final class FilterExecutor {

    public List<EngineRow> apply(List<EngineRow> rows, ComparisonExpr where) {
        if (where == null) {
            return rows;
        }
        List<EngineRow> out = new ArrayList<>(rows.size());
        for (EngineRow r : rows) {
            if (matches(r, where)) {
                out.add(r);
            }
        }
        return out;
    }

    private static boolean matches(EngineRow r, ComparisonExpr w) {
        Object actual = RowFields.get(r, w.field());
        Object expected = w.value();
        return switch (w.operator()) {
            case EQ -> Objects.equals(actual, expected);
            case GT -> FilterValueOps.compareNullSafe(actual, expected) > 0;
            case LT -> FilterValueOps.compareNullSafe(actual, expected) < 0;
            case LIKE -> FilterValueOps.likeMatch(actual, expected);
        };
    }
}
