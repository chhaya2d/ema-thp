package org.emathp.engine;



import java.util.ArrayList;

import java.util.Comparator;

import java.util.List;

import org.emathp.engine.internal.FilterValueOps;

import org.emathp.engine.internal.RowFields;

import org.emathp.model.Direction;

import org.emathp.model.EngineRow;

import org.emathp.model.OrderBy;



/**

 * Applies residual ORDER BY (not pushed to the connector) to rows in memory.

 *

 * @implNote Sort keys are compared via {@link FilterValueOps#compareNullSafe}; incompatible

 *           runtime types throw {@link ClassCastException}, same as residual WHERE comparisons.

 */

public final class SortExecutor {



    public List<EngineRow> apply(List<EngineRow> rows, List<OrderBy> orderBy) {

        if (orderBy.isEmpty()) {

            return rows;

        }

        Comparator<EngineRow> cmp = comparatorFor(orderBy.get(0));

        for (int i = 1; i < orderBy.size(); i++) {

            cmp = cmp.thenComparing(comparatorFor(orderBy.get(i)));

        }

        List<EngineRow> out = new ArrayList<>(rows);

        out.sort(cmp);

        return out;

    }



    private static Comparator<EngineRow> comparatorFor(OrderBy ob) {

        String field = ob.field();

        Comparator<EngineRow> base =

                (a, b) ->

                        FilterValueOps.compareNullSafe(

                                RowFields.get(a, field), RowFields.get(b, field));

        return ob.direction() == Direction.DESC ? base.reversed() : base;

    }

}

