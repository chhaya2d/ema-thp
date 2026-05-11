package org.emathp.federation;

import java.util.List;
import org.emathp.engine.LimitExecutor;
import org.emathp.model.EngineRow;

/** Logical rows after engine {@code LIMIT} (or unconstrained slice), kept for paging and snapshot IO. */
public record MaterializedRowSet(List<EngineRow> limitedRows, int totalBeforeLimit, boolean stoppedAtLimit) {

    public MaterializedRowSet {
        limitedRows = limitedRows == null ? List.of() : List.copyOf(limitedRows);
    }

    public static MaterializedRowSet limitedFrom(List<EngineRow> rowsBeforeLimit, Integer limit) {
        LimitExecutor.Limited<EngineRow> lim = LimitExecutor.apply(rowsBeforeLimit, limit);
        return new MaterializedRowSet(lim.rows(), lim.totalBeforeLimit(), lim.stoppedAtLimit());
    }
}
