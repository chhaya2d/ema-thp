package org.emathp.federation;

import java.util.List;
import org.emathp.model.EngineRow;

/** One client page: offset cursor + page size applied to a capped materialization. */
public record MaterializedPage(
        List<EngineRow> rows, String nextCursor, int upstreamRowCount, boolean stoppedAtLimit) {

    public MaterializedPage {
        rows = rows == null ? List.of() : List.copyOf(rows);
    }
}
