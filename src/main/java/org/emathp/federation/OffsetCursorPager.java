package org.emathp.federation;

import java.util.ArrayList;
import java.util.List;
import org.emathp.model.EngineRow;

/** Numeric-offset cursor slicing over an in-memory capped row list — query-shape agnostic. */
public final class OffsetCursorPager {

    private OffsetCursorPager() {}

    public static MaterializedPage page(MaterializedRowSet rowSet, String cursor, Integer pageSize) {
        return page(
                rowSet.limitedRows(),
                rowSet.totalBeforeLimit(),
                rowSet.stoppedAtLimit(),
                cursor,
                pageSize);
    }

    /** For cache replay: capped rows plus metrics stored alongside the snapshot. */
    public static MaterializedPage page(
            List<EngineRow> cappedRows,
            int upstreamRowCount,
            boolean stoppedAtLimit,
            String cursor,
            Integer pageSize) {
        return slice(cappedRows, upstreamRowCount, stoppedAtLimit, cursor, pageSize);
    }

    private static MaterializedPage slice(
            List<EngineRow> cappedRows,
            int upstreamRowCount,
            boolean stoppedAtLimit,
            String cursor,
            Integer pageSize) {

        int materialized = cappedRows.size();
        int start = parseCursor(cursor);
        if (start > materialized) {
            start = materialized;
        }
        int ps = pageSize == null ? Integer.MAX_VALUE : pageSize;
        int end = (int) Math.min((long) start + ps, materialized);

        List<EngineRow> page = new ArrayList<>(cappedRows.subList(start, end));
        String nextCursor = end < materialized ? String.valueOf(end) : null;

        return new MaterializedPage(List.copyOf(page), nextCursor, upstreamRowCount, stoppedAtLimit);
    }

    public static int parseCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        try {
            int v = Integer.parseInt(cursor.trim(), 10);
            if (v < 0) {
                throw new IllegalArgumentException("Cursor must be non-negative; got '" + cursor + "'");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cursor must be a numeric offset; got '" + cursor + "'", e);
        }
    }
}
