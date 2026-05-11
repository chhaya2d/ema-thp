package org.emathp.snapshot.model;

/** Pure logical pagination (UI): independent of provider batch size. */
public final class LogicalPagination {

    private LogicalPagination() {}

    public static int startRow(int pageNumber, int pageSize) {
        return Math.multiplyExact(pageNumber, pageSize);
    }

    /** Inclusive end row index per specification {@code endRow = startRow + pageSize - 1}. */
    public static int endRowInclusive(int pageNumber, int pageSize) {
        return startRow(pageNumber, pageSize) + pageSize - 1;
    }
}
