package org.emathp.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.emathp.snapshot.model.LogicalPagination;
import org.junit.jupiter.api.Test;

class LogicalPaginationTest {

    @Test
    void logicalPagesIndependentOfProviderBatchSize() {
        assertEquals(0, LogicalPagination.startRow(0, 2));
        assertEquals(1, LogicalPagination.endRowInclusive(0, 2));
        assertEquals(10, LogicalPagination.startRow(5, 2));
        assertEquals(11, LogicalPagination.endRowInclusive(5, 2));
        assertEquals(100, LogicalPagination.startRow(10, 10));
    }
}
