package org.emathp.engine.internal;

import org.emathp.model.EngineRow;

/**
 * Single source for field reads on {@link EngineRow} (residual filter/sort, join keys).
 */
public final class RowFields {

    private RowFields() {}

    public static Object get(EngineRow row, String field) {
        if (row == null || field == null) {
            return null;
        }
        return row.get(field);
    }
}
