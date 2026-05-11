package org.emathp.model;

import java.util.List;

/**
 * Connector-agnostic search response. {@code nextCursor} is a normalized opaque token; the
 * planner / engine MUST NOT inspect it or know about {@code pageToken}, {@code start_cursor},
 * etc. To request the next page, pass {@code nextCursor} back as
 * {@link ConnectorQuery#cursor()} on the following call.
 */
public record SearchResult<T>(List<T> rows, String nextCursor) {

    public SearchResult {
        rows = rows == null ? List.of() : List.copyOf(rows);
    }
}
