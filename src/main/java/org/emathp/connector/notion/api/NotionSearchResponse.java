package org.emathp.connector.notion.api;

import java.util.List;

public record NotionSearchResponse(List<NotionPage> pages, String nextCursor, boolean hasMore) {

    public NotionSearchResponse {
        pages = pages == null ? List.of() : List.copyOf(pages);
    }
}
