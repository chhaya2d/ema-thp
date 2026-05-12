package org.emathp.connector.google.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.notion.api.NotionSearchRequest;
import org.emathp.connector.notion.mock.MockNotionApi;
import org.junit.jupiter.api.Test;

class ProviderPaginationTest {

    @Test
    void mockGoogleCursorProgression_eofNull() {
        MockGoogleDriveApi api = new MockGoogleDriveApi();
        // Unfiltered: 8 sample files — page size 6 → cursor_6 then EOF.
        GoogleSearchRequest r1 = new GoogleSearchRequest(null, "name asc", 6, null, null);
        var p1 = api.search(r1, null);
        assertEquals("cursor_6", p1.nextPageToken());
        GoogleSearchRequest r2 =
                new GoogleSearchRequest(null, "name asc", 6, null, p1.nextPageToken());
        var p2 = api.search(r2, null);
        assertNull(p2.nextPageToken());
    }

    @Test
    void mockNotionCursorProgression_eofNull() {
        MockNotionApi api = new MockNotionApi();
        var p1 = api.search(new NotionSearchRequest(null, null, 4), null);
        assertEquals("cursor_4", p1.nextCursor());
        var p2 = api.search(new NotionSearchRequest(null, p1.nextCursor(), 4), null);
        assertNull(p2.nextCursor());
    }
}
