package org.emathp.connector.google.api;

import java.util.List;

public record GoogleSearchResponse(List<GoogleDriveFile> files, String nextPageToken) {

    public GoogleSearchResponse {
        files = files == null ? List.of() : List.copyOf(files);
    }
}
