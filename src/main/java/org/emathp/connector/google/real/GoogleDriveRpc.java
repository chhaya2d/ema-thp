package org.emathp.connector.google.real;

import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;

/**
 * Abstraction over Drive {@code files.list} so production HTTP and unit-test doubles share a port.
 */
@FunctionalInterface
public interface GoogleDriveRpc {

    GoogleSearchResponse listFiles(String accessToken, GoogleSearchRequest request)
            throws Exception;
}
