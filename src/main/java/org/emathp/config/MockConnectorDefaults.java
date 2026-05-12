package org.emathp.config;

/**
 * Compiled-in mock-connector behaviour (not {@code .env} / UI). Used by {@link
 * org.emathp.connector.mock.MockConnectorDevSettings#compiled()} for the demo web server; unit
 * tests use {@link org.emathp.connector.mock.MockConnectorDevSettings#none()}.
 */
public final class MockConnectorDefaults {

    private MockConnectorDefaults() {}

    /**
     * Milliseconds to sleep before each mock Google / mock Notion {@code search}. Keep {@code 0}
     * for fast CI; raise locally if you want loading-state demos without changing demo mode.
     */
    public static final int MOCK_SEARCH_DELAY_MILLIS = 0;
}
