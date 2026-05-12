package org.emathp.config;

/**
 * Compiled-in settings for {@link org.emathp.connector.google.demo.DemoGoogleDriveConnector} and
 * {@link org.emathp.connector.notion.demo.DemoNotionConnector} (not {@code .env} / UI). Adjust here
 * for local join demos and simulated latency.
 */
public final class DemoConnectorDefaults {

    private DemoConnectorDefaults() {}

    /** Fixed delay before each demo {@code search} (both Google and Notion demo connectors). */
    public static final int SEARCH_DELAY_MILLIS = 5_000;

    /** Provider batch size advertised by demo connectors (each {@code search} returns up to this many rows). */
    public static final int PROVIDER_PAGE_SIZE = 6;

    /** Default UI page size when the playground runs in {@code demo} connector mode. */
    public static final int DEMO_UI_PAGE_SIZE = 2;
}
