package org.emathp.config;

import java.time.Duration;

/**
 * Compiled-in fallbacks for {@link WebEnv} and related demo web wiring. OS env and {@code .env}
 * override these via {@link RuntimeEnv#get(String, String)} when keys are set to non-blank values.
 */
public final class WebDefaults {

    private WebDefaults() {}

    public static final String EMA_WEB_USE_MOCK_CONNECTORS = "false";

    /**
     * When {@code true}, filesystem snapshot IO is allowed per {@link org.emathp.snapshot.policy.SnapshotMaterializationPolicy}
     * (incremental vs fully materialised). When {@code false}, all snapshot IO is skipped.
     */
    public static final String EMA_PUSHDOWN_SNAPSHOT_RUN = "false";
    public static final String EMA_DEV_H2 = "false";
    public static final String WEB_PORT = "8080";

    public static final String POSTGRES_HOST = "localhost";
    public static final String POSTGRES_PORT = "5432";
    public static final String POSTGRES_DB = "emathp";
    public static final String POSTGRES_USER = "emathp";
    public static final String POSTGRES_PASSWORD = "emathp";

    /** In-memory H2 when {@code EMA_DEV_H2=true}. */
    public static final String DEV_H2_JDBC_URL =
            "jdbc:h2:mem:webdemo;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    public static final String DEV_H2_USER = "sa";
    public static final String DEV_H2_PASSWORD = "";

    public static final String DEMO_USER_ID = "demo-user";

    /** Loopback-only bind for {@link org.emathp.web.DemoWebServer}. */
    public static final String HTTP_BIND_ADDRESS = "127.0.0.1";

    public static final int MAX_HTTP_REQUEST_BODY_BYTES = 512_000;

    /**
     * Logical UI page size for production OAuth demo (real Google connector stack); independent of
     * provider batch size.
     */
    public static final int UI_QUERY_PAGE_SIZE = 10;

    /**
     * Logical UI page size for mock-connector demo + tests (filesystem snapshots under {@code
     * data/test/}).
     */
    public static final int UI_QUERY_PAGE_SIZE_MOCK = 2;

    /** Alias for tests constructing runners explicitly. */
    public static final int UI_QUERY_PAGE_SIZE_TESTS = UI_QUERY_PAGE_SIZE_MOCK;

    /** OS env / {@code .env} override for {@link #EMA_PUSHDOWN_SNAPSHOT_RUN}. */
    public static boolean persistSnapshotMaterialization() {
        return "true".equalsIgnoreCase(RuntimeEnv.get(EMA_PUSHDOWN_SNAPSHOT_RUN, EMA_PUSHDOWN_SNAPSHOT_RUN));
    }

    /**
     * Default TTL written on snapshot chunk metadata when the client omits {@code maxStaleness};
     * passed to {@link org.emathp.snapshot.api.SnapshotQueryService} as the write horizon for new
     * materializations.
     */
    public static Duration snapshotChunkFreshness() {
        return Duration.ofMinutes(5);
    }

    /** Upper bound when clients pass {@code pageSize} with JSON/form body (cheap abuse guard). */
    public static final int UI_QUERY_PAGE_SIZE_CLIENT_MAX = 200;

    /**
     * Default Google OAuth redirect when {@code GOOGLE_OAUTH_REDIRECT_URI} is unset; must match an
     * authorized URI in Google Cloud (including host — {@code localhost} vs {@code 127.0.0.1}).
     */
    public static String googleOAuthRedirectUri(int webPort) {
        return "http://localhost:" + webPort + "/oauth/google/callback";
    }
}
