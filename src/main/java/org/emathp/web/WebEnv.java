package org.emathp.web;

import java.util.Objects;
import org.emathp.config.RuntimeEnv;
import org.emathp.config.WebDefaults;

/**
 * Environment-driven settings for {@link DemoWebServer}. Keys are resolved via {@link
 * RuntimeEnv}; compiled-in fallbacks live in {@link WebDefaults} and are overridden by OS env
 * or {@code .env} when set to non-blank values.
 */
public record WebEnv(
        int webPort,
        String jdbcUrl,
        String pgUser,
        String pgPassword,
        String googleClientId,
        String googleClientSecret,
        String oauthRedirectUri,
        String demoUserId,
        boolean useDevH2) {

    /**
     * Loads settings for {@link DemoWebServer}. Google OAuth client fields may be blank when only
     * mock/demo modes are used; the server still builds JDBC defaults for optional live-stack init.
     */
    public static WebEnv load() {
        boolean devH2 = "true".equalsIgnoreCase(env("EMA_DEV_H2", WebDefaults.EMA_DEV_H2));
        int port = parsePort(env("WEB_PORT", WebDefaults.WEB_PORT));
        String jdbc = env("EMA_JDBC_URL", null);
        if (devH2) {
            jdbc = WebDefaults.DEV_H2_JDBC_URL;
        } else if (jdbc == null || jdbc.isBlank()) {
            String host = env("POSTGRES_HOST", WebDefaults.POSTGRES_HOST);
            String p = env("POSTGRES_PORT", WebDefaults.POSTGRES_PORT);
            String db = env("POSTGRES_DB", WebDefaults.POSTGRES_DB);
            jdbc = "jdbc:postgresql://" + host + ":" + p + "/" + db;
        }
        String userPg = devH2 ? WebDefaults.DEV_H2_USER : env("POSTGRES_USER", WebDefaults.POSTGRES_USER);
        String passPg =
                devH2 ? WebDefaults.DEV_H2_PASSWORD : env("POSTGRES_PASSWORD", WebDefaults.POSTGRES_PASSWORD);
        String cid = blankToEmpty(env("GOOGLE_OAUTH_CLIENT_ID", ""));
        String sec = blankToEmpty(env("GOOGLE_OAUTH_CLIENT_SECRET", ""));
        String redirect = env("GOOGLE_OAUTH_REDIRECT_URI", null);
        if (redirect == null || redirect.isBlank()) {
            // NOTE: must match an Authorized redirect URI in Google Cloud exactly (localhost vs
            // 127.0.0.1 are different hosts to Google).
            redirect = WebDefaults.googleOAuthRedirectUri(port);
        }
        String demoUser = env("DEMO_USER_ID", WebDefaults.DEMO_USER_ID);
        return new WebEnv(port, jdbc, userPg, passPg, cid, sec, redirect, demoUser, devH2);
    }

    private static String env(String key, String defaultVal) {
        return RuntimeEnv.get(key, defaultVal);
    }

    private static String blankToEmpty(String s) {
        return s == null || s.isBlank() ? "" : s.trim();
    }

    private static int parsePort(String raw) {
        try {
            int p = Integer.parseInt(Objects.requireNonNull(raw).trim());
            if (p <= 0 || p > 65535) {
                throw new IllegalArgumentException("port out of range: " + p);
            }
            return p;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("WEB_PORT must be an integer", e);
        }
    }
}
