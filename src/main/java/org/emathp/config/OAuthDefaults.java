package org.emathp.config;

/**
 * Google OAuth / Drive API constants used by {@link org.emathp.oauth.GoogleOAuthService} and token
 * storage; not overridden by {@link RuntimeEnv} (Google’s public endpoints and scope strings).
 */
public final class OAuthDefaults {

    private OAuthDefaults() {}

    public static final String GOOGLE_DRIVE_READONLY_SCOPE =
            "https://www.googleapis.com/auth/drive.readonly";

    public static final String GOOGLE_OAUTH_AUTH_ENDPOINT =
            "https://accounts.google.com/o/oauth2/v2/auth";

    public static final String GOOGLE_OAUTH_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";

    /** Used when the token response omits {@code expires_in}. */
    public static final int GOOGLE_TOKEN_EXPIRES_IN_FALLBACK_SECONDS = 3600;
}
