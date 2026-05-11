package org.emathp.oauth;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.emathp.config.OAuthDefaults;

/**
 * Authorization-code + refresh-token flows against Google's OAuth 2.0 endpoints. Persistence is the
 * caller's responsibility (see {@code GoogleTokenStore} in the real Google connector package).
 *
 * <p>Every {@linkplain #buildAuthorizationUrl authorization URL} uses {@code access_type=offline}
 * and {@code prompt=consent} so Google reliably returns a <strong>refresh token</strong> for
 * delegated user Drive access (see Google OAuth docs on offline access and consent re-prompt).
 */
public final class GoogleOAuthService {

    /** @see OAuthDefaults#GOOGLE_DRIVE_READONLY_SCOPE */
    public static final String DRIVE_READONLY_SCOPE = OAuthDefaults.GOOGLE_DRIVE_READONLY_SCOPE;

    private final HttpClient http;
    private final Gson gson = new Gson();
    private final String clientId;
    private final String clientSecret;
    private final String redirectUri;
    private final String authEndpoint;
    private final String tokenEndpoint;

    public GoogleOAuthService(String clientId, String clientSecret, String redirectUri) {
        this(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build(),
                clientId,
                clientSecret,
                redirectUri,
                OAuthDefaults.GOOGLE_OAUTH_AUTH_ENDPOINT,
                OAuthDefaults.GOOGLE_OAUTH_TOKEN_ENDPOINT);
    }

    /**
     * For tests: inject {@link HttpClient} and custom token URL (e.g. local {@link HttpServer}
     * stub).
     */
    GoogleOAuthService(
            HttpClient http,
            String clientId,
            String clientSecret,
            String redirectUri,
            String authEndpoint,
            String tokenEndpoint) {
        this.http = Objects.requireNonNull(http, "http");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret");
        this.redirectUri = Objects.requireNonNull(redirectUri, "redirectUri");
        this.authEndpoint = Objects.requireNonNull(authEndpoint, "authEndpoint");
        this.tokenEndpoint = Objects.requireNonNull(tokenEndpoint, "tokenEndpoint");
    }

    public URI buildAuthorizationUrl(String state) {
        return buildAuthorizationUrl(state, null);
    }

    /**
     * Builds the browser redirect URL for the authorization-code flow.
     *
     * <p>Always includes {@code access_type=offline} (request refresh token) and {@code
     * prompt=consent} (force consent screen so a refresh token is issued, including re-connects).
     *
     * <p>When {@code codeChallengeS256} is non-null, adds {@code code_challenge} and {@code
     * code_challenge_method=S256} (PKCE). The same verifier must be sent as {@code code_verifier}
     * on token exchange.
     */
    public URI buildAuthorizationUrl(String state, String codeChallengeS256) {
        String scope = URLEncoder.encode(DRIVE_READONLY_SCOPE, StandardCharsets.UTF_8);
        String redirect = URLEncoder.encode(redirectUri, StandardCharsets.UTF_8);
        String st = URLEncoder.encode(Objects.requireNonNull(state, "state"), StandardCharsets.UTF_8);
        StringBuilder query = new StringBuilder(
                String.join(
                        "&",
                        "response_type=code",
                        "access_type=offline",
                        "prompt=consent",
                        "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8),
                        "redirect_uri=" + redirect,
                        "scope=" + scope,
                        "state=" + st));
        if (codeChallengeS256 != null && !codeChallengeS256.isBlank()) {
            query.append("&code_challenge=")
                    .append(enc(codeChallengeS256))
                    .append("&code_challenge_method=S256");
        }
        return URI.create(authEndpoint + "?" + query);
    }

    public TokenBundle exchangeAuthorizationCode(String code) throws IOException, InterruptedException {
        return exchangeAuthorizationCode(code, null);
    }

    public TokenBundle exchangeAuthorizationCode(String code, String codeVerifier)
            throws IOException, InterruptedException {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("code", code);
        form.put("client_id", clientId);
        form.put("client_secret", clientSecret);
        form.put("redirect_uri", redirectUri);
        form.put("grant_type", "authorization_code");
        if (codeVerifier != null && !codeVerifier.isBlank()) {
            form.put("code_verifier", codeVerifier);
        }
        return postToken(form);
    }

    public TokenBundle refreshAccessToken(String refreshTokenPlain) throws IOException, InterruptedException {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("refresh_token", refreshTokenPlain);
        form.put("client_id", clientId);
        form.put("client_secret", clientSecret);
        form.put("grant_type", "refresh_token");
        return postToken(form);
    }

    private TokenBundle postToken(Map<String, String> form) throws IOException, InterruptedException {
        String body = form.entrySet().stream()
                .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
                .collect(Collectors.joining("&"));
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() < 200 || res.statusCode() >= 300) {
            throw new IllegalStateException("Token endpoint HTTP " + res.statusCode() + ": " + res.body());
        }
        JsonObject o = gson.fromJson(res.body(), JsonObject.class);
        String access = o.get("access_token").getAsString();
        String refresh = o.has("refresh_token") && !o.get("refresh_token").isJsonNull()
                ? o.get("refresh_token").getAsString()
                : null;
        int expiresIn =
                o.has("expires_in")
                        ? o.get("expires_in").getAsInt()
                        : OAuthDefaults.GOOGLE_TOKEN_EXPIRES_IN_FALLBACK_SECONDS;
        Instant expiresAt = Instant.now().plusSeconds(expiresIn);
        return new TokenBundle(access, refresh, expiresAt);
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    public record TokenBundle(String accessToken, String refreshToken, Instant expiresAt) {}
}
