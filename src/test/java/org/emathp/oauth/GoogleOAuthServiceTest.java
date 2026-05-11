package org.emathp.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class GoogleOAuthServiceTest {

    @Test
    void authorizationUrlContainsOfflineConsentAndScope() {
        GoogleOAuthService oauth =
                new GoogleOAuthService("cid", "sec", "http://localhost/cb");

        URI uri = oauth.buildAuthorizationUrl("state-xyz");

        String s = uri.toString();
        assertTrue(s.contains("access_type=offline"), s);
        assertTrue(s.contains("prompt=consent"), s);
        assertTrue(s.contains("drive.readonly"), s);
        assertTrue(s.contains("client_id=cid"), s);
        assertTrue(s.contains("state=state-xyz"), s);
    }

    @Test
    void authorizationUrlContainsPkceParamsWhenChallengeProvided() {
        GoogleOAuthService oauth =
                new GoogleOAuthService("cid", "sec", "http://localhost/cb");

        URI uri = oauth.buildAuthorizationUrl("state-xyz", "fakeChallengeBase64url");

        String s = uri.toString();
        assertTrue(s.contains("code_challenge="), s);
        assertTrue(s.contains("code_challenge_method=S256"), s);
        assertTrue(s.contains("access_type=offline"), s);
        assertTrue(s.contains("prompt=consent"), s);
    }

    @Test
    void exchangeAuthorizationCodeAgainstStubTokenEndpoint() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/token",
                exchange -> {
                    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                        exchange.sendResponseHeaders(405, -1);
                        exchange.close();
                        return;
                    }
                    String body =
                            "{\"access_token\":\"tok\",\"expires_in\":3600,\"refresh_token\":\"ref\"}";
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                });
        server.start();
        try {
            int port = server.getAddress().getPort();
            String tokenUrl = "http://127.0.0.1:" + port + "/token";
            GoogleOAuthService oauth =
                    new GoogleOAuthService(
                            HttpClient.newHttpClient(),
                            "cid",
                            "sec",
                            "http://localhost/cb",
                            "https://accounts.google.com/o/oauth2/v2/auth",
                            tokenUrl);

            GoogleOAuthService.TokenBundle bundle = oauth.exchangeAuthorizationCode("auth-code", null);

            assertEquals("tok", bundle.accessToken());
            assertEquals("ref", bundle.refreshToken());
            assertNotNull(bundle.expiresAt());
            assertTrue(bundle.expiresAt().isAfter(Instant.now()));
        } finally {
            server.stop(0);
        }
    }
}
