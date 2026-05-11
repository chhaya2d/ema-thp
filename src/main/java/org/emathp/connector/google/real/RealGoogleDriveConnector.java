package org.emathp.connector.google.real;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.ConnectorAccount;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.Operator;
import org.emathp.model.SearchResult;
import org.emathp.oauth.GoogleOAuthService;
import org.emathp.oauth.TokenEncryptor;

/**
 * OAuth-backed Google Drive connector. Same {@link CapabilitySet} and {@link #source()} as
 * {@link org.emathp.connector.google.mock.GoogleDriveConnector} so the planner treats both
 * interchangeably; only the execution path (HTTP + tokens) differs.
 */
public final class RealGoogleDriveConnector implements Connector {

    public static final String ACCOUNT_TYPE = "google-drive";

    // Mirror mock Google capabilities (ADR-0003: no supportsLimit).
    private static final CapabilitySet CAPABILITIES = new CapabilitySet(
            true,
            true,
            true,
            true,
            Set.of("title", "updatedAt"),
            Set.of(Operator.EQ, Operator.GT, Operator.LIKE));

    private final GoogleTokenStore store;
    private final TokenEncryptor encryptor;
    private final GoogleOAuthService oauth;
    private final GoogleDriveRpc rpc;
    private final RealGoogleQueryTranslator translator = new RealGoogleQueryTranslator();

    public RealGoogleDriveConnector(
            GoogleTokenStore store,
            TokenEncryptor encryptor,
            GoogleOAuthService oauth,
            GoogleDriveRpc rpc) {
        this.store = store;
        this.encryptor = encryptor;
        this.oauth = oauth;
        this.rpc = rpc;
    }

    @Override
    public String source() {
        return ACCOUNT_TYPE;
    }

    @Override
    public CapabilitySet capabilities() {
        return CAPABILITIES;
    }

    @Override
    public int defaultFetchPageSize() {
        return 20;
    }

    @Override
    public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
        String userId = userContext.userId();
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("RealGoogleDriveConnector requires a non-blank user id");
        }
        return searchWithOptional401Retry(userContext, query, false);
    }

    private SearchResult<EngineRow> searchWithOptional401Retry(
            UserContext userContext, ConnectorQuery query, boolean alreadyRefreshed) {
        try {
            String access = validAccessToken(userContext.userId());
            GoogleSearchResponse response = rpc.listFiles(access, translator.translate(query));
            return toSearchResult(response);
        } catch (Exception e) {
            if (!alreadyRefreshed && isUnauthorized(e)) {
                forceRefresh(userContext.userId());
                return searchWithOptional401Retry(userContext, query, true);
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(e);
        }
    }

    private static boolean isUnauthorized(Throwable e) {
        Throwable t = e;
        while (t != null) {
            String m = t.getMessage();
            if (m != null && m.contains("HTTP 401")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private void forceRefresh(String userId) {
        try {
            ConnectorAccount row =
                    store.find(userId, ACCOUNT_TYPE).orElseThrow(() -> new IllegalStateException(
                            "No connector_accounts row for userId=" + userId + " type=" + ACCOUNT_TYPE));
            refreshAndPersist(row);
        } catch (SQLException | IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private String validAccessToken(String userId) {
        try {
            ConnectorAccount acc = store.find(userId, ACCOUNT_TYPE).orElseThrow(() -> new IllegalStateException(
                    "No connector_accounts row for userId=" + userId + " type=" + ACCOUNT_TYPE));
            Instant skewedNow = Instant.now().plusSeconds(90);
            if (acc.expiresAt().isAfter(skewedNow)) {
                return encryptor.decrypt(acc.accessTokenEncrypted());
            }
            return refreshAndPersist(acc);
        } catch (SQLException | IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private String refreshAndPersist(org.emathp.auth.ConnectorAccount acc)
            throws SQLException, IOException, InterruptedException {
        String refreshPlain = encryptor.decrypt(acc.refreshTokenEncrypted());
        GoogleOAuthService.TokenBundle bundle = oauth.refreshAccessToken(refreshPlain);
        String accessEnc = encryptor.encrypt(bundle.accessToken());
        String refreshEnc =
                bundle.refreshToken() != null
                        ? encryptor.encrypt(bundle.refreshToken())
                        : acc.refreshTokenEncrypted();
        Instant now = Instant.now();
        var updated = GoogleTokenStore.copyWithNewTokens(acc, accessEnc, refreshEnc, bundle.expiresAt(), now);
        store.upsert(updated);
        return bundle.accessToken();
    }

    private SearchResult<EngineRow> toSearchResult(GoogleSearchResponse response) {
        List<EngineRow> rows = response.files().stream().map(this::toEngineRow).toList();
        return new SearchResult<>(rows, response.nextPageToken());
    }

    private EngineRow toEngineRow(GoogleDriveFile file) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", file.id());
        m.put("source", source());
        m.put("title", file.name());
        m.put("updatedAt", file.updatedAt());
        m.put("url", file.webViewLink());
        m.put("owner", file.owner());
        m.put("createdAt", file.createdAt());
        m.put("modifiers", file.modifiers());
        return new EngineRow(m);
    }
}
