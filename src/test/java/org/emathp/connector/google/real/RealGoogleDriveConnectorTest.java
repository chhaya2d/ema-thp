package org.emathp.connector.google.real;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.emathp.auth.ConnectorAccount;
import org.emathp.auth.UserContext;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;
import org.emathp.oauth.GoogleOAuthService;
import org.emathp.oauth.TokenEncryptor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RealGoogleDriveConnectorTest {

    private GoogleTokenStore store;
    private TokenEncryptor encryptor;

    @BeforeEach
    void setUp() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:realgoog;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        ds.setUser("sa");
        ds.setPassword("");
        store = new GoogleTokenStore(storeDataSource(ds));
        store.ensureTable();
        store.deleteAllForTests();
        encryptor = TokenEncryptor.fromUtf8Passphrase("real-connector-test-secret-key-!!");
    }

    /** Wrap so each test class gets a fresh in-memory name if needed */
    private static DataSource storeDataSource(JdbcDataSource ds) {
        return ds;
    }

    @Test
    void searchUsesRefreshedTokensAndNormalizesResources() throws Exception {
        Instant now = Instant.now();
        ConnectorAccount row = new ConnectorAccount(
                null,
                "alice",
                RealGoogleDriveConnector.ACCOUNT_TYPE,
                encryptor.encrypt("opaque-access"),
                encryptor.encrypt("opaque-refresh"),
                now.plusSeconds(10_000),
                GoogleOAuthService.DRIVE_READONLY_SCOPE,
                now,
                now);
        store.upsert(row);

        GoogleOAuthService oauth = new GoogleOAuthService("id", "sec", "http://localhost/cb");

        GoogleDriveRpc rpc =
                new GoogleDriveRpc() {
                    @Override
                    public GoogleSearchResponse listFiles(String accessToken, GoogleSearchRequest request)
                            throws Exception {
                        assertEquals("opaque-access", accessToken);
                        var file =
                                new GoogleDriveFile(
                                        "id1",
                                        "Doc",
                                        "o",
                                        List.of(),
                                        now,
                                        now,
                                        "https://example.com/x");
                        return new GoogleSearchResponse(List.of(file), null);
                    }
                };

        RealGoogleDriveConnector conn = new RealGoogleDriveConnector(store, encryptor, oauth, rpc);
        ConnectorQuery q =
                new ConnectorQuery(List.of("title"), null, List.of(), null, null);
        SearchResult<EngineRow> result = conn.search(new UserContext("alice"), q);
        assertEquals(1, result.rows().size());
        assertEquals("Doc", result.rows().getFirst().fields().get("title"));
        assertEquals(RealGoogleDriveConnector.ACCOUNT_TYPE, result.rows().getFirst().fields().get("source"));
    }

    @Test
    void requiresUserId() {
        RealGoogleDriveConnector conn =
                new RealGoogleDriveConnector(
                        store,
                        encryptor,
                        new GoogleOAuthService("a", "b", "c"),
                        (t, r) -> new GoogleSearchResponse(List.of(), null));
        var q = new ConnectorQuery(List.of(), null, List.of(), null, null);
        assertThrows(IllegalArgumentException.class, () -> conn.search(UserContext.anonymous(), q));
    }
}
