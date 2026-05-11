package org.emathp.connector.google.real;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Instant;
import org.emathp.auth.ConnectorAccount;
import org.emathp.oauth.GoogleOAuthService;
import org.emathp.oauth.TokenEncryptor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GoogleTokenStoreTest {

    private GoogleTokenStore store;
    private TokenEncryptor encryptor;

    @BeforeEach
    void setUp() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:tokentest;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        ds.setUser("sa");
        ds.setPassword("");
        store = new GoogleTokenStore(ds);
        store.ensureTable();
        store.deleteAllForTests();
        encryptor = TokenEncryptor.fromUtf8Passphrase("test-encryption-key-for-h2-store-xx");
    }

    @Test
    void persistAndLoadEncryptedTokens() throws SQLException {
        Instant now = Instant.parse("2026-05-01T12:00:00Z");
        ConnectorAccount a = new ConnectorAccount(
                null,
                "user-1",
                RealGoogleDriveConnector.ACCOUNT_TYPE,
                encryptor.encrypt("access-plain"),
                encryptor.encrypt("refresh-plain"),
                now.plusSeconds(3600),
                GoogleOAuthService.DRIVE_READONLY_SCOPE,
                now,
                now);

        store.upsert(a);

        ConnectorAccount loaded = store.find("user-1", RealGoogleDriveConnector.ACCOUNT_TYPE).orElseThrow();
        assertEquals("access-plain", encryptor.decrypt(loaded.accessTokenEncrypted()));
        assertEquals("refresh-plain", encryptor.decrypt(loaded.refreshTokenEncrypted()));
    }

    @Test
    void updateAfterRefresh() throws SQLException {
        Instant t0 = Instant.parse("2026-05-01T12:00:00Z");
        ConnectorAccount first =
                new ConnectorAccount(null, "u2", RealGoogleDriveConnector.ACCOUNT_TYPE, encryptor.encrypt("a1"), encryptor.encrypt("r1"), t0, "s", t0, t0);
        store.upsert(first);
        String id = store.find("u2", RealGoogleDriveConnector.ACCOUNT_TYPE).orElseThrow().id();

        Instant t1 = Instant.parse("2026-05-02T12:00:00Z");
        ConnectorAccount second =
                new ConnectorAccount(id, "u2", RealGoogleDriveConnector.ACCOUNT_TYPE, encryptor.encrypt("a2"), encryptor.encrypt("r2"), t1.plusSeconds(3600), "s", t0, t1);
        store.upsert(second);

        ConnectorAccount loaded = store.find("u2", RealGoogleDriveConnector.ACCOUNT_TYPE).orElseThrow();
        assertEquals("a2", encryptor.decrypt(loaded.accessTokenEncrypted()));
        assertTrue(loaded.expiresAt().isAfter(t0));
    }
}
