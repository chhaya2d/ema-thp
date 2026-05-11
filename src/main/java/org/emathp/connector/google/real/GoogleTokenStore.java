package org.emathp.connector.google.real;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.emathp.auth.ConnectorAccount;

/**
 * Plain JDBC persistence for {@link ConnectorAccount}. Compatible with PostgreSQL and H2 (tests).
 */
public final class GoogleTokenStore {

    private final DataSource dataSource;

    public GoogleTokenStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void ensureTable() throws SQLException {
        try (Connection c = dataSource.getConnection();
                var st = c.createStatement()) {
            st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS connector_accounts (
                      id VARCHAR(36) PRIMARY KEY,
                      user_id VARCHAR(256) NOT NULL,
                      connector_type VARCHAR(64) NOT NULL,
                      access_token_encrypted TEXT NOT NULL,
                      refresh_token_encrypted TEXT NOT NULL,
                      expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      scopes TEXT NOT NULL,
                      created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      UNIQUE (user_id, connector_type)
                    )""");
        }
    }

    public Optional<ConnectorAccount> find(String userId, String connectorType) throws SQLException {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        """
                        SELECT id, user_id, connector_type, access_token_encrypted,
                               refresh_token_encrypted, expires_at, scopes, created_at, updated_at
                        FROM connector_accounts
                        WHERE user_id = ? AND connector_type = ?""")) {
            ps.setString(1, userId);
            ps.setString(2, connectorType);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapRow(rs));
            }
        }
    }

    public void upsert(ConnectorAccount account) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                Optional<ConnectorAccount> existing =
                        findWithinConnection(c, account.userId(), account.connectorType());
                if (existing.isEmpty()) {
                    String id = account.id() != null ? account.id() : UUID.randomUUID().toString();
                    insert(c, withId(account, id));
                } else {
                    update(c, mergeForUpdate(account, existing.get()));
                }
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        }
    }

    private static ConnectorAccount mergeForUpdate(ConnectorAccount incoming, ConnectorAccount existing) {
        return new ConnectorAccount(
                existing.id(),
                incoming.userId(),
                incoming.connectorType(),
                incoming.accessTokenEncrypted(),
                incoming.refreshTokenEncrypted(),
                incoming.expiresAt(),
                incoming.scopes(),
                existing.createdAt(),
                incoming.updatedAt());
    }

    private Optional<ConnectorAccount> findWithinConnection(
            Connection c, String userId, String connectorType) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                """
                SELECT id, user_id, connector_type, access_token_encrypted,
                       refresh_token_encrypted, expires_at, scopes, created_at, updated_at
                FROM connector_accounts
                WHERE user_id = ? AND connector_type = ?""")) {
            ps.setString(1, userId);
            ps.setString(2, connectorType);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapRow(rs));
            }
        }
    }

    private static void insert(Connection c, ConnectorAccount a) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                """
                INSERT INTO connector_accounts (
                  id, user_id, connector_type, access_token_encrypted, refresh_token_encrypted,
                  expires_at, scopes, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?)""")) {
            ps.setString(1, a.id());
            ps.setString(2, a.userId());
            ps.setString(3, a.connectorType());
            ps.setString(4, a.accessTokenEncrypted());
            ps.setString(5, a.refreshTokenEncrypted());
            ps.setTimestamp(6, Timestamp.from(a.expiresAt()));
            ps.setString(7, a.scopes());
            ps.setTimestamp(8, Timestamp.from(a.createdAt()));
            ps.setTimestamp(9, Timestamp.from(a.updatedAt()));
            ps.executeUpdate();
        }
    }

    private static void update(Connection c, ConnectorAccount a) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                """
                UPDATE connector_accounts SET
                  access_token_encrypted = ?,
                  refresh_token_encrypted = ?,
                  expires_at = ?,
                  scopes = ?,
                  updated_at = ?
                WHERE id = ?""")) {
            ps.setString(1, a.accessTokenEncrypted());
            ps.setString(2, a.refreshTokenEncrypted());
            ps.setTimestamp(3, Timestamp.from(a.expiresAt()));
            ps.setString(4, a.scopes());
            ps.setTimestamp(5, Timestamp.from(a.updatedAt()));
            ps.setString(6, a.id());
            if (ps.executeUpdate() != 1) {
                throw new SQLException("UPDATE expected 1 row for id=" + a.id());
            }
        }
    }

    private static ConnectorAccount withId(ConnectorAccount a, String id) {
        return new ConnectorAccount(
                id,
                a.userId(),
                a.connectorType(),
                a.accessTokenEncrypted(),
                a.refreshTokenEncrypted(),
                a.expiresAt(),
                a.scopes(),
                a.createdAt(),
                a.updatedAt());
    }

    /** Visible for ConnectorAccount immutability updates from caller packages — prefer upsert. */
    static ConnectorAccount copyWithNewTokens(
            ConnectorAccount existing,
            String accessEnc,
            String refreshEnc,
            Instant expiresAt,
            Instant updatedAt) {
        return new ConnectorAccount(
                existing.id(),
                existing.userId(),
                existing.connectorType(),
                accessEnc,
                refreshEnc,
                expiresAt,
                existing.scopes(),
                existing.createdAt(),
                updatedAt);
    }

    private static ConnectorAccount mapRow(ResultSet rs) throws SQLException {
        return new ConnectorAccount(
                rs.getString("id"),
                rs.getString("user_id"),
                rs.getString("connector_type"),
                rs.getString("access_token_encrypted"),
                rs.getString("refresh_token_encrypted"),
                rs.getTimestamp("expires_at").toInstant(),
                rs.getString("scopes"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    /** Optional: wipe row for tests. */
    public void deleteAllForTests() throws SQLException {
        try (Connection c = dataSource.getConnection();
                var st = c.createStatement()) {
            st.execute("DELETE FROM connector_accounts");
        }
    }
}
