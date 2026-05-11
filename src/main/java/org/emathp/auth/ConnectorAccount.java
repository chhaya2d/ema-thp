package org.emathp.auth;

import java.time.Instant;

/**
 * Persisted linkage between an application user and a SaaS OAuth grant. Token fields are stored
 * encrypted at rest; this record is the in-memory shape after load / before save.
 */
public record ConnectorAccount(
        String id,
        String userId,
        String connectorType,
        String accessTokenEncrypted,
        String refreshTokenEncrypted,
        Instant expiresAt,
        String scopes,
        Instant createdAt,
        Instant updatedAt) {}
