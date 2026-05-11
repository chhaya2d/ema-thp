-- PostgreSQL / H2 compatible schema for OAuth token persistence.
-- NOTE: in production use migrations; this file documents the shape used by GoogleTokenStore.

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
);
