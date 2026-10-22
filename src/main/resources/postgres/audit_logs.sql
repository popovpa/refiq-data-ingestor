-- Applied by the infrastructure/migration contour before data-ingest-audit starts.
-- The runtime service does not execute this script.

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    action VARCHAR(64) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    account_id VARCHAR(128),
    actor_type VARCHAR(64) NOT NULL,
    actor_id VARCHAR(128),
    entity_type VARCHAR(128) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    request_id VARCHAR(128),
    correlation_id VARCHAR(128),
    ip_address VARCHAR(64),
    user_agent TEXT,
    changed_fields JSONB,
    before_data JSONB,
    after_data JSONB,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS audit_logs_occurred_at_idx
    ON audit_logs (occurred_at DESC);

CREATE INDEX IF NOT EXISTS audit_logs_entity_idx
    ON audit_logs (entity_type, entity_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS audit_logs_account_idx
    ON audit_logs (account_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS audit_logs_actor_idx
    ON audit_logs (actor_id, occurred_at DESC);
