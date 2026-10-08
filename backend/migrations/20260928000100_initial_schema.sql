CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}'::jsonb,
    api_key_hash TEXT NOT NULL UNIQUE CHECK (length(api_key_hash) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER users_set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    url TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_type TEXT NOT NULL DEFAULT 'json'
        CHECK (payload_type IN ('json', 'text')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX webhooks_user_id_created_at_idx
    ON webhooks (user_id, created_at DESC);

CREATE TABLE webhook_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id UUID NOT NULL REFERENCES webhooks (id) ON DELETE RESTRICT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL CHECK (completed_at >= started_at),
    http_status SMALLINT CHECK (http_status BETWEEN 100 AND 599),
    latency_ms BIGINT NOT NULL CHECK (latency_ms >= 0),
    response_body TEXT,
    error_message TEXT,
    CONSTRAINT webhook_attempts_has_status_or_error
        CHECK (http_status IS NOT NULL OR error_message IS NOT NULL)
);

CREATE FUNCTION reject_webhook_attempt_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'webhook_attempts is append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER webhook_attempts_reject_update_or_delete
BEFORE UPDATE OR DELETE ON webhook_attempts
FOR EACH ROW
EXECUTE FUNCTION reject_webhook_attempt_mutation();

CREATE TRIGGER webhook_attempts_reject_truncate
BEFORE TRUNCATE ON webhook_attempts
FOR EACH STATEMENT
EXECUTE FUNCTION reject_webhook_attempt_mutation();

CREATE TABLE webhook_deliveries (
    webhook_id UUID PRIMARY KEY REFERENCES webhooks (id) ON DELETE RESTRICT,
    max_attempts SMALLINT NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'delivering', 'retry_wait', 'delivered', 'failed')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE webhook_delivery_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id UUID NOT NULL REFERENCES webhook_deliveries (webhook_id) ON DELETE RESTRICT,
    attempt_number SMALLINT NOT NULL CHECK (attempt_number BETWEEN 1 AND 100),
    available_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed')),
    publish_token UUID,
    publish_lease_until TIMESTAMPTZ,
    last_published_at TIMESTAMPTZ,
    processing_token UUID,
    processing_lease_until TIMESTAMPTZ,
    completed_attempt_id UUID UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT webhook_delivery_jobs_attempt_unique UNIQUE (webhook_id, attempt_number),
    CONSTRAINT webhook_delivery_jobs_publish_lease_pair CHECK (
        (publish_token IS NULL) = (publish_lease_until IS NULL)
    ),
    CONSTRAINT webhook_delivery_jobs_processing_lease_pair CHECK (
        (processing_token IS NULL) = (processing_lease_until IS NULL)
    ),
    CONSTRAINT webhook_delivery_jobs_completion_state CHECK (
        (status = 'completed') = (completed_attempt_id IS NOT NULL)
    ),
    CONSTRAINT webhook_delivery_jobs_processing_lease_state CHECK (
        (status = 'processing') = (processing_token IS NOT NULL)
    )
);

CREATE INDEX webhook_delivery_jobs_pending_idx
    ON webhook_delivery_jobs (available_at, created_at)
    WHERE status = 'pending';

CREATE INDEX webhook_delivery_jobs_stale_processing_idx
    ON webhook_delivery_jobs (processing_lease_until)
    WHERE status = 'processing';
