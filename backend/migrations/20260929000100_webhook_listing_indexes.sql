DROP INDEX IF EXISTS webhooks_user_id_created_at_idx;

CREATE INDEX webhooks_user_created_at_id_idx
    ON webhooks (user_id, created_at DESC, id DESC);

CREATE INDEX webhook_attempts_webhook_completed_at_id_idx
    ON webhook_attempts (webhook_id, completed_at DESC, id DESC);
