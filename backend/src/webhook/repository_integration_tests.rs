use super::{PostgresWebhookRepository, WebhookRepository};
use crate::webhook::model::{NewWebhookAttempt, WebhookRepositoryError};
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use uuid::Uuid;

async fn create_webhook(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('attempt test user', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(pool)
    .await?;

    sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO webhooks (user_id, url, payload) VALUES ($1, 'https://example.com/hook', '{}') RETURNING id",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}

fn completed_attempt(webhook_id: Uuid) -> NewWebhookAttempt {
    let started_at = DateTime::from_timestamp(1_000, 0).expect("valid test timestamp");
    NewWebhookAttempt::new(webhook_id, started_at, started_at, Some(200), 0, None, None)
        .expect("valid test attempt")
}

#[derive(sqlx::FromRow)]
struct StoredAttempt {
    webhook_id: Uuid,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    http_status: Option<i16>,
    latency_ms: i64,
    response_body: Option<String>,
    error_message: Option<String>,
}

#[sqlx::test(migrations = "./migrations")]
async fn persists_completed_attempts_and_allows_repeated_inputs(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let webhook_id = create_webhook(&pool).await?;
    let repository = PostgresWebhookRepository::new(pool.clone());
    let started_at = DateTime::from_timestamp(1_000, 123_000_000).expect("valid test timestamp");
    let completed_at = started_at + TimeDelta::seconds(2);
    let mut ids = Vec::new();

    for (status, body, error) in [
        (Some(200), Some("delivered"), None),
        (None, None, Some("connection timed out")),
        (Some(503), Some("unavailable"), Some("delivery failed")),
        (None, Some(""), Some("")),
        (Some(200), Some("delivered"), None),
    ] {
        let attempt = NewWebhookAttempt::new(
            webhook_id,
            started_at,
            completed_at,
            status,
            17,
            body.map(str::to_owned),
            error.map(str::to_owned),
        )
        .expect("valid completed attempt");

        let id = repository
            .add_attempt(attempt)
            .await
            .expect("attempt should be inserted");
        assert!(!id.is_nil());
        assert!(!ids.contains(&id), "each insertion generates a new UUID");
        ids.push(id);

        let row = sqlx::query_as::<_, StoredAttempt>(
            "SELECT webhook_id, started_at, completed_at, http_status, latency_ms, response_body, error_message \
             FROM webhook_attempts WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(row.webhook_id, webhook_id);
        assert_eq!(row.started_at, started_at);
        assert_eq!(row.completed_at, completed_at);
        assert_eq!(row.http_status, status);
        assert_eq!(
            row.latency_ms, 17,
            "latency is provided, not derived from timestamps"
        );
        assert_eq!(row.response_body.as_deref(), body);
        assert_eq!(row.error_message.as_deref(), error);
    }

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_attempts WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 5);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn reports_missing_webhook_without_inserting_attempt(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let repository = PostgresWebhookRepository::new(pool.clone());
    let error = repository
        .add_attempt(completed_attempt(Uuid::new_v4()))
        .await
        .expect_err("webhook does not exist");

    assert_eq!(error, WebhookRepositoryError::WebhookNotFound);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM webhook_attempts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn lists_webhooks_by_creation_cursor_with_latest_attempt_and_count(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('list test user', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let other_user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('other list test user', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let created_at = DateTime::from_timestamp(2_000, 0).expect("valid timestamp");
    let older_created_at = DateTime::from_timestamp(1_999, 0).expect("valid timestamp");
    let first_id = insert_list_webhook(&pool, user_id, created_at, "pending").await?;
    let second_id = insert_list_webhook(&pool, user_id, created_at, "pending").await?;
    let newest_webhook_id = first_id.max(second_id);
    sqlx::query("UPDATE webhook_deliveries SET status = 'delivering' WHERE webhook_id = $1")
        .bind(newest_webhook_id)
        .execute(&pool)
        .await?;
    let third_id = insert_list_webhook(&pool, user_id, older_created_at, "retry_wait").await?;
    insert_list_webhook(&pool, other_user_id, created_at, "failed").await?;

    let older_attempt = DateTime::from_timestamp(2_100, 0).expect("valid timestamp");
    let newest_attempt = DateTime::from_timestamp(2_101, 0).expect("valid timestamp");
    insert_list_attempt(&pool, newest_webhook_id, older_attempt, Some(200), None).await?;
    insert_list_attempt(
        &pool,
        newest_webhook_id,
        newest_attempt,
        Some(503),
        Some("temporarily unavailable"),
    )
    .await?;

    let repository = PostgresWebhookRepository::new(pool.clone());
    let first_page = repository
        .list(user_id, None, 2)
        .await
        .expect("first page should load");

    assert_eq!(first_page.items.len(), 2);
    assert_eq!(first_page.items[0].id, newest_webhook_id);
    assert_eq!(first_page.items[1].id, first_id.min(second_id));
    assert_eq!(first_page.items[0].attempt_count, 2);
    assert_eq!(first_page.items[1].attempt_count, 0);
    assert_eq!(first_page.items[0].status, "delivering");
    assert_eq!(first_page.items[0].http_status, Some(503));
    assert_eq!(
        first_page.items[0].error_message.as_deref(),
        Some("temporarily unavailable")
    );
    assert_eq!(first_page.items[0].completed_at, Some(newest_attempt));
    assert_eq!(first_page.next_cursor, Some(first_page.items[1].id));

    let final_page = repository
        .list(user_id, first_page.next_cursor, 2)
        .await
        .expect("final page should load");
    assert_eq!(final_page.items.len(), 1);
    assert_eq!(final_page.items[0].id, third_id);
    assert_eq!(final_page.items[0].http_status, None);
    assert_eq!(final_page.items[0].error_message, None);
    assert_eq!(final_page.items[0].completed_at, None);
    assert_eq!(final_page.next_cursor, None);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn rejects_cursor_from_another_user(pool: PgPool) -> Result<(), sqlx::Error> {
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('cursor owner', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let other_user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('cursor outsider', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let cursor = insert_list_webhook(
        &pool,
        other_user_id,
        DateTime::from_timestamp(2_000, 0).expect("valid timestamp"),
        "pending",
    )
    .await?;

    let error = PostgresWebhookRepository::new(pool)
        .list(user_id, Some(cursor), 20)
        .await
        .expect_err("foreign cursor should be rejected");
    assert_eq!(error, WebhookRepositoryError::CursorNotFound);
    Ok(())
}

async fn insert_list_webhook(
    pool: &PgPool,
    user_id: Uuid,
    created_at: DateTime<Utc>,
    delivery_status: &str,
) -> Result<Uuid, sqlx::Error> {
    let webhook_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO webhooks (user_id, url, payload, created_at) \
         VALUES ($1, 'https://example.com/list', '{}', $2) RETURNING id",
    )
    .bind(user_id)
    .bind(created_at)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO webhook_deliveries (webhook_id, max_attempts, status) VALUES ($1, 5, $2)",
    )
    .bind(webhook_id)
    .bind(delivery_status)
    .execute(pool)
    .await?;
    Ok(webhook_id)
}

async fn insert_list_attempt(
    pool: &PgPool,
    webhook_id: Uuid,
    completed_at: DateTime<Utc>,
    http_status: Option<i16>,
    error_message: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO webhook_attempts \
         (webhook_id, started_at, completed_at, http_status, latency_ms, error_message) \
         VALUES ($1, $2, $2, $3, 0, $4)",
    )
    .bind(webhook_id)
    .bind(completed_at)
    .bind(http_status)
    .bind(error_message)
    .execute(pool)
    .await?;
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn creates_webhook_delivery_and_initial_outbox_entry_atomically(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash, configuration) \
         VALUES ('delivery test user', $1, '{\"delivery\":{\"max_attempts\":4}}') RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let repository = PostgresWebhookRepository::new(pool.clone());
    let webhook_id = repository
        .insert(crate::webhook::model::NewWebhook {
            user_id,
            url: "https://example.com/hook".to_owned(),
            payload: "{}".to_owned(),
            payload_type: crate::webhook::model::PayloadType::Json,
        })
        .await
        .expect("webhook and first delivery job should be created");

    let delivery: (i16, String) =
        sqlx::query_as("SELECT max_attempts, status FROM webhook_deliveries WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(delivery, (4, "pending".to_owned()));

    let first_job: (i16, String) = sqlx::query_as(
        "SELECT attempt_number, status FROM webhook_delivery_jobs WHERE webhook_id = $1",
    )
    .bind(webhook_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(first_job, (1, "pending".to_owned()));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn rejects_invalid_delivery_configuration_without_persisting_webhook(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash, configuration) \
         VALUES ('invalid delivery test user', $1, '{\"delivery\":{\"max_attempts\":0}}') RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let repository = PostgresWebhookRepository::new(pool.clone());
    let error = repository
        .insert(crate::webhook::model::NewWebhook {
            user_id,
            url: "https://example.com/hook".to_owned(),
            payload: "{}".to_owned(),
            payload_type: crate::webhook::model::PayloadType::Json,
        })
        .await
        .expect_err("invalid retry configuration must reject creation");
    assert!(matches!(
        error,
        WebhookRepositoryError::InvalidDeliveryConfiguration(_)
    ));

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM webhooks WHERE user_id = $1")
        .bind(user_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn migration_chain_preserves_attempt_schema_and_can_be_reapplied(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let migration_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success")
            .fetch_one(&pool)
            .await?;
    assert!(
        migration_count >= 2,
        "fresh database applies the baseline and incremental migrations"
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'webhook_attempts' AND column_name = 'attempt_number'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 0);

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migration checksum should match");
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn database_rejects_invalid_attempts_when_rust_validation_is_bypassed(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    let webhook_id = create_webhook(&pool).await?;
    let started_at = DateTime::from_timestamp(1_000, 0).expect("valid test timestamp");

    for (completed_at, status, latency) in [
        (started_at - TimeDelta::seconds(1), Some(200_i16), 0_i64),
        (started_at, Some(99), 0),
        (started_at, Some(600), 0),
        (started_at, Some(200), -1),
        (started_at, None, 0),
    ] {
        let error = sqlx::query(
            "INSERT INTO webhook_attempts (webhook_id, started_at, completed_at, http_status, latency_ms) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(webhook_id).bind(started_at).bind(completed_at).bind(status).bind(latency)
        .execute(&pool)
        .await.expect_err("database check constraint should reject invalid input");

        assert_eq!(
            error
                .as_database_error()
                .expect("database error")
                .code()
                .as_deref(),
            Some("23514")
        );
    }
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn rejects_attempt_updates_deletes_and_truncation(pool: PgPool) -> Result<(), sqlx::Error> {
    let webhook_id = create_webhook(&pool).await?;
    let repository = PostgresWebhookRepository::new(pool.clone());
    repository
        .add_attempt(completed_attempt(webhook_id))
        .await
        .expect("insert attempt");

    for statement in [
        "UPDATE webhook_attempts SET http_status = 500",
        "DELETE FROM webhook_attempts",
        "TRUNCATE webhook_attempts",
    ] {
        let error = sqlx::query(statement)
            .execute(&pool)
            .await
            .expect_err("attempt history is append-only");
        assert_eq!(
            error
                .as_database_error()
                .expect("database error")
                .code()
                .as_deref(),
            Some("55000")
        );
    }

    let status: Option<i16> = sqlx::query_scalar("SELECT http_status FROM webhook_attempts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, Some(200));
    Ok(())
}
