use crate::webhook::model::{NewWebhookAttempt, WebhookRepositoryError};
use chrono::{DateTime, Duration, Utc};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use std::future::Future;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct QueueMessage {
    pub(crate) job_id: Uuid,
    pub(crate) webhook_id: Uuid,
    pub(crate) attempt_number: i16,
}

#[derive(Debug, Clone)]
pub(crate) struct ReservedJob {
    pub(crate) message: QueueMessage,
    pub(crate) available_at: DateTime<Utc>,
    pub(crate) publish_token: Uuid,
}

#[derive(Debug, Clone)]
pub(crate) struct DeliveryInput {
    pub(crate) message: QueueMessage,
    pub(crate) url: String,
    pub(crate) payload: String,
    pub(crate) payload_type: String,
    pub(crate) max_attempts: i16,
    pub(crate) token: Uuid,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ClaimResult {
    Acquired,
    AlreadyCompleted,
    Busy,
    NotReady,
    Missing,
}

pub(crate) trait DeliveryStore: Send + Sync + 'static {
    fn reserve_publications(
        &self,
        limit: i64,
        lease: Duration,
    ) -> impl Future<Output = Result<Vec<ReservedJob>, String>> + Send;

    fn finish_publication(
        &self,
        job_id: Uuid,
        token: Uuid,
        published: bool,
    ) -> impl Future<Output = Result<(), String>> + Send;

    fn claim<'a>(
        &'a self,
        message: &'a QueueMessage,
        token: Uuid,
        lease: Duration,
    ) -> impl Future<Output = Result<(ClaimResult, Option<DeliveryInput>), String>> + Send + 'a;

    fn renew_lease(
        &self,
        job_id: Uuid,
        token: Uuid,
        lease: Duration,
    ) -> impl Future<Output = Result<bool, String>> + Send;

    fn complete<'a>(
        &'a self,
        input: &'a DeliveryInput,
        attempt: NewWebhookAttempt,
        successful: bool,
        retryable: bool,
        next_available_at: Option<DateTime<Utc>>,
    ) -> impl Future<Output = Result<Option<Uuid>, String>> + Send + 'a;

    fn recover_expired_processing(
        &self,
        limit: i64,
    ) -> impl Future<Output = Result<u64, String>> + Send;
}

#[derive(Clone)]
pub(crate) struct PostgresDeliveryStore {
    pool: PgPool,
}

impl PostgresDeliveryStore {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(crate) async fn reserve_publications(
        &self,
        limit: i64,
        lease: Duration,
    ) -> Result<Vec<ReservedJob>, String> {
        let mut transaction = self.pool.begin().await.map_err(|error| error.to_string())?;
        let rows = sqlx::query(
            "WITH candidates AS (\
                 SELECT id FROM webhook_delivery_jobs \
                 WHERE status = 'pending' \
                   AND (publish_lease_until IS NULL OR publish_lease_until <= now()) \
                   AND (last_published_at IS NULL OR last_published_at <= now() - interval '60 seconds') \
                 ORDER BY available_at, created_at \
                 LIMIT $1 FOR UPDATE SKIP LOCKED\
             ) \
             UPDATE webhook_delivery_jobs AS jobs \
             SET publish_token = gen_random_uuid(), publish_lease_until = now() + $2 \
             FROM candidates WHERE jobs.id = candidates.id \
             RETURNING jobs.id, jobs.webhook_id, jobs.attempt_number, jobs.available_at, jobs.publish_token",
        )
        .bind(limit)
        .bind(lease)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        rows.into_iter().map(parse_reserved_job).collect()
    }

    pub(crate) async fn finish_publication(
        &self,
        job_id: Uuid,
        token: Uuid,
        published: bool,
    ) -> Result<(), String> {
        sqlx::query(
            "UPDATE webhook_delivery_jobs \
             SET publish_token = NULL, publish_lease_until = NULL, \
                 last_published_at = CASE WHEN $3 THEN now() ELSE last_published_at END \
             WHERE id = $1 AND publish_token = $2",
        )
        .bind(job_id)
        .bind(token)
        .bind(published)
        .execute(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) async fn claim(
        &self,
        message: &QueueMessage,
        token: Uuid,
        lease: Duration,
    ) -> Result<(ClaimResult, Option<DeliveryInput>), String> {
        let mut transaction = self.pool.begin().await.map_err(|error| error.to_string())?;
        let row = sqlx::query(
            "SELECT jobs.status, jobs.available_at, jobs.processing_lease_until, \
                    deliveries.max_attempts, webhooks.url, webhooks.payload, webhooks.payload_type \
             FROM webhook_delivery_jobs AS jobs \
             JOIN webhook_deliveries AS deliveries USING (webhook_id) \
             JOIN webhooks ON webhooks.id = jobs.webhook_id \
             WHERE jobs.id = $1 AND jobs.webhook_id = $2 AND jobs.attempt_number = $3 \
             FOR UPDATE OF jobs",
        )
        .bind(message.job_id)
        .bind(message.webhook_id)
        .bind(message.attempt_number)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
        let Some(row) = row else {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())?;
            return Ok((ClaimResult::Missing, None));
        };

        let status: String = row.try_get("status").map_err(|error| error.to_string())?;
        if status == "completed" {
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())?;
            return Ok((ClaimResult::AlreadyCompleted, None));
        }
        let available_at: DateTime<Utc> = row
            .try_get("available_at")
            .map_err(|error| error.to_string())?;
        if available_at > Utc::now() {
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())?;
            return Ok((ClaimResult::NotReady, None));
        }
        let processing_lease_until: Option<DateTime<Utc>> = row
            .try_get("processing_lease_until")
            .map_err(|error| error.to_string())?;
        if status == "processing" && processing_lease_until.is_some_and(|until| until > Utc::now())
        {
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())?;
            return Ok((ClaimResult::Busy, None));
        }

        let lease_until = Utc::now() + lease;
        sqlx::query(
            "UPDATE webhook_delivery_jobs \
             SET status = 'processing', processing_token = $2, processing_lease_until = $3, \
                 publish_token = NULL, publish_lease_until = NULL \
             WHERE id = $1",
        )
        .bind(message.job_id)
        .bind(token)
        .bind(lease_until)
        .execute(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
        sqlx::query(
            "UPDATE webhook_deliveries SET status = 'delivering', updated_at = now() \
             WHERE webhook_id = $1 AND status IN ('pending', 'retry_wait', 'delivering')",
        )
        .bind(message.webhook_id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;

        let input = DeliveryInput {
            message: message.clone(),
            url: row.try_get("url").map_err(|error| error.to_string())?,
            payload: row.try_get("payload").map_err(|error| error.to_string())?,
            payload_type: row
                .try_get("payload_type")
                .map_err(|error| error.to_string())?,
            max_attempts: row
                .try_get("max_attempts")
                .map_err(|error| error.to_string())?,
            token,
        };
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok((ClaimResult::Acquired, Some(input)))
    }

    pub(crate) async fn renew_lease(
        &self,
        job_id: Uuid,
        token: Uuid,
        lease: Duration,
    ) -> Result<bool, String> {
        let result = sqlx::query(
            "UPDATE webhook_delivery_jobs SET processing_lease_until = now() + $3 \
             WHERE id = $1 AND processing_token = $2 AND status = 'processing' \
               AND processing_lease_until > now()",
        )
        .bind(job_id)
        .bind(token)
        .bind(lease)
        .execute(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn complete(
        &self,
        input: &DeliveryInput,
        attempt: NewWebhookAttempt,
        successful: bool,
        retryable: bool,
        next_available_at: Option<DateTime<Utc>>,
    ) -> Result<Option<Uuid>, String> {
        let mut transaction = self.pool.begin().await.map_err(|error| error.to_string())?;
        let ownership = sqlx::query_scalar::<_, Uuid>(
            "SELECT webhook_id FROM webhook_delivery_jobs \
             WHERE id = $1 AND processing_token = $2 AND status = 'processing' \
               AND processing_lease_until > now() FOR UPDATE",
        )
        .bind(input.message.job_id)
        .bind(input.token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
        if ownership != Some(input.message.webhook_id) {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(None);
        }

        let attempt_id = insert_attempt(&mut transaction, attempt)
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query(
            "UPDATE webhook_delivery_jobs SET status = 'completed', completed_attempt_id = $3, \
             processing_token = NULL, processing_lease_until = NULL WHERE id = $1 AND processing_token = $2",
        )
            .bind(input.message.job_id)
            .bind(input.token)
            .bind(attempt_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| error.to_string())?;

        let (delivery_status, next_job_id) = if successful {
            ("delivered", None)
        } else if retryable && input.message.attempt_number < input.max_attempts {
            let available_at =
                next_available_at.ok_or_else(|| "retry needs a schedule".to_owned())?;
            let next_job_id = sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO webhook_delivery_jobs \
                 (webhook_id, attempt_number, available_at, status) \
                 VALUES ($1, $2, $3, 'pending') RETURNING id",
            )
            .bind(input.message.webhook_id)
            .bind(input.message.attempt_number + 1)
            .bind(available_at)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| error.to_string())?;
            ("retry_wait", Some(next_job_id))
        } else {
            ("failed", None)
        };
        sqlx::query(
            "UPDATE webhook_deliveries SET status = $2, updated_at = now() WHERE webhook_id = $1",
        )
        .bind(input.message.webhook_id)
        .bind(delivery_status)
        .execute(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(next_job_id)
    }

    pub(crate) async fn recover_expired_processing(&self, limit: i64) -> Result<u64, String> {
        let result = sqlx::query(
            "WITH expired AS (\
                 SELECT id, webhook_id FROM webhook_delivery_jobs \
                 WHERE status = 'processing' AND processing_lease_until <= now() \
                 ORDER BY processing_lease_until LIMIT $1 FOR UPDATE SKIP LOCKED\
             ), reset_jobs AS (\
                 UPDATE webhook_delivery_jobs AS jobs \
                 SET status = 'pending', processing_token = NULL, processing_lease_until = NULL \
                 FROM expired WHERE jobs.id = expired.id RETURNING expired.webhook_id\
             ) \
             UPDATE webhook_deliveries AS deliveries SET status = 'retry_wait', updated_at = now() \
             FROM reset_jobs WHERE deliveries.webhook_id = reset_jobs.webhook_id \
               AND deliveries.status = 'delivering'",
        )
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(result.rows_affected())
    }
}

impl DeliveryStore for PostgresDeliveryStore {
    async fn reserve_publications(
        &self,
        limit: i64,
        lease: Duration,
    ) -> Result<Vec<ReservedJob>, String> {
        PostgresDeliveryStore::reserve_publications(self, limit, lease).await
    }

    async fn finish_publication(
        &self,
        job_id: Uuid,
        token: Uuid,
        published: bool,
    ) -> Result<(), String> {
        PostgresDeliveryStore::finish_publication(self, job_id, token, published).await
    }

    async fn claim(
        &self,
        message: &QueueMessage,
        token: Uuid,
        lease: Duration,
    ) -> Result<(ClaimResult, Option<DeliveryInput>), String> {
        PostgresDeliveryStore::claim(self, message, token, lease).await
    }

    async fn renew_lease(
        &self,
        job_id: Uuid,
        token: Uuid,
        lease: Duration,
    ) -> Result<bool, String> {
        PostgresDeliveryStore::renew_lease(self, job_id, token, lease).await
    }

    async fn complete(
        &self,
        input: &DeliveryInput,
        attempt: NewWebhookAttempt,
        successful: bool,
        retryable: bool,
        next_available_at: Option<DateTime<Utc>>,
    ) -> Result<Option<Uuid>, String> {
        PostgresDeliveryStore::complete(
            self,
            input,
            attempt,
            successful,
            retryable,
            next_available_at,
        )
        .await
    }

    async fn recover_expired_processing(&self, limit: i64) -> Result<u64, String> {
        PostgresDeliveryStore::recover_expired_processing(self, limit).await
    }
}

async fn insert_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    attempt: NewWebhookAttempt,
) -> Result<Uuid, WebhookRepositoryError> {
    sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO webhook_attempts \
         (webhook_id, started_at, completed_at, http_status, latency_ms, response_body, error_message) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind(attempt.webhook_id())
    .bind(attempt.started_at())
    .bind(attempt.completed_at())
    .bind(attempt.http_status())
    .bind(attempt.latency_ms())
    .bind(attempt.response_body())
    .bind(attempt.error_message())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| WebhookRepositoryError::Database(error.to_string()))
}

fn parse_reserved_job(row: PgRow) -> Result<ReservedJob, String> {
    Ok(ReservedJob {
        message: QueueMessage {
            job_id: row.try_get("id").map_err(|error| error.to_string())?,
            webhook_id: row
                .try_get("webhook_id")
                .map_err(|error| error.to_string())?,
            attempt_number: row
                .try_get("attempt_number")
                .map_err(|error| error.to_string())?,
        },
        available_at: row
            .try_get("available_at")
            .map_err(|error| error.to_string())?,
        publish_token: row
            .try_get("publish_token")
            .map_err(|error| error.to_string())?,
    })
}

#[cfg(test)]
#[sqlx::test(migrations = "./migrations")]
async fn failed_attempt_and_next_outbox_entry_commit_together(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    use crate::webhook::{
        model::{NewWebhook, PayloadType},
        repository::WebhookRepository,
    };

    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash) VALUES ('worker integration user', $1) RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let webhook_id = crate::webhook::repository::PostgresWebhookRepository::new(pool.clone())
        .insert(NewWebhook {
            user_id,
            url: "https://example.com/hook".to_owned(),
            payload: "{}".to_owned(),
            payload_type: PayloadType::Json,
        })
        .await
        .expect("initial webhook and outbox job should be committed");
    let store = PostgresDeliveryStore::new(pool.clone());
    let job: (Uuid, i16) = sqlx::query_as(
        "SELECT id, attempt_number FROM webhook_delivery_jobs WHERE webhook_id = $1",
    )
    .bind(webhook_id)
    .fetch_one(&pool)
    .await?;
    let token = Uuid::new_v4();
    sqlx::query(
        "UPDATE webhook_delivery_jobs SET status = 'processing', processing_token = $2, \
         processing_lease_until = now() + interval '2 minutes' WHERE id = $1",
    )
    .bind(job.0)
    .bind(token)
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE webhook_deliveries SET status = 'delivering' WHERE webhook_id = $1")
        .bind(webhook_id)
        .execute(&pool)
        .await?;
    let input = DeliveryInput {
        message: QueueMessage {
            job_id: job.0,
            webhook_id,
            attempt_number: job.1,
        },
        url: "https://example.com/hook".to_owned(),
        payload: "{}".to_owned(),
        payload_type: "json".to_owned(),
        max_attempts: 5,
        token,
    };
    let now = Utc::now();
    let audit = NewWebhookAttempt::new(
        webhook_id,
        now,
        now,
        Some(503),
        12,
        Some("unavailable".to_owned()),
        Some("retryable status".to_owned()),
    )
    .expect("valid audit data");
    let next_available_at = now + Duration::seconds(5);

    let next_job_id = store
        .complete(&input, audit, false, true, Some(next_available_at))
        .await
        .expect("attempt and next job should commit")
        .expect("retry should be scheduled");
    let next_job: (Uuid, i16, String) = sqlx::query_as(
        "SELECT webhook_id, attempt_number, status FROM webhook_delivery_jobs WHERE id = $1",
    )
    .bind(next_job_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(next_job, (webhook_id, 2, "pending".to_owned()));

    let old_job_status: String =
        sqlx::query_scalar("SELECT status FROM webhook_delivery_jobs WHERE id = $1")
            .bind(job.0)
            .fetch_one(&pool)
            .await?;
    assert_eq!(old_job_status, "completed");
    let delivery_status: String =
        sqlx::query_scalar("SELECT status FROM webhook_deliveries WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(delivery_status, "retry_wait");
    let attempts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_attempts WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(attempts, 1);
    Ok(())
}

#[cfg(test)]
#[sqlx::test(migrations = "./migrations")]
async fn final_retryable_attempt_fails_delivery_without_creating_another_job(
    pool: PgPool,
) -> Result<(), sqlx::Error> {
    use crate::webhook::{
        model::{NewWebhook, PayloadType},
        repository::WebhookRepository,
    };

    let user_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (name, api_key_hash, configuration) \
         VALUES ('retry limit user', $1, '{\"delivery\":{\"max_attempts\":1}}') RETURNING id",
    )
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&pool)
    .await?;
    let webhook_id = crate::webhook::repository::PostgresWebhookRepository::new(pool.clone())
        .insert(NewWebhook {
            user_id,
            url: "https://example.com/hook".to_owned(),
            payload: "{}".to_owned(),
            payload_type: PayloadType::Json,
        })
        .await
        .expect("create webhook and first job");
    let (job_id, attempt_number): (Uuid, i16) = sqlx::query_as(
        "SELECT id, attempt_number FROM webhook_delivery_jobs WHERE webhook_id = $1",
    )
    .bind(webhook_id)
    .fetch_one(&pool)
    .await?;
    let token = Uuid::new_v4();
    sqlx::query(
        "UPDATE webhook_delivery_jobs SET status = 'processing', processing_token = $2, \
         processing_lease_until = now() + interval '2 minutes' WHERE id = $1",
    )
    .bind(job_id)
    .bind(token)
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE webhook_deliveries SET status = 'delivering' WHERE webhook_id = $1")
        .bind(webhook_id)
        .execute(&pool)
        .await?;
    let input = DeliveryInput {
        message: QueueMessage {
            job_id,
            webhook_id,
            attempt_number,
        },
        url: "https://example.com/hook".to_owned(),
        payload: "{}".to_owned(),
        payload_type: "json".to_owned(),
        max_attempts: 1,
        token,
    };
    let now = Utc::now();
    let audit = NewWebhookAttempt::new(
        webhook_id,
        now,
        now,
        Some(503),
        12,
        None,
        Some("retry limit reached".to_owned()),
    )
    .expect("valid failed attempt");
    let store = PostgresDeliveryStore::new(pool.clone());

    let next_job = store
        .complete(&input, audit, false, true, None)
        .await
        .expect("final attempt should commit");
    assert_eq!(next_job, None);
    let (status, job_count): (String, i64) = sqlx::query_as(
        "SELECT deliveries.status, count(jobs.id) \
         FROM webhook_deliveries AS deliveries \
         JOIN webhook_delivery_jobs AS jobs USING (webhook_id) \
         WHERE deliveries.webhook_id = $1 GROUP BY deliveries.status",
    )
    .bind(webhook_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(status, "failed");
    assert_eq!(job_count, 1);
    Ok(())
}
