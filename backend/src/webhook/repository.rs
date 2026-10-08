use super::model::{
    NewWebhook, NewWebhookAttempt, WebhookAttemptDetail, WebhookAttemptResult, WebhookDetail,
    WebhookListItem, WebhookListPage, WebhookRepositoryError,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{types::Json, PgPool};
use std::future::Future;
use uuid::Uuid;

pub(crate) trait WebhookRepository {
    fn insert(
        &self,
        webhook: NewWebhook,
    ) -> impl Future<Output = Result<Uuid, WebhookRepositoryError>> + Send;

    #[allow(
        dead_code,
        reason = "Attempt recording is not connected to a delivery worker yet"
    )]
    fn add_attempt(
        &self,
        attempt: NewWebhookAttempt,
    ) -> impl Future<Output = Result<Uuid, WebhookRepositoryError>> + Send;

    fn list(
        &self,
        user_id: Uuid,
        cursor: Option<Uuid>,
        count: i64,
    ) -> impl Future<Output = Result<WebhookListPage, WebhookRepositoryError>> + Send;

    fn get(
        &self,
        user_id: Uuid,
        webhook_id: Uuid,
    ) -> impl Future<Output = Result<WebhookDetail, WebhookRepositoryError>> + Send;
}

pub(crate) struct PostgresWebhookRepository {
    pool: PgPool,
}

impl PostgresWebhookRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl WebhookRepository for PostgresWebhookRepository {
    async fn insert(&self, webhook: NewWebhook) -> Result<Uuid, WebhookRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(map_database_error)?;
        let configuration = sqlx::query_scalar::<_, Json<Value>>(
            "SELECT configuration FROM users WHERE id = $1 FOR SHARE",
        )
        .bind(webhook.user_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database_error)?;
        let Some(Json(configuration)) = configuration else {
            return Err(WebhookRepositoryError::UserNotFound);
        };
        let max_attempts = crate::webhook_delivery::policy::delivery_configuration(&configuration)
            .map_err(|error| {
                WebhookRepositoryError::InvalidDeliveryConfiguration(error.to_owned())
            })?;

        let webhook_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO webhooks (user_id, url, payload, payload_type) \
             VALUES ($1, $2, $3, $4) \
             RETURNING id",
        )
        .bind(webhook.user_id)
        .bind(webhook.url)
        .bind(webhook.payload)
        .bind(webhook.payload_type.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_database_error)?;

        sqlx::query(
            "INSERT INTO webhook_deliveries (webhook_id, max_attempts, status) \
             VALUES ($1, $2, 'pending')",
        )
        .bind(webhook_id)
        .bind(max_attempts)
        .execute(&mut *transaction)
        .await
        .map_err(map_database_error)?;
        sqlx::query(
            "INSERT INTO webhook_delivery_jobs (webhook_id, attempt_number, available_at, status) \
             VALUES ($1, 1, now(), 'pending')",
        )
        .bind(webhook_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_database_error)?;

        transaction.commit().await.map_err(map_database_error)?;
        Ok(webhook_id)
    }

    async fn add_attempt(
        &self,
        attempt: NewWebhookAttempt,
    ) -> Result<Uuid, WebhookRepositoryError> {
        sqlx::query_scalar!(
            "INSERT INTO webhook_attempts \
             (webhook_id, started_at, completed_at, http_status, latency_ms, response_body, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             RETURNING id",
            attempt.webhook_id(),
            attempt.started_at(),
            attempt.completed_at(),
            attempt.http_status(),
            attempt.latency_ms(),
            attempt.response_body(),
            attempt.error_message(),
        )
        .fetch_one(&self.pool)
        .await
        .map_err(map_database_error)
    }

    async fn list(
        &self,
        user_id: Uuid,
        cursor: Option<Uuid>,
        count: i64,
    ) -> Result<WebhookListPage, WebhookRepositoryError> {
        let cursor_position = if let Some(cursor) = cursor {
            sqlx::query_as::<_, (DateTime<Utc>, Uuid)>(
                "SELECT created_at, id FROM webhooks WHERE id = $1 AND user_id = $2",
            )
            .bind(cursor)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_database_error)?
            .ok_or(WebhookRepositoryError::CursorNotFound)?
            .into()
        } else {
            None
        };

        let rows = sqlx::query!(
            "SELECT webhooks.id, webhooks.url, webhooks.created_at, \
                    deliveries.status AS \"status!\", \
                    (SELECT count(*)::bigint FROM webhook_attempts \
                     WHERE webhook_id = webhooks.id) AS \"attempt_count!\", \
                    latest.http_status, latest.error_message, \
                    latest.completed_at AS \"completed_at?\" \
             FROM webhooks \
             JOIN webhook_deliveries AS deliveries ON deliveries.webhook_id = webhooks.id \
             LEFT JOIN LATERAL ( \
                 SELECT http_status, error_message, completed_at \
                 FROM webhook_attempts WHERE webhook_id = webhooks.id \
                 ORDER BY completed_at DESC, id DESC LIMIT 1 \
             ) AS latest ON true \
             WHERE webhooks.user_id = $1 \
               AND ($2::timestamptz IS NULL OR \
                    (webhooks.created_at, webhooks.id) < ($2, $3::uuid)) \
             ORDER BY webhooks.created_at DESC, webhooks.id DESC \
             LIMIT $4",
            user_id,
            cursor_position.map(|position| position.0),
            cursor_position.map(|position| position.1),
            count + 1,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_database_error)?;

        let mut items = rows
            .into_iter()
            .map(|row| WebhookListItem {
                id: row.id,
                url: row.url,
                created_at: row.created_at,
                status: row.status,
                attempt_count: row.attempt_count,
                http_status: row.http_status,
                error_message: row.error_message,
                completed_at: row.completed_at,
            })
            .collect::<Vec<_>>();
        let has_more = items.len() > count as usize;
        if has_more {
            items.truncate(count as usize);
        }
        let next_cursor = has_more.then(|| items.last().map(|item| item.id)).flatten();

        Ok(WebhookListPage { items, next_cursor })
    }

    async fn get(
        &self,
        user_id: Uuid,
        webhook_id: Uuid,
    ) -> Result<WebhookDetail, WebhookRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(map_database_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(map_database_error)?;

        let webhook = sqlx::query_as::<_, WebhookDetailRow>(
            "SELECT webhooks.id, webhooks.created_at, webhooks.url, webhooks.payload, \
                    webhooks.payload_type, deliveries.status, deliveries.max_attempts, \
                    pending.available_at AS next_attempt_at \
             FROM webhooks \
             JOIN webhook_deliveries AS deliveries ON deliveries.webhook_id = webhooks.id \
             LEFT JOIN LATERAL ( \
                 SELECT available_at FROM webhook_delivery_jobs \
                 WHERE webhook_id = webhooks.id AND status = 'pending' \
                 ORDER BY attempt_number LIMIT 1 \
             ) AS pending ON true \
             WHERE webhooks.id = $1 AND webhooks.user_id = $2",
        )
        .bind(webhook_id)
        .bind(user_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database_error)?
        .ok_or(WebhookRepositoryError::WebhookNotFound)?;

        let attempt_rows = sqlx::query_as::<_, WebhookAttemptRow>(
            "SELECT started_at, completed_at, http_status, latency_ms, response_body, error_message \
             FROM webhook_attempts WHERE webhook_id = $1 \
             ORDER BY completed_at, id",
        )
        .bind(webhook_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_database_error)?;

        transaction.commit().await.map_err(map_database_error)?;

        let content_type = match webhook.payload_type.as_str() {
            "json" => "application/json",
            "text" => "text/plain; charset=utf-8",
            _ => {
                return Err(WebhookRepositoryError::Database(
                    "webhook has an unsupported payload type".to_owned(),
                ))
            }
        };
        let attempts = attempt_rows
            .into_iter()
            .map(|attempt| WebhookAttemptDetail {
                started_at: attempt.started_at,
                completed_at: attempt.completed_at,
                result: WebhookAttemptResult::from_outcome(
                    attempt.http_status,
                    attempt.error_message.as_deref(),
                ),
                http_status: attempt.http_status,
                latency_ms: attempt.latency_ms,
                response_body: attempt.response_body,
                error_message: attempt.error_message,
            })
            .collect();
        let mut headers = std::collections::BTreeMap::new();
        headers.insert("Content-Type".to_owned(), content_type.to_owned());

        Ok(WebhookDetail {
            id: webhook.id,
            created_at: webhook.created_at,
            method: "POST",
            url: webhook.url,
            body: webhook.payload,
            headers,
            payload_type: webhook.payload_type,
            status: webhook.status,
            max_attempts: webhook.max_attempts,
            next_attempt_at: webhook.next_attempt_at,
            attempts,
        })
    }
}

#[derive(sqlx::FromRow)]
struct WebhookDetailRow {
    id: Uuid,
    created_at: DateTime<Utc>,
    url: String,
    payload: String,
    payload_type: String,
    status: String,
    max_attempts: i16,
    next_attempt_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct WebhookAttemptRow {
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    http_status: Option<i16>,
    latency_ms: i64,
    response_body: Option<String>,
    error_message: Option<String>,
}

fn map_database_error(error: sqlx::Error) -> WebhookRepositoryError {
    if let sqlx::Error::Database(database_error) = &error {
        if database_error.code().as_deref() == Some("23503") {
            match database_error.constraint() {
                Some("webhooks_user_id_fkey") => return WebhookRepositoryError::UserNotFound,
                Some("webhook_attempts_webhook_id_fkey") => {
                    return WebhookRepositoryError::WebhookNotFound
                }
                _ => {}
            }
        }
    }

    WebhookRepositoryError::Database(error.to_string())
}

#[cfg(test)]
#[path = "repository_integration_tests.rs"]
mod integration_tests;

#[cfg(test)]
mod tests {
    use super::map_database_error;
    use crate::webhook::model::WebhookRepositoryError;
    use sqlx::error::{DatabaseError, ErrorKind};
    use std::{borrow::Cow, error::Error, fmt};

    #[derive(Debug)]
    struct StubDatabaseError {
        code: &'static str,
        constraint: Option<&'static str>,
    }

    impl fmt::Display for StubDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message())
        }
    }

    impl Error for StubDatabaseError {}

    impl DatabaseError for StubDatabaseError {
        fn message(&self) -> &str {
            "test database error"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }
        fn constraint(&self) -> Option<&str> {
            self.constraint
        }
        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }
    }

    #[test]
    fn maps_missing_webhook_foreign_key() {
        let error = sqlx::Error::Database(Box::new(StubDatabaseError {
            code: "23503",
            constraint: Some("webhook_attempts_webhook_id_fkey"),
        }));

        assert_eq!(
            map_database_error(error),
            WebhookRepositoryError::WebhookNotFound
        );
    }

    #[test]
    fn preserves_missing_user_foreign_key_mapping() {
        let error = sqlx::Error::Database(Box::new(StubDatabaseError {
            code: "23503",
            constraint: Some("webhooks_user_id_fkey"),
        }));

        assert_eq!(
            map_database_error(error),
            WebhookRepositoryError::UserNotFound
        );
    }

    #[test]
    fn preserves_unrelated_database_errors() {
        for (code, constraint) in [
            ("23503", Some("another_foreign_key")),
            ("23503", None),
            ("23514", Some("webhook_attempts_webhook_id_fkey")),
        ] {
            let error = sqlx::Error::Database(Box::new(StubDatabaseError { code, constraint }));
            let message = error.to_string();
            assert_eq!(
                map_database_error(error),
                WebhookRepositoryError::Database(message)
            );
        }
    }

    #[test]
    fn preserves_connection_errors() {
        let error = sqlx::Error::PoolClosed;
        let message = error.to_string();

        assert_eq!(
            map_database_error(error),
            WebhookRepositoryError::Database(message)
        );
    }
}
