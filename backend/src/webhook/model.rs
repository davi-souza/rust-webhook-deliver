use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::BTreeMap, fmt};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PayloadType {
    #[default]
    Json,
    Text,
}

impl PayloadType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Text => "text",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewWebhook {
    pub(crate) user_id: Uuid,
    pub(crate) url: String,
    pub(crate) payload: String,
    pub(crate) payload_type: PayloadType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WebhookListItem {
    pub(crate) id: Uuid,
    pub(crate) url: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) status: String,
    pub(crate) attempt_count: i64,
    pub(crate) http_status: Option<i16>,
    pub(crate) error_message: Option<String>,
    pub(crate) completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WebhookListPage {
    pub(crate) items: Vec<WebhookListItem>,
    pub(crate) next_cursor: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WebhookDetail {
    pub(crate) id: Uuid,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) method: &'static str,
    pub(crate) url: String,
    pub(crate) body: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) payload_type: String,
    pub(crate) status: String,
    pub(crate) max_attempts: i16,
    pub(crate) next_attempt_at: Option<DateTime<Utc>>,
    pub(crate) attempts: Vec<WebhookAttemptDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WebhookAttemptDetail {
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) completed_at: DateTime<Utc>,
    pub(crate) result: WebhookAttemptResult,
    pub(crate) http_status: Option<i16>,
    pub(crate) latency_ms: i64,
    pub(crate) response_body: Option<String>,
    pub(crate) error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WebhookAttemptResult {
    Success,
    Failure,
}

impl WebhookAttemptResult {
    pub(crate) fn from_outcome(http_status: Option<i16>, error_message: Option<&str>) -> Self {
        if http_status.is_some_and(|status| (200..300).contains(&status)) && error_message.is_none()
        {
            Self::Success
        } else {
            Self::Failure
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Attempt recording is not connected to a delivery worker yet"
)]
pub(crate) struct NewWebhookAttempt {
    webhook_id: Uuid,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    http_status: Option<i16>,
    latency_ms: i64,
    response_body: Option<String>,
    error_message: Option<String>,
}

#[allow(
    dead_code,
    reason = "Attempt recording is not connected to a delivery worker yet"
)]
impl NewWebhookAttempt {
    pub(crate) fn new(
        webhook_id: Uuid,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        http_status: Option<i16>,
        latency_ms: i64,
        response_body: Option<String>,
        error_message: Option<String>,
    ) -> Result<Self, NewWebhookAttemptError> {
        if completed_at < started_at {
            return Err(NewWebhookAttemptError::CompletedBeforeStarted);
        }
        if latency_ms < 0 {
            return Err(NewWebhookAttemptError::NegativeLatency);
        }
        if http_status.is_some_and(|status| !(100..=599).contains(&status)) {
            return Err(NewWebhookAttemptError::InvalidHttpStatus);
        }
        if http_status.is_none() && error_message.is_none() {
            return Err(NewWebhookAttemptError::MissingOutcome);
        }

        Ok(Self {
            webhook_id,
            started_at,
            completed_at,
            http_status,
            latency_ms,
            response_body,
            error_message,
        })
    }

    pub(crate) fn webhook_id(&self) -> Uuid {
        self.webhook_id
    }
    pub(crate) fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
    pub(crate) fn completed_at(&self) -> DateTime<Utc> {
        self.completed_at
    }
    pub(crate) fn http_status(&self) -> Option<i16> {
        self.http_status
    }
    pub(crate) fn latency_ms(&self) -> i64 {
        self.latency_ms
    }
    pub(crate) fn response_body(&self) -> Option<&str> {
        self.response_body.as_deref()
    }
    pub(crate) fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

#[derive(Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Attempt recording is not connected to a delivery worker yet"
)]
pub(crate) enum NewWebhookAttemptError {
    CompletedBeforeStarted,
    InvalidHttpStatus,
    NegativeLatency,
    MissingOutcome,
}

impl fmt::Display for NewWebhookAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CompletedBeforeStarted => {
                formatter.write_str("completion must not precede start")
            }
            Self::InvalidHttpStatus => {
                formatter.write_str("HTTP status must be between 100 and 599")
            }
            Self::NegativeLatency => formatter.write_str("latency must not be negative"),
            Self::MissingOutcome => formatter.write_str("HTTP status or error message is required"),
        }
    }
}

impl std::error::Error for NewWebhookAttemptError {}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WebhookRepositoryError {
    UserNotFound,
    WebhookNotFound,
    CursorNotFound,
    InvalidDeliveryConfiguration(String),
    Database(String),
}

impl fmt::Display for WebhookRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UserNotFound => formatter.write_str("user does not exist"),
            Self::WebhookNotFound => formatter.write_str("webhook does not exist"),
            Self::CursorNotFound => formatter.write_str("webhook cursor does not exist"),
            Self::InvalidDeliveryConfiguration(message) => {
                write!(formatter, "invalid delivery configuration: {message}")
            }
            Self::Database(message) => write!(formatter, "database operation failed: {message}"),
        }
    }
}

impl std::error::Error for WebhookRepositoryError {}

#[cfg(test)]
mod tests {
    use super::{NewWebhookAttempt, NewWebhookAttemptError, WebhookAttemptResult};
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    fn started_at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_000, 0).expect("valid test timestamp")
    }

    #[test]
    fn attempt_result_is_success_only_for_2xx_without_an_error() {
        assert_eq!(
            WebhookAttemptResult::from_outcome(Some(200), None),
            WebhookAttemptResult::Success
        );
        assert_eq!(
            WebhookAttemptResult::from_outcome(Some(299), None),
            WebhookAttemptResult::Success
        );
        for (http_status, error_message) in [
            (Some(200), Some("response body read failed")),
            (Some(503), None),
            (None, Some("connection timed out")),
        ] {
            assert_eq!(
                WebhookAttemptResult::from_outcome(http_status, error_message),
                WebhookAttemptResult::Failure
            );
        }
    }

    #[test]
    fn preserves_completed_attempt_data() {
        let webhook_id = Uuid::new_v4();
        let completed_at = DateTime::from_timestamp(1_001, 0).expect("valid test timestamp");
        let attempt = NewWebhookAttempt::new(
            webhook_id,
            started_at(),
            completed_at,
            Some(503),
            42,
            Some("service unavailable".to_owned()),
            Some("delivery failed".to_owned()),
        )
        .expect("HTTP status and error can coexist");

        assert_eq!(attempt.webhook_id(), webhook_id);
        assert_eq!(attempt.started_at(), started_at());
        assert_eq!(attempt.completed_at(), completed_at);
        assert_eq!(attempt.http_status(), Some(503));
        assert_eq!(attempt.latency_ms(), 42);
        assert_eq!(attempt.response_body(), Some("service unavailable"));
        assert_eq!(attempt.error_message(), Some("delivery failed"));
    }

    #[test]
    fn accepts_http_status_without_response_body_or_error() {
        for status in [100, 200, 404, 500, 599] {
            let attempt = NewWebhookAttempt::new(
                Uuid::new_v4(),
                started_at(),
                started_at(),
                Some(status),
                0,
                None,
                None,
            )
            .expect("valid HTTP status");

            assert_eq!(attempt.http_status(), Some(status));
            assert_eq!(attempt.latency_ms(), 0);
            assert_eq!(attempt.response_body(), None);
            assert_eq!(attempt.error_message(), None);
        }
    }

    #[test]
    fn accepts_error_without_http_status() {
        let attempt = NewWebhookAttempt::new(
            Uuid::new_v4(),
            started_at(),
            started_at(),
            None,
            0,
            None,
            Some("connection timed out".to_owned()),
        )
        .expect("network error is a valid outcome");

        assert_eq!(attempt.http_status(), None);
        assert_eq!(attempt.error_message(), Some("connection timed out"));
    }

    #[test]
    fn accepts_empty_text_and_maximum_latency_as_allowed_by_schema() {
        let attempt = NewWebhookAttempt::new(
            Uuid::new_v4(),
            started_at(),
            started_at(),
            None,
            i64::MAX,
            Some(String::new()),
            Some(String::new()),
        )
        .expect("empty text is distinct from NULL");

        assert_eq!(attempt.latency_ms(), i64::MAX);
        assert_eq!(attempt.response_body(), Some(""));
        assert_eq!(attempt.error_message(), Some(""));
    }

    #[test]
    fn rejects_completion_before_start() {
        let completed_at = DateTime::from_timestamp(999, 0).expect("valid test timestamp");
        let error = NewWebhookAttempt::new(
            Uuid::new_v4(),
            started_at(),
            completed_at,
            Some(200),
            0,
            None,
            None,
        )
        .expect_err("completion cannot precede start");

        assert_eq!(error, NewWebhookAttemptError::CompletedBeforeStarted);
    }

    #[test]
    fn rejects_negative_latency() {
        let error = NewWebhookAttempt::new(
            Uuid::new_v4(),
            started_at(),
            started_at(),
            Some(200),
            -1,
            None,
            None,
        )
        .expect_err("negative latency");

        assert_eq!(error, NewWebhookAttemptError::NegativeLatency);
    }

    #[test]
    fn rejects_http_status_outside_supported_range() {
        for status in [i16::MIN, 0, 99, 600, i16::MAX] {
            let error = NewWebhookAttempt::new(
                Uuid::new_v4(),
                started_at(),
                started_at(),
                Some(status),
                0,
                None,
                Some("error does not make an invalid status valid".to_owned()),
            )
            .expect_err("invalid HTTP status");

            assert_eq!(error, NewWebhookAttemptError::InvalidHttpStatus);
        }
    }

    #[test]
    fn rejects_missing_status_and_error_even_with_response_body() {
        let error = NewWebhookAttempt::new(
            Uuid::new_v4(),
            started_at(),
            started_at(),
            None,
            0,
            Some("response alone is not an outcome".to_owned()),
            None,
        )
        .expect_err("missing outcome");

        assert_eq!(error, NewWebhookAttemptError::MissingOutcome);
    }
}
