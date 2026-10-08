use super::{
    repository::{PostgresWebhookRepository, WebhookRepository},
    request::{parse_create_webhook, parse_list_webhooks_query},
    service::{CreateWebhook, CreateWebhookError, WebhookService},
};
use crate::{
    http::error::{FieldError, HttpError},
    user::auth::AuthenticatedUser,
    AppState,
};
use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

pub(crate) async fn create_webhook_handler(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedUser(user_id)): Extension<AuthenticatedUser>,
    body: Bytes,
) -> Response {
    let repository = PostgresWebhookRepository::new(state.db.clone());
    match create_webhook(&repository, user_id, &body).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn list_webhooks_handler(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedUser(user_id)): Extension<AuthenticatedUser>,
    RawQuery(query): RawQuery,
) -> Response {
    let query = match parse_list_webhooks_query(query.as_deref()) {
        Ok(query) => query,
        Err(errors) => return HttpError::Validation(errors).into_response(),
    };

    let repository = PostgresWebhookRepository::new(state.db.clone());
    match WebhookService::new(&repository)
        .list_webhooks(user_id, query.cursor, query.count)
        .await
    {
        Ok(page) => Json(page).into_response(),
        Err(crate::webhook::model::WebhookRepositoryError::CursorNotFound) => {
            HttpError::Validation(vec![FieldError::new(
                "cursor",
                "must identify a webhook owned by the authenticated user",
            )])
            .into_response()
        }
        Err(error) => {
            error!(error = %error, "webhook listing failed");
            HttpError::Internal.into_response()
        }
    }
}

pub(crate) async fn get_webhook_handler(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedUser(user_id)): Extension<AuthenticatedUser>,
    Path(id): Path<String>,
) -> Response {
    let webhook_id = match Uuid::parse_str(&id) {
        Ok(webhook_id) => webhook_id,
        Err(_) => {
            return HttpError::Validation(vec![FieldError::new("id", "must be a valid UUID")])
                .into_response()
        }
    };

    let repository = PostgresWebhookRepository::new(state.db.clone());
    match WebhookService::new(&repository)
        .get_webhook(user_id, webhook_id)
        .await
    {
        Ok(webhook) => Json(webhook).into_response(),
        Err(crate::webhook::model::WebhookRepositoryError::WebhookNotFound) => {
            HttpError::NotFound.into_response()
        }
        Err(error) => {
            error!(error = %error, webhook_id = %webhook_id, "webhook detail lookup failed");
            HttpError::Internal.into_response()
        }
    }
}

async fn create_webhook<R: WebhookRepository>(
    repository: &R,
    user_id: Uuid,
    bytes: &[u8],
) -> Result<Uuid, HttpError> {
    let body = parse_create_webhook(bytes).map_err(|error| HttpError::Validation(error.errors))?;
    let request = CreateWebhook {
        user_id,
        url: body.url,
        payload: body.payload,
        payload_type: Some(body.payload_type),
    };

    WebhookService::new(repository)
        .create_webhook(request)
        .await
        .map_err(|error| match error {
            CreateWebhookError::InvalidUrl => HttpError::Validation(vec![FieldError::new(
                "url",
                "must be an absolute HTTP or HTTPS URL",
            )]),
            CreateWebhookError::InvalidJsonPayload => {
                HttpError::Validation(vec![FieldError::new("payload", "must contain valid JSON")])
            }
            CreateWebhookError::UserNotFound => HttpError::Unauthorized,
            CreateWebhookError::Repository(error) => {
                error!(error = %error, "webhook persistence failed");
                HttpError::Internal
            }
        })
}

#[cfg(test)]
mod tests {
    use super::create_webhook;
    use crate::webhook::{
        model::{
            NewWebhook, NewWebhookAttempt, PayloadType, WebhookListPage, WebhookRepositoryError,
        },
        repository::WebhookRepository,
    };
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Default)]
    struct MockWebhookRepository {
        inserted: Mutex<Vec<NewWebhook>>,
    }

    impl WebhookRepository for MockWebhookRepository {
        async fn insert(&self, webhook: NewWebhook) -> Result<Uuid, WebhookRepositoryError> {
            self.inserted.lock().expect("insert lock").push(webhook);
            Ok(Uuid::nil())
        }

        async fn add_attempt(
            &self,
            _attempt: NewWebhookAttempt,
        ) -> Result<Uuid, WebhookRepositoryError> {
            Err(WebhookRepositoryError::Database(
                "unexpected attempt insertion during webhook creation".to_owned(),
            ))
        }

        async fn list(
            &self,
            _user_id: Uuid,
            _cursor: Option<Uuid>,
            _count: i64,
        ) -> Result<WebhookListPage, WebhookRepositoryError> {
            Ok(WebhookListPage {
                items: Vec::new(),
                next_cursor: None,
            })
        }

        async fn get(
            &self,
            _user_id: Uuid,
            _webhook_id: Uuid,
        ) -> Result<crate::webhook::model::WebhookDetail, WebhookRepositoryError> {
            Err(WebhookRepositoryError::WebhookNotFound)
        }
    }

    #[tokio::test]
    async fn rejects_invalid_body_before_repository_insert() {
        let repository = MockWebhookRepository::default();

        let result = create_webhook(
            &repository,
            Uuid::new_v4(),
            br#"{"url":"file:///tmp/payload","payload":"{}"}"#,
        )
        .await;

        assert!(result.is_err());
        assert!(repository.inserted.lock().expect("insert lock").is_empty());
    }

    #[tokio::test]
    async fn creates_webhook_for_authenticated_user_and_persists_payload_type() {
        let repository = MockWebhookRepository::default();
        let user_id = Uuid::new_v4();

        let id = create_webhook(
            &repository,
            user_id,
            br#"{"url":"https://hooks.example.com/events","payload":"plain text","payload_type":"text"}"#,
        )
        .await
        .expect("valid webhook should be created");

        assert_eq!(id, Uuid::nil());
        let inserted = repository.inserted.lock().expect("insert lock");
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].user_id, user_id);
        assert_eq!(inserted[0].payload_type, PayloadType::Text);
    }
}
