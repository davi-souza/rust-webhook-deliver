use super::{
    model::{NewWebhook, PayloadType, WebhookDetail, WebhookListPage, WebhookRepositoryError},
    repository::WebhookRepository,
};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateWebhook {
    pub(crate) user_id: Uuid,
    pub(crate) url: String,
    pub(crate) payload: String,
    pub(crate) payload_type: Option<PayloadType>,
}

pub(crate) struct WebhookService<'a, R> {
    repository: &'a R,
}

impl<'a, R> WebhookService<'a, R>
where
    R: WebhookRepository,
{
    pub(crate) fn new(repository: &'a R) -> Self {
        Self { repository }
    }

    pub(crate) async fn create_webhook(
        &self,
        request: CreateWebhook,
    ) -> Result<Uuid, CreateWebhookError> {
        validate_url(&request.url)?;

        let payload_type = request.payload_type.unwrap_or_default();
        if payload_type == PayloadType::Json {
            serde_json::from_str::<serde_json::Value>(&request.payload)
                .map_err(|_| CreateWebhookError::InvalidJsonPayload)?;
        }

        let webhook = NewWebhook {
            user_id: request.user_id,
            url: request.url,
            payload: request.payload,
            payload_type,
        };

        self.repository
            .insert(webhook)
            .await
            .map_err(|error| match error {
                WebhookRepositoryError::UserNotFound => CreateWebhookError::UserNotFound,
                error => CreateWebhookError::Repository(error),
            })
    }

    pub(crate) async fn list_webhooks(
        &self,
        user_id: Uuid,
        cursor: Option<Uuid>,
        count: i64,
    ) -> Result<WebhookListPage, WebhookRepositoryError> {
        self.repository.list(user_id, cursor, count).await
    }

    pub(crate) async fn get_webhook(
        &self,
        user_id: Uuid,
        webhook_id: Uuid,
    ) -> Result<WebhookDetail, WebhookRepositoryError> {
        self.repository.get(user_id, webhook_id).await
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CreateWebhookError {
    InvalidUrl,
    InvalidJsonPayload,
    UserNotFound,
    Repository(WebhookRepositoryError),
}

impl fmt::Display for CreateWebhookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl => formatter.write_str("URL must be an absolute HTTP or HTTPS URL"),
            Self::InvalidJsonPayload => formatter.write_str("payload must contain valid JSON"),
            Self::UserNotFound => formatter.write_str("user does not exist"),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CreateWebhookError {}

fn validate_url(value: &str) -> Result<(), CreateWebhookError> {
    let url = url::Url::parse(value).map_err(|_| CreateWebhookError::InvalidUrl)?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(CreateWebhookError::InvalidUrl);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CreateWebhook, CreateWebhookError, WebhookService};
    use crate::webhook::{
        model::{
            NewWebhook, NewWebhookAttempt, PayloadType, WebhookListPage, WebhookRepositoryError,
        },
        repository::WebhookRepository,
    };
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Default)]
    struct MockRepository {
        inserted: Mutex<Vec<NewWebhook>>,
        error: Mutex<Option<WebhookRepositoryError>>,
    }

    impl WebhookRepository for MockRepository {
        async fn insert(&self, webhook: NewWebhook) -> Result<Uuid, WebhookRepositoryError> {
            let error = self.error.lock().expect("mock error lock").take();
            if let Some(error) = error {
                return Err(error);
            }

            self.inserted
                .lock()
                .expect("mock insert lock")
                .push(webhook);
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
        ) -> Result<super::super::model::WebhookDetail, WebhookRepositoryError> {
            Err(WebhookRepositoryError::WebhookNotFound)
        }
    }

    fn input(payload: &str, payload_type: Option<PayloadType>) -> CreateWebhook {
        CreateWebhook {
            user_id: Uuid::new_v4(),
            url: "https://hooks.example.com/events".to_owned(),
            payload: payload.to_owned(),
            payload_type,
        }
    }

    #[tokio::test]
    async fn creates_json_webhook_and_defaults_payload_type_to_json() {
        let repository = MockRepository::default();
        let service = WebhookService::new(&repository);

        let id = service
            .create_webhook(input(r#"{"event":"created"}"#, None))
            .await
            .expect("webhook should be created");

        assert_eq!(id, Uuid::nil());
        let inserted = repository.inserted.lock().expect("mock insert lock");
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].payload, r#"{"event":"created"}"#);
        assert_eq!(inserted[0].payload_type, PayloadType::Json);
    }

    #[tokio::test]
    async fn creates_text_webhook_without_json_validation() {
        let repository = MockRepository::default();
        let service = WebhookService::new(&repository);

        service
            .create_webhook(input("plain text", Some(PayloadType::Text)))
            .await
            .expect("text webhook should be created");

        let inserted = repository.inserted.lock().expect("mock insert lock");
        assert_eq!(inserted[0].payload, "plain text");
        assert_eq!(inserted[0].payload_type, PayloadType::Text);
    }

    #[tokio::test]
    async fn rejects_invalid_url() {
        let repository = MockRepository::default();
        let service = WebhookService::new(&repository);
        let mut request = input("{}", None);
        request.url = "file:///tmp/payload".to_owned();

        let error = service
            .create_webhook(request)
            .await
            .expect_err("non-http URL should be rejected");

        assert!(matches!(error, CreateWebhookError::InvalidUrl));
        assert!(repository
            .inserted
            .lock()
            .expect("mock insert lock")
            .is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_json_payload() {
        let repository = MockRepository::default();
        let service = WebhookService::new(&repository);

        let error = service
            .create_webhook(input("not json", Some(PayloadType::Json)))
            .await
            .expect_err("invalid JSON should be rejected");

        assert!(matches!(error, CreateWebhookError::InvalidJsonPayload));
        assert!(repository
            .inserted
            .lock()
            .expect("mock insert lock")
            .is_empty());
    }

    #[tokio::test]
    async fn reports_missing_user() {
        let repository = MockRepository::default();
        *repository.error.lock().expect("mock error lock") =
            Some(WebhookRepositoryError::UserNotFound);
        let service = WebhookService::new(&repository);

        let error = service
            .create_webhook(input("{}", None))
            .await
            .expect_err("missing user should be reported");

        assert!(matches!(error, CreateWebhookError::UserNotFound));
    }

    #[tokio::test]
    async fn reports_repository_failure() {
        let repository = MockRepository::default();
        *repository.error.lock().expect("mock error lock") = Some(
            WebhookRepositoryError::Database("database unavailable".to_owned()),
        );
        let service = WebhookService::new(&repository);

        let error = service
            .create_webhook(input("{}", None))
            .await
            .expect_err("repository failure should be reported");

        assert!(matches!(error, CreateWebhookError::Repository(_)));
    }
}
