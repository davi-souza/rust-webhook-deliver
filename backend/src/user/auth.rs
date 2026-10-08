use super::{
    model::User,
    repository::{PostgresUserRepository, UserRepository, UserRepositoryError},
};
use crate::http::error::HttpError;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::fmt;
use uuid::Uuid;

const API_KEY_HEADER: &str = "x-api-key";

#[derive(Debug, Clone, Copy)]
pub(crate) struct AuthenticatedUser(pub(crate) Uuid);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AuthError {
    Unauthorized,
    Internal,
}

impl fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized => formatter.write_str("API key is missing or invalid"),
            Self::Internal => formatter.write_str("user lookup failed"),
        }
    }
}

impl std::error::Error for AuthError {}

pub(crate) async fn authenticate_api_key<R: UserRepository>(
    api_key: Option<&str>,
    repository: &R,
) -> Result<Uuid, AuthError> {
    let api_key = api_key
        .filter(|api_key| !api_key.is_empty())
        .ok_or(AuthError::Unauthorized)?;
    let api_key_hash = hash_api_key(api_key);

    repository
        .find_by_api_key_hash(&api_key_hash)
        .await
        .map_err(|_: UserRepositoryError| AuthError::Internal)?
        .map(|user: User| user.id)
        .ok_or(AuthError::Unauthorized)
}

fn hash_api_key(api_key: &str) -> String {
    format!("{:x}", Sha256::digest(api_key.as_bytes()))
}

pub(crate) async fn authenticate(
    State(pool): State<PgPool>,
    mut request: Request,
    next: Next,
) -> Response {
    let api_key = request
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|value| value.to_str().ok());
    let repository = PostgresUserRepository::new(pool);

    match authenticate_api_key(api_key, &repository).await {
        Ok(user_id) => {
            request.extensions_mut().insert(AuthenticatedUser(user_id));
            next.run(request).await
        }
        Err(AuthError::Unauthorized) => HttpError::Unauthorized.into_response(),
        Err(AuthError::Internal) => HttpError::Internal.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{authenticate_api_key, hash_api_key, AuthError};
    use crate::user::{
        model::User,
        repository::{UserRepository, UserRepositoryError},
    };
    use serde_json::json;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Default)]
    struct MockUserRepository {
        expected_api_key_hash: Mutex<Option<String>>,
        user: Mutex<Option<User>>,
        fail_lookup: Mutex<bool>,
        received_api_key_hash: Mutex<Option<String>>,
    }

    impl UserRepository for MockUserRepository {
        async fn find_by_api_key_hash(
            &self,
            api_key_hash: &str,
        ) -> Result<Option<User>, UserRepositoryError> {
            *self
                .received_api_key_hash
                .lock()
                .expect("received API key hash lock") = Some(api_key_hash.to_owned());
            if *self.fail_lookup.lock().expect("lookup failure lock") {
                return Err(UserRepositoryError::Database);
            }
            if self
                .expected_api_key_hash
                .lock()
                .expect("expected API key hash lock")
                .as_deref()
                != Some(api_key_hash)
            {
                return Ok(None);
            }
            Ok(self.user.lock().expect("user lock").clone())
        }
    }

    #[tokio::test]
    async fn rejects_missing_api_key_without_repository_lookup() {
        let repository = MockUserRepository::default();

        let error = authenticate_api_key(None, &repository)
            .await
            .expect_err("missing key should be rejected");

        assert_eq!(error, AuthError::Unauthorized);
        assert!(repository
            .received_api_key_hash
            .lock()
            .expect("received API key hash lock")
            .is_none());
    }

    #[tokio::test]
    async fn rejects_empty_api_key_without_repository_lookup() {
        let repository = MockUserRepository::default();

        let error = authenticate_api_key(Some(""), &repository)
            .await
            .expect_err("empty key should be rejected");

        assert_eq!(error, AuthError::Unauthorized);
        assert!(repository
            .received_api_key_hash
            .lock()
            .expect("received API key hash lock")
            .is_none());
    }

    #[tokio::test]
    async fn rejects_api_key_not_present_in_repository() {
        let repository = MockUserRepository::default();

        let error = authenticate_api_key(Some("invalid-key"), &repository)
            .await
            .expect_err("unknown key should be rejected");

        assert_eq!(error, AuthError::Unauthorized);
    }

    #[tokio::test]
    async fn reports_database_lookup_failure_as_internal_auth_error() {
        let repository = MockUserRepository::default();
        *repository.fail_lookup.lock().expect("lookup failure lock") = true;

        let error = authenticate_api_key(Some("abc"), &repository)
            .await
            .expect_err("repository failure should fail authentication");

        assert_eq!(error, AuthError::Internal);
    }

    #[tokio::test]
    async fn passes_api_key_hash_to_repository_and_returns_user_id() {
        let user_id = Uuid::new_v4();
        let repository = MockUserRepository::default();
        *repository
            .expected_api_key_hash
            .lock()
            .expect("expected API key hash lock") =
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned());
        *repository.user.lock().expect("user lock") = Some(User {
            id: user_id,
            configuration: json!({"delivery": "enabled"}),
        });

        let authenticated_user = authenticate_api_key(Some("abc"), &repository)
            .await
            .expect("valid key should authenticate");

        assert_eq!(authenticated_user, user_id);
        assert_eq!(
            repository
                .received_api_key_hash
                .lock()
                .expect("received API key hash lock")
                .as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
    }

    #[test]
    fn hashes_api_key_as_lowercase_sha256_hex() {
        assert_eq!(
            hash_api_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
