use super::model::User;
use serde_json::Value;
use sqlx::{types::Json, PgPool};
use std::fmt;
use std::future::Future;
use tracing::error;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UserRepositoryError {
    Database,
}

impl fmt::Display for UserRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database => formatter.write_str("database operation failed"),
        }
    }
}

impl std::error::Error for UserRepositoryError {}

pub(crate) trait UserRepository {
    fn find_by_api_key_hash(
        &self,
        api_key: &str,
    ) -> impl Future<Output = Result<Option<User>, UserRepositoryError>> + Send;
}

pub(crate) struct PostgresUserRepository {
    pool: PgPool,
}

impl PostgresUserRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl UserRepository for PostgresUserRepository {
    async fn find_by_api_key_hash(
        &self,
        api_key_hash: &str,
    ) -> Result<Option<User>, UserRepositoryError> {
        let row = sqlx::query_as::<_, (Uuid, Json<Value>)>(
            "SELECT id, configuration FROM users WHERE api_key_hash = $1",
        )
        .bind(api_key_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|database_error| {
            error!(error = %database_error, "API key lookup failed");
            UserRepositoryError::Database
        })?;

        Ok(row.map(map_user_row))
    }
}

fn map_user_row((id, Json(configuration)): (Uuid, Json<Value>)) -> User {
    User { id, configuration }
}

#[cfg(test)]
mod tests {
    use super::map_user_row;
    use crate::user::model::User;
    use serde_json::json;
    use sqlx::types::Json;
    use uuid::Uuid;

    #[test]
    fn maps_user_lookup_row_to_id_and_configuration() {
        let id = Uuid::new_v4();
        let configuration = json!({"retries": 5});

        let user = map_user_row((id, Json(configuration.clone())));

        assert_eq!(user, User { id, configuration });
    }
}
