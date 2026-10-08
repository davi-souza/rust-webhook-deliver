use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::json;

const API_VERSION_HEADER: &str = "x-api-version";
const API_KEY_HEADER: &str = "x-api-key";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FieldError {
    pub(crate) field: String,
    pub(crate) reason: String,
}

impl FieldError {
    pub(crate) fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum HttpError {
    Validation(Vec<FieldError>),
    UnsupportedVersion,
    Unauthorized,
    NotFound,
    UnsupportedMediaType,
    Internal,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, errors) = match self {
            Self::Validation(errors) => (StatusCode::BAD_REQUEST, errors),
            Self::UnsupportedVersion => (
                StatusCode::BAD_REQUEST,
                vec![FieldError::new(
                    API_VERSION_HEADER,
                    "must be `v1`; supported version is `v1`",
                )],
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                vec![FieldError::new(API_KEY_HEADER, "is missing or invalid")],
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                vec![FieldError::new("id", "webhook does not exist")],
            ),
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                vec![FieldError::new(
                    "Content-Type",
                    "must be `application/json` with an optional UTF-8 charset",
                )],
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                vec![FieldError::new("server", "an internal error occurred")],
            ),
        };

        (status, Json(json!({ "errors": errors }))).into_response()
    }
}
