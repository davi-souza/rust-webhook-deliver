use super::error::HttpError;
use axum::{
    extract::Request,
    http::HeaderValue,
    middleware::Next,
    response::{IntoResponse, Response},
};

const API_VERSION_HEADER: &str = "x-api-version";

fn validate_api_version(version: Option<&str>) -> Result<(), HttpError> {
    match version {
        None | Some("v1") => Ok(()),
        Some(_) => Err(HttpError::UnsupportedVersion),
    }
}

fn validate_api_version_header(version: Option<&HeaderValue>) -> Result<(), HttpError> {
    match version {
        None => validate_api_version(None),
        Some(value) => value
            .to_str()
            .map_err(|_| HttpError::UnsupportedVersion)
            .and_then(|value| validate_api_version(Some(value))),
    }
}

pub(crate) async fn version_middleware(request: Request, next: Next) -> Response {
    let version = request.headers().get(API_VERSION_HEADER);

    match validate_api_version_header(version) {
        Ok(()) => next.run(request).await,
        Err(error) => error.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_api_version, validate_api_version_header};
    use axum::http::HeaderValue;

    #[test]
    fn defaults_missing_api_version_to_v1_and_accepts_v1() {
        assert!(validate_api_version(None).is_ok());
        assert!(validate_api_version(Some("v1")).is_ok());
    }

    #[test]
    fn rejects_unsupported_api_version() {
        assert!(validate_api_version(Some("v2")).is_err());
    }

    #[test]
    fn rejects_api_version_header_that_is_not_valid_utf8() {
        let header = HeaderValue::from_bytes(&[0xff]).expect("obs-text header is permitted");
        assert!(validate_api_version_header(Some(&header)).is_err());
    }
}
