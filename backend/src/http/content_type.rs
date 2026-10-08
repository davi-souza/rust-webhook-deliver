use crate::http::error::HttpError;
use axum::{
    extract::Request,
    http::{header, HeaderMap},
    middleware::Next,
    response::{IntoResponse, Response},
};
use mime::Mime;

pub(crate) async fn require_json_content_type(request: Request, next: Next) -> Response {
    if has_json_content_type(request.headers()) {
        next.run(request).await
    } else {
        HttpError::UnsupportedMediaType.into_response()
    }
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Ok(content_type) = content_type.parse::<Mime>() else {
        return false;
    };

    content_type.type_() == mime::APPLICATION
        && content_type.subtype() == mime::JSON
        && content_type.params().all(|(name, value)| {
            name == mime::CHARSET && value.as_str().eq_ignore_ascii_case("utf-8")
        })
}

#[cfg(test)]
mod tests {
    use super::require_json_content_type;
    use crate::http::version::version_middleware;
    use axum::{
        body::{to_bytes, Body},
        http::{header, HeaderMap, HeaderValue, Request, StatusCode},
        middleware::{self, Next},
        response::{IntoResponse, Response},
        routing::{get, post},
        Router,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tower::ServiceExt;

    fn headers(content_type: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(content_type) = content_type {
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_bytes(content_type.as_bytes()).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn accepts_json_with_optional_utf8_charset() {
        for content_type in [
            "application/json",
            "application/json; charset=utf-8",
            "application/json; charset=UTF-8",
        ] {
            assert!(super::has_json_content_type(&headers(Some(content_type))));
        }
    }

    #[test]
    fn rejects_missing_malformed_or_unsupported_content_type() {
        for content_type in [
            None,
            Some("application/json; charset=iso-8859-1"),
            Some("application/json; charset=utf-8; profile=compact"),
            Some("application/cloudevents+json"),
            Some("not a mime type"),
        ] {
            assert!(
                !super::has_json_content_type(&headers(content_type)),
                "unexpectedly accepted {content_type:?}"
            );
        }

        let mut non_text = HeaderMap::new();
        non_text.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_bytes(&[0xff]).expect("header bytes should be permitted"),
        );
        assert!(!super::has_json_content_type(&non_text));
    }

    async fn simulated_auth(request: axum::extract::Request, next: Next) -> Response {
        if request.headers().contains_key("x-api-key") {
            next.run(request).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        }
    }

    async fn echo_body(body: axum::body::Bytes) -> Response {
        (StatusCode::OK, body).into_response()
    }

    fn test_router(handler_called: Arc<AtomicBool>) -> Router {
        async fn plain_route() -> StatusCode {
            StatusCode::OK
        }

        let json_routes = Router::new()
            .route(
                "/json",
                post(move |body: axum::body::Bytes| {
                    let handler_called = Arc::clone(&handler_called);
                    async move {
                        handler_called.store(true, Ordering::SeqCst);
                        echo_body(body).await
                    }
                }),
            )
            .route_layer(middleware::from_fn(simulated_auth))
            .route_layer(middleware::from_fn(require_json_content_type))
            .route_layer(middleware::from_fn(version_middleware));

        Router::new()
            .merge(json_routes)
            .route("/plain", get(plain_route))
    }

    async fn send(
        router: Router,
        method: &str,
        uri: &str,
        headers: HeaderMap,
        body: &'static str,
    ) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body))
            .unwrap();
        *request.headers_mut() = headers;
        router.oneshot(request).await.unwrap()
    }

    #[tokio::test]
    async fn version_then_content_type_then_authentication_precedence() {
        let called = Arc::new(AtomicBool::new(false));

        let request = Request::builder()
            .method("POST")
            .uri("/json")
            .header("x-api-version", "v2")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("{}"))
            .unwrap();
        let response = test_router(Arc::clone(&called))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!called.load(Ordering::SeqCst));

        let response = send(
            test_router(Arc::clone(&called)),
            "POST",
            "/json",
            headers(Some("text/plain")),
            "{}",
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(!called.load(Ordering::SeqCst));
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["errors"][0]["field"], "Content-Type");
        assert_eq!(
            error["errors"][0]["reason"],
            "must be `application/json` with an optional UTF-8 charset"
        );

        let response = send(
            test_router(Arc::clone(&called)),
            "POST",
            "/json",
            HeaderMap::new(),
            "{}",
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let response = send(
            test_router(Arc::clone(&called)),
            "POST",
            "/json",
            headers(Some("application/json")),
            "{}",
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn accepted_json_reaches_authentication_and_plain_route_needs_no_content_type() {
        let called = Arc::new(AtomicBool::new(false));
        let response = send(
            test_router(Arc::clone(&called)),
            "POST",
            "/json",
            headers(Some("application/json; charset=utf-8")),
            "{\"ok\":true}",
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!called.load(Ordering::SeqCst));

        let response = send(
            test_router(called),
            "GET",
            "/plain",
            HeaderMap::new(),
            "ignored",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accepted_header_does_not_consume_request_body() {
        let called = Arc::new(AtomicBool::new(false));
        let request = Request::builder()
            .method("POST")
            .uri("/json")
            .header("x-api-key", "test")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{\"ok\":true}"))
            .unwrap();
        let response = test_router(Arc::clone(&called))
            .oneshot(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .as_ref(),
            b"{\"ok\":true}"
        );
    }
}
