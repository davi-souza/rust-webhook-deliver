use super::handler::{create_webhook_handler, get_webhook_handler, list_webhooks_handler};
use crate::{
    http::{content_type::require_json_content_type, version::version_middleware},
    user::auth::authenticate,
    AppState,
};
use axum::{extract::Request, http::Method, middleware, response::Response, routing::get, Router};
use sqlx::PgPool;
use std::sync::Arc;

pub(crate) fn routes(pool: PgPool) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/webhooks",
            get(list_webhooks_handler).post(create_webhook_handler),
        )
        .route("/webhooks/{id}", get(get_webhook_handler))
        .route_layer(middleware::from_fn_with_state(pool, authenticate))
        .route_layer(middleware::from_fn(require_json_content_type_for_create))
        .route_layer(middleware::from_fn(version_middleware))
}

async fn require_json_content_type_for_create(
    request: Request,
    next: middleware::Next,
) -> Response {
    if request.method() == Method::POST {
        require_json_content_type(request, next).await
    } else {
        next.run(request).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{http, AppState};
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use chrono::{Duration, Utc};
    use redis::aio::ConnectionManager;
    use sha2::{Digest, Sha256};
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;

    const API_KEY: &str = "webhook-list-route-test-key";

    #[sqlx::test(migrations = "./migrations")]
    async fn plural_webhook_routes_list_without_content_type_and_create_with_json(
        pool: PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query("INSERT INTO users (name, api_key_hash) VALUES ($1, $2)")
            .bind("route test user")
            .bind(format!("{:x}", Sha256::digest(API_KEY.as_bytes())))
            .execute(&pool)
            .await?;
        let redis_client = redis::Client::open("redis://redis:6379")?;
        let redis = ConnectionManager::new(redis_client).await?;
        let app = http::router(Arc::new(AppState {
            db: pool.clone(),
            redis,
        }));

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = to_bytes(list_response.into_body(), 4096).await?;
        let list_json: serde_json::Value = serde_json::from_slice(&list_body)?;
        assert_eq!(list_json["items"].as_array().map(Vec::len), Some(0));
        assert!(list_json["next_cursor"].is_null());

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/webhooks")
                    .header("x-api-key", API_KEY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://hooks.example.com/events","payload":"{}"}"#,
                    ))?,
            )
            .await?;
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let created_body = to_bytes(create_response.into_body(), 4096).await?;
        let created_json: serde_json::Value = serde_json::from_slice(&created_body)?;
        let webhook_id = created_json["id"]
            .as_str()
            .expect("creation should return a webhook UUID");

        let detail_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/webhooks/{webhook_id}"))
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), 4096).await?;
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body)?;
        assert_eq!(detail_json["id"], webhook_id);
        assert_eq!(detail_json["method"], "POST");
        assert_eq!(detail_json["body"], "{}");
        assert_eq!(detail_json["headers"]["Content-Type"], "application/json");
        assert_eq!(detail_json["payload_type"], "json");
        assert_eq!(detail_json["status"], "pending");
        assert_eq!(detail_json["max_attempts"], 5);
        assert!(detail_json["created_at"].is_string());
        assert!(detail_json["next_attempt_at"].is_string());
        assert_eq!(detail_json["attempts"].as_array().map(Vec::len), Some(0));

        let webhook_uuid = uuid::Uuid::parse_str(webhook_id)?;
        let base_time = Utc::now() - Duration::minutes(1);
        for (offset, http_status, error, response_body) in [
            (4, Some(200_i16), None, Some("accepted")),
            (
                3,
                Some(503_i16),
                Some("HTTP request returned status 503"),
                Some("unavailable"),
            ),
            (2, None, Some("connection timed out"), None),
            (
                1,
                Some(200_i16),
                Some("response body read failed"),
                Some("partial"),
            ),
        ] {
            let completed_at = base_time + Duration::seconds(offset);
            sqlx::query(
                "INSERT INTO webhook_attempts \
                 (webhook_id, started_at, completed_at, http_status, latency_ms, response_body, error_message) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(webhook_uuid)
            .bind(completed_at - Duration::milliseconds(12))
            .bind(completed_at)
            .bind(http_status)
            .bind(offset * 10)
            .bind(response_body)
            .bind(error)
            .execute(&pool)
            .await?;
        }

        let populated_detail_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/webhooks/{webhook_id}"))
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(populated_detail_response.status(), StatusCode::OK);
        let populated_detail_body = to_bytes(populated_detail_response.into_body(), 8192).await?;
        let populated_detail_json: serde_json::Value =
            serde_json::from_slice(&populated_detail_body)?;
        let attempts = populated_detail_json["attempts"]
            .as_array()
            .expect("attempts should be an array");
        assert_eq!(attempts.len(), 4);
        assert_eq!(attempts[0]["http_status"], 200);
        assert_eq!(attempts[0]["result"], "failure");
        assert_eq!(attempts[0]["error_message"], "response body read failed");
        assert!(attempts[1]["http_status"].is_null());
        assert_eq!(attempts[1]["result"], "failure");
        assert_eq!(attempts[1]["error_message"], "connection timed out");
        assert_eq!(attempts[2]["http_status"], 503);
        assert_eq!(attempts[2]["result"], "failure");
        assert_eq!(attempts[2]["latency_ms"], 30);
        assert_eq!(attempts[3]["http_status"], 200);
        assert_eq!(attempts[3]["result"], "success");
        assert_eq!(attempts[3]["response_body"], "accepted");

        let malformed_id_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks/not-a-uuid")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(malformed_id_response.status(), StatusCode::BAD_REQUEST);
        let malformed_id_body = to_bytes(malformed_id_response.into_body(), 4096).await?;
        let malformed_id_json: serde_json::Value = serde_json::from_slice(&malformed_id_body)?;
        assert_eq!(malformed_id_json["errors"][0]["field"], "id");

        let missing_id_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks/00000000-0000-0000-0000-000000000000")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(missing_id_response.status(), StatusCode::NOT_FOUND);
        let missing_id_body = to_bytes(missing_id_response.into_body(), 4096).await?;
        let missing_id_json: serde_json::Value = serde_json::from_slice(&missing_id_body)?;
        assert_eq!(missing_id_json["errors"][0]["field"], "id");

        let other_user_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "INSERT INTO users (name, api_key_hash) VALUES ($1, $2) RETURNING id",
        )
        .bind("another route test user")
        .bind(format!("{:x}", Sha256::digest(b"another-route-test-key")))
        .fetch_one(&pool)
        .await?;
        let foreign_webhook_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "INSERT INTO webhooks (user_id, url, payload) \
             VALUES ($1, 'https://hooks.example.com/foreign', '{}') RETURNING id",
        )
        .bind(other_user_id)
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO webhook_deliveries (webhook_id, max_attempts, status) \
             VALUES ($1, 5, 'pending')",
        )
        .bind(foreign_webhook_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO webhook_delivery_jobs (webhook_id, attempt_number, available_at, status) \
             VALUES ($1, 1, now(), 'pending')",
        )
        .bind(foreign_webhook_id)
        .execute(&pool)
        .await?;
        let foreign_detail_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/webhooks/{foreign_webhook_id}"))
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(foreign_detail_response.status(), StatusCode::NOT_FOUND);

        let unauthenticated_detail_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/webhooks/{webhook_id}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(
            unauthenticated_detail_response.status(),
            StatusCode::UNAUTHORIZED
        );

        let created_list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks?count=0")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(created_list_response.status(), StatusCode::BAD_REQUEST);

        let invalid_cursor_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks?cursor=00000000-0000-0000-0000-000000000000")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(invalid_cursor_response.status(), StatusCode::BAD_REQUEST);

        let populated_list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/webhooks")
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(populated_list_response.status(), StatusCode::OK);
        let populated_body = to_bytes(populated_list_response.into_body(), 4096).await?;
        let populated_json: serde_json::Value = serde_json::from_slice(&populated_body)?;
        assert_eq!(populated_json["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(populated_json["items"][0]["attempt_count"], 4);
        assert_eq!(populated_json["items"][0]["status"], "pending");
        assert_eq!(populated_json["items"][0]["http_status"], 200);
        assert!(populated_json["items"][0]["error_message"].is_null());
        assert!(populated_json["items"][0]["completed_at"].is_string());
        assert!(populated_json["items"][0].get("payload_type").is_none());
        assert!(populated_json["items"][0].get("last_attempt").is_none());

        let text_create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/webhooks")
                    .header("x-api-key", API_KEY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://hooks.example.com/text","payload":"hello","payload_type":"text"}"#,
                    ))?,
            )
            .await?;
        assert_eq!(text_create_response.status(), StatusCode::CREATED);
        let text_create_body = to_bytes(text_create_response.into_body(), 4096).await?;
        let text_create_json: serde_json::Value = serde_json::from_slice(&text_create_body)?;
        let text_webhook_id = text_create_json["id"]
            .as_str()
            .expect("creation should return a webhook UUID");
        let text_detail_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/webhooks/{text_webhook_id}"))
                    .header("x-api-key", API_KEY)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(text_detail_response.status(), StatusCode::OK);
        let text_detail_body = to_bytes(text_detail_response.into_body(), 4096).await?;
        let text_detail_json: serde_json::Value = serde_json::from_slice(&text_detail_body)?;
        assert_eq!(text_detail_json["body"], "hello");
        assert_eq!(text_detail_json["payload_type"], "text");
        assert_eq!(
            text_detail_json["headers"]["Content-Type"],
            "text/plain; charset=utf-8"
        );

        let old_create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/webhook")
                    .header("x-api-key", API_KEY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://hooks.example.com/events","payload":"{}"}"#,
                    ))?,
            )
            .await?;
        assert_eq!(old_create_response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }
}
