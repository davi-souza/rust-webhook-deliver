use crate::AppState;
use axum::{extract::State, http::StatusCode, response::Json};
use serde_json::json;
use std::sync::Arc;

pub(crate) async fn root_handler() -> Json<serde_json::Value> {
    Json(json!({
        "service": "RustWebhook",
        "status": "ready",
        "message": "Local development environment is active and running."
    }))
}

pub(crate) async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();

    let mut redis_conn = state.redis.clone();
    let redis_ok = redis::cmd("PING")
        .query_async::<String>(&mut redis_conn)
        .await
        .is_ok();

    let is_healthy = db_ok && redis_ok;
    let status_code = if is_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status_code,
        Json(json!({
            "status": if is_healthy { "healthy" } else { "degraded" },
            "dependencies": {
                "postgres": if db_ok { "connected" } else { "disconnected" },
                "redis": if redis_ok { "connected" } else { "disconnected" }
            }
        })),
    )
}
