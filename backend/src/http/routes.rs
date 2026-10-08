use crate::{webhook, AppState};
use axum::{routing::get, Router};
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(super::status::root_handler))
        .route("/health", get(super::status::health_handler))
        .nest("/api", webhook::routes::routes(state.db.clone()))
        .with_state(state)
}
