pub mod http;
mod user;
mod webhook;
pub mod webhook_delivery;

use redis::aio::ConnectionManager;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: ConnectionManager,
}
