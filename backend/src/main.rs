use redis::aio::ConnectionManager;
use rust_webhook::{http, AppState};
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,rust_webhook=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting RustWebhook service in local development mode...");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgres:5432/rust_webhook".to_string());
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_string());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(8000);

    info!("Connecting to PostgreSQL at {}...", database_url);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .map_err(|error| {
            error!("Failed to connect to PostgreSQL: {}", error);
            error
        })?;

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .map_err(|error| {
            error!("Failed to execute test query on PostgreSQL: {}", error);
            error
        })?;
    info!("✅ PostgreSQL connection established successfully.");

    info!("Connecting to Redis at {}...", redis_url);
    let redis_client = redis::Client::open(redis_url.clone()).map_err(|error| {
        error!("Invalid Redis URL: {}", error);
        error
    })?;
    let redis_conn = ConnectionManager::new(redis_client)
        .await
        .map_err(|error| {
            error!("Failed to establish Redis connection manager: {}", error);
            error
        })?;

    let mut conn_test = redis_conn.clone();
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn_test)
        .await
        .map_err(|error| {
            error!("Failed to ping Redis: {}", error);
            error
        })?;
    info!(
        "✅ Redis connection established successfully (response: {}).",
        pong
    );

    let state = Arc::new(AppState {
        db: pool,
        redis: redis_conn,
    });
    let app = http::router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🚀 RustWebhook server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
