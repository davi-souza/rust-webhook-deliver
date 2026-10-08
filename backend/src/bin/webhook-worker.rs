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

    rust_webhook::webhook_delivery::run_worker().await
}
