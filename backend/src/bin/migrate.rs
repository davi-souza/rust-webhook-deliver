use sqlx::{migrate::Migrator, postgres::PgPoolOptions};
use std::error::Error;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;
    println!("Database migrations applied successfully.");
    pool.close().await;

    Ok(())
}
