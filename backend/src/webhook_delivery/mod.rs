pub(crate) mod policy;
mod postgres;
mod redis_queue;
mod worker;

pub async fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    worker::run().await
}
