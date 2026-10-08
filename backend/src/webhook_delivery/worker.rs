use super::{
    policy::{backoff_delay_with_settings, DeliveryOutcome},
    postgres::{ClaimResult, DeliveryInput, DeliveryStore, PostgresDeliveryStore, QueueMessage},
    redis_queue::RedisDeliveryQueue,
};
use crate::webhook::model::NewWebhookAttempt;
use chrono::{Duration as ChronoDuration, Utc};
use redis::{aio::ConnectionManager, Client};
use reqwest::{redirect::Policy, Client as HttpClient};
use std::{
    env,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinSet, time::MissedTickBehavior};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct WorkerConfig {
    concurrency: usize,
    http_timeout: Duration,
    backoff_base: u64,
    backoff_max: u64,
    jitter_max_percent: u8,
    poll_interval: Duration,
    reconcile_interval: Duration,
    batch_size: usize,
}

impl WorkerConfig {
    fn from_env() -> Result<Self, String> {
        let concurrency = parse_env("DELIVERY_CONCURRENCY", 10_usize)?;
        let timeout_seconds = parse_env("DELIVERY_HTTP_TIMEOUT_SECS", 30_u64)?;
        let backoff_base = parse_env("DELIVERY_BACKOFF_BASE_SECS", 5_u64)?;
        let backoff_max = parse_env("DELIVERY_BACKOFF_MAX_SECS", 300_u64)?;
        let jitter_max_percent = parse_env("DELIVERY_BACKOFF_JITTER_PERCENT", 20_u8)?;
        let poll_interval_ms = parse_env("DELIVERY_POLL_INTERVAL_MS", 1_000_u64)?;
        let reconcile_interval_seconds = parse_env("DELIVERY_RECONCILE_INTERVAL_SECS", 60_u64)?;
        let batch_size = parse_env("DELIVERY_BATCH_SIZE", 100_usize)?;
        if !(1..=256).contains(&concurrency) {
            return Err("DELIVERY_CONCURRENCY must be between 1 and 256".to_owned());
        }
        if !(1..=300).contains(&timeout_seconds) {
            return Err("DELIVERY_HTTP_TIMEOUT_SECS must be between 1 and 300".to_owned());
        }
        if !(1..=60).contains(&backoff_base) || !(backoff_base..=3600).contains(&backoff_max) {
            return Err("delivery backoff values must satisfy 1 <= base <= max <= 3600".to_owned());
        }
        if jitter_max_percent > 20 {
            return Err("DELIVERY_BACKOFF_JITTER_PERCENT must be between 0 and 20".to_owned());
        }
        if !(50..=60_000).contains(&poll_interval_ms) {
            return Err("DELIVERY_POLL_INTERVAL_MS must be between 50 and 60000".to_owned());
        }
        if !(1..=600).contains(&reconcile_interval_seconds) {
            return Err("DELIVERY_RECONCILE_INTERVAL_SECS must be between 1 and 600".to_owned());
        }
        if !(1..=1_000).contains(&batch_size) {
            return Err("DELIVERY_BATCH_SIZE must be between 1 and 1000".to_owned());
        }
        Ok(Self {
            concurrency,
            http_timeout: Duration::from_secs(timeout_seconds),
            backoff_base,
            backoff_max,
            jitter_max_percent,
            poll_interval: Duration::from_millis(poll_interval_ms),
            reconcile_interval: Duration::from_secs(reconcile_interval_seconds),
            batch_size,
        })
    }

    fn processing_lease(&self) -> ChronoDuration {
        ChronoDuration::seconds((self.http_timeout.as_secs().saturating_mul(3)).max(90) as i64)
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| format!("{name} has an invalid value")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("{name} could not be read: {error}")),
    }
}

trait DeliveryTransport: Send + Sync + 'static {
    fn send<'a>(
        &'a self,
        input: &'a DeliveryInput,
    ) -> Pin<Box<dyn Future<Output = HttpResult> + Send + 'a>>;
}

trait DeliveryClock: Send + Sync + 'static {
    fn utc_now(&self) -> chrono::DateTime<Utc>;
    fn monotonic_now(&self) -> Instant;
}

trait JitterSource: Send + Sync + 'static {
    fn percent(&self, maximum: u8) -> u8;
}

struct SystemClock;

impl DeliveryClock for SystemClock {
    fn utc_now(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }

    fn monotonic_now(&self) -> Instant {
        Instant::now()
    }
}

struct RandomJitter;

impl JitterSource for RandomJitter {
    fn percent(&self, maximum: u8) -> u8 {
        if maximum == 0 {
            0
        } else {
            Uuid::new_v4().as_bytes()[0] % (maximum + 1)
        }
    }
}

struct ReqwestTransport(HttpClient);

impl DeliveryTransport for ReqwestTransport {
    fn send<'a>(
        &'a self,
        input: &'a DeliveryInput,
    ) -> Pin<Box<dyn Future<Output = HttpResult> + Send + 'a>> {
        Box::pin(send_http(&self.0, input))
    }
}

struct Worker<S, T, C, J> {
    store: S,
    queue: RedisDeliveryQueue,
    transport: T,
    clock: C,
    jitter: J,
    config: WorkerConfig,
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig::from_env()?;
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgres:5432/rust_webhook".to_owned());
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_owned());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((config.concurrency + 10).min(u32::MAX as usize) as u32)
        .connect(&database_url)
        .await?;
    let redis_client = Client::open(redis_url)?;
    let redis_manager = ConnectionManager::new(redis_client.clone()).await?;
    let queue = RedisDeliveryQueue::new(redis_client, redis_manager);
    queue.ensure_group().await?;
    let http = HttpClient::builder()
        .timeout(config.http_timeout)
        .redirect(Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let worker = Arc::new(Worker {
        store: PostgresDeliveryStore::new(pool.clone()),
        queue,
        transport: ReqwestTransport(http),
        clock: SystemClock,
        jitter: RandomJitter,
        config,
    });

    info!(
        concurrency = worker.config.concurrency,
        http_timeout_secs = worker.config.http_timeout.as_secs(),
        "webhook delivery worker started"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let publisher = tokio::spawn(publisher_loop(worker.clone(), shutdown_rx.clone()));
    let consumer = tokio::spawn(consumer_loop(worker, shutdown_rx));
    wait_for_shutdown_signal().await?;
    info!("shutdown requested; draining active deliveries");
    let _ = shutdown_tx.send(true);
    let drain_deadline = Duration::from_secs(60);
    let consumer_abort = consumer.abort_handle();
    let mut consumer = consumer;
    if tokio::time::timeout(drain_deadline, &mut consumer)
        .await
        .is_err()
    {
        warn!("worker drain deadline reached; remaining messages will be recovered");
        consumer_abort.abort();
        let _ = consumer.await;
    }
    publisher.abort();
    let _ = publisher.await;
    pool.close().await;
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

async fn publisher_loop<S, T, C, J>(
    worker: Arc<Worker<S, T, C, J>>,
    mut shutdown: watch::Receiver<bool>,
) where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    let mut poll = tokio::time::interval(worker.config.poll_interval);
    let mut reconcile = tokio::time::interval(worker.config.reconcile_interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    reconcile.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = poll.tick() => {
                if let Err(error) = publish_tick(&worker).await {
                    warn!(error = %error, "delivery outbox publish cycle failed");
                }
            }
            _ = reconcile.tick() => {
                if let Err(error) = worker.store.recover_expired_processing(worker.config.batch_size as i64).await {
                    warn!(error = %error, "expired delivery leases could not be recovered");
                }
            }
        }
    }
}

async fn publish_tick<S, T, C, J>(worker: &Worker<S, T, C, J>) -> Result<(), String>
where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    worker.queue.promote_due(worker.config.batch_size).await?;
    let reserved = worker
        .store
        .reserve_publications(worker.config.batch_size as i64, ChronoDuration::seconds(10))
        .await?;
    for job in reserved {
        let published = worker.queue.publish(&job).await.is_ok();
        worker
            .store
            .finish_publication(job.message.job_id, job.publish_token, published)
            .await?;
        if !published {
            return Err("Redis rejected an outbox publication".to_owned());
        }
    }
    Ok(())
}

async fn consumer_loop<S, T, C, J>(
    worker: Arc<Worker<S, T, C, J>>,
    mut shutdown: watch::Receiver<bool>,
) where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    let consumer_name = format!(
        "{}-{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".to_owned()),
        Uuid::new_v4()
    );
    let mut active = JoinSet::new();
    let mut claim_cursor = "0-0".to_owned();
    let mut next_claim = Instant::now();
    loop {
        if *shutdown.borrow() {
            break;
        }
        while active.len() >= worker.config.concurrency {
            tokio::select! {
                _ = shutdown.changed() => break,
                result = active.join_next() => {
                    if let Some(Err(error)) = result {
                        error!(error = %error, "delivery task panicked");
                    }
                }
            }
            if *shutdown.borrow() {
                break;
            }
        }
        if *shutdown.borrow() {
            break;
        }

        let available_slots = worker.config.concurrency.saturating_sub(active.len());
        if available_slots == 0 {
            continue;
        }
        if Instant::now() >= next_claim {
            match worker
                .queue
                .claim_idle(
                    &consumer_name,
                    worker
                        .config
                        .processing_lease()
                        .to_std()
                        .unwrap_or(Duration::from_secs(90)),
                    &claim_cursor,
                    available_slots,
                )
                .await
            {
                Ok((next, entries)) => {
                    claim_cursor = if next == "0-0" {
                        "0-0".to_owned()
                    } else {
                        next
                    };
                    for (stream_id, message, job_id) in entries {
                        spawn_message(&mut active, worker.clone(), stream_id, message, job_id);
                    }
                }
                Err(error) => {
                    warn!(error = %error, "could not recover pending Redis stream entries")
                }
            }
            next_claim = Instant::now() + Duration::from_secs(5);
        }
        if active.len() >= worker.config.concurrency {
            continue;
        }

        let available_slots = worker.config.concurrency.saturating_sub(active.len());
        let result = tokio::select! {
            _ = shutdown.changed() => break,
            result = worker.queue.read_new(&consumer_name, available_slots, Duration::from_secs(1)) => result,
        };
        match result {
            Ok(entries) => {
                for (stream_id, message, job_id) in entries {
                    spawn_message(&mut active, worker.clone(), stream_id, message, job_id);
                }
            }
            Err(error) => {
                warn!(error = %error, "Redis stream read failed; reconnecting shortly");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    let drain = async { while active.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(60), drain)
        .await
        .is_err()
    {
        active.abort_all();
    }
}

fn spawn_message<S, T, C, J>(
    active: &mut JoinSet<()>,
    worker: Arc<Worker<S, T, C, J>>,
    stream_id: String,
    message: Option<QueueMessage>,
    job_id: Option<Uuid>,
) where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    active.spawn(async move {
        if let Some(message) = message {
            process_message(worker, stream_id, message).await;
        } else {
            warn!(
                stream_id,
                "discarding malformed webhook delivery queue entry"
            );
            if let Err(error) = worker.queue.acknowledge_malformed(&stream_id, job_id).await {
                warn!(error = %error, "could not acknowledge malformed queue entry");
            }
        }
    });
}

async fn process_message<S, T, C, J>(
    worker: Arc<Worker<S, T, C, J>>,
    stream_id: String,
    message: QueueMessage,
) where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    let token = Uuid::new_v4();
    let lease = worker.config.processing_lease();
    let (claim, input) = match worker.store.claim(&message, token, lease).await {
        Ok(result) => result,
        Err(error) => {
            warn!(job_id = %message.job_id, error = %error, "could not claim delivery job");
            return;
        }
    };
    let input = match (claim, input) {
        (ClaimResult::Acquired, Some(input)) => input,
        (
            ClaimResult::AlreadyCompleted
            | ClaimResult::Busy
            | ClaimResult::NotReady
            | ClaimResult::Missing,
            _,
        ) => {
            if let Err(error) = worker
                .queue
                .acknowledge(&stream_id, Some(message.job_id))
                .await
            {
                warn!(job_id = %message.job_id, error = %error, "could not acknowledge duplicate delivery message");
            }
            return;
        }
        (ClaimResult::Acquired, None) => {
            error!(job_id = %message.job_id, "delivery claim omitted its input");
            return;
        }
    };

    let started_at = worker.clock.utc_now();
    let started = worker.clock.monotonic_now();
    let result = match send_with_lease(worker.clone(), input.clone()).await {
        Some(result) => result,
        None => {
            warn!(job_id = %message.job_id, "delivery lease was lost before the HTTP request completed");
            return;
        }
    };
    let completed_at = worker.clock.utc_now().max(started_at);
    let latency_ms = worker
        .clock
        .monotonic_now()
        .saturating_duration_since(started)
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let outcome = match &result.transport_error {
        Some(error) => DeliveryOutcome::TransportError(error.clone()),
        None => DeliveryOutcome::from_http_status(result.status.unwrap_or(599)),
    };
    let successful = matches!(outcome, DeliveryOutcome::Success);
    let retryable = outcome.is_retryable();
    let outcome_label = match &outcome {
        DeliveryOutcome::Success => "success",
        DeliveryOutcome::Retryable(_) => "retryable_http_failure",
        DeliveryOutcome::PermanentFailure(_) => "permanent_http_failure",
        DeliveryOutcome::TransportError(_) => "transport_failure",
    };
    let error_message = result.transport_error.or_else(|| match outcome {
        DeliveryOutcome::Retryable(_) | DeliveryOutcome::PermanentFailure(_) => Some(format!(
            "HTTP request returned status {}",
            result.status.unwrap_or_default()
        )),
        _ => None,
    });
    let attempt = match NewWebhookAttempt::new(
        message.webhook_id,
        started_at,
        completed_at,
        result
            .status
            .filter(|status| (100..=599).contains(status))
            .and_then(|status| i16::try_from(status).ok()),
        latency_ms,
        result.body,
        error_message,
    ) {
        Ok(attempt) => attempt,
        Err(error) => {
            error!(job_id = %message.job_id, error = %error, "could not construct delivery audit attempt");
            return;
        }
    };

    let next_available_at = if retryable && message.attempt_number < input.max_attempts {
        let jitter = worker.jitter.percent(worker.config.jitter_max_percent);
        let delay = configured_backoff(&worker.config, message.attempt_number, jitter);
        Some(completed_at + ChronoDuration::from_std(delay).unwrap_or(ChronoDuration::MAX))
    } else {
        None
    };
    match worker
        .store
        .complete(&input, attempt, successful, retryable, next_available_at)
        .await
    {
        Ok(Some(next_job_id)) => info!(
            webhook_id = %message.webhook_id,
            job_id = %message.job_id,
            next_job_id = %next_job_id,
            attempt_number = message.attempt_number,
            http_status = result.status,
            latency_ms,
            next_attempt_at = %next_available_at.unwrap_or_default(),
            delivery_result = outcome_label,
            "webhook attempt failed; retry scheduled"
        ),
        Ok(None) => info!(
            webhook_id = %message.webhook_id,
            job_id = %message.job_id,
            attempt_number = message.attempt_number,
            http_status = result.status,
            latency_ms,
            delivery_result = outcome_label,
            "webhook delivery attempt completed"
        ),
        Err(error) => {
            warn!(job_id = %message.job_id, error = %error, "could not persist delivery result; job remains recoverable");
            return;
        }
    }
    if let Err(error) = worker
        .queue
        .acknowledge(&stream_id, Some(message.job_id))
        .await
    {
        warn!(job_id = %message.job_id, error = %error, "delivery is durable but Redis acknowledgement failed");
    }
}

fn configured_backoff(config: &WorkerConfig, attempt_number: i16, jitter_percent: u8) -> Duration {
    backoff_delay_with_settings(
        attempt_number,
        config.backoff_base,
        config.backoff_max,
        jitter_percent.min(config.jitter_max_percent),
    )
}

struct HttpResult {
    status: Option<u16>,
    body: Option<String>,
    transport_error: Option<String>,
}

async fn send_with_lease<S, T, C, J>(
    worker: Arc<Worker<S, T, C, J>>,
    input: DeliveryInput,
) -> Option<HttpResult>
where
    S: DeliveryStore,
    T: DeliveryTransport,
    C: DeliveryClock,
    J: JitterSource,
{
    let lease = worker.config.processing_lease();
    let mut heartbeat =
        tokio::time::interval(lease.to_std().unwrap_or(Duration::from_secs(90)) / 3);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let request = worker.transport.send(&input);
    tokio::pin!(request);
    loop {
        tokio::select! {
            result = &mut request => return Some(result),
            _ = heartbeat.tick() => {
                match worker.store.renew_lease(input.message.job_id, input.token, lease).await {
                    Ok(true) => {},
                    Ok(false) => return None,
                    Err(error) => {
                        warn!(job_id = %input.message.job_id, error = %error, "delivery lease renewal failed");
                        return None;
                    }
                }
            }
        }
    }
}

async fn send_http(client: &HttpClient, input: &DeliveryInput) -> HttpResult {
    let content_type = match input.payload_type.as_str() {
        "json" => "application/json",
        "text" => "text/plain; charset=utf-8",
        _ => {
            return HttpResult {
                status: None,
                body: None,
                transport_error: Some("unsupported persisted payload type".to_owned()),
            }
        }
    };
    let response = match client
        .post(&input.url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(input.payload.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return HttpResult {
                status: None,
                body: None,
                transport_error: Some(error.to_string()),
            }
        }
    };
    let status = response.status().as_u16();
    let (body, body_error) = read_response_body(response).await;
    HttpResult {
        status: Some(status),
        body,
        transport_error: body_error,
    }
}

async fn read_response_body(mut response: reqwest::Response) -> (Option<String>, Option<String>) {
    const MAX_BODY_BYTES: usize = 64 * 1024;
    const TRUNCATION_MARKER: &str = "\n[response body truncated at 65536 bytes]";
    let mut body = Vec::with_capacity(MAX_BODY_BYTES.min(8 * 1024));
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = MAX_BODY_BYTES.saturating_add(1).saturating_sub(body.len());
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if body.len() > MAX_BODY_BYTES || chunk.len() > remaining {
                    truncated = true;
                    body.truncate(MAX_BODY_BYTES);
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                return (
                    Some(String::from_utf8_lossy(&body).into_owned()),
                    Some(error.to_string()),
                )
            }
        }
    }
    let mut text = if truncated {
        body.truncate(MAX_BODY_BYTES.saturating_sub(TRUNCATION_MARKER.len()));
        String::from_utf8_lossy(&body).into_owned()
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    if truncated {
        while text.len() + TRUNCATION_MARKER.len() > MAX_BODY_BYTES {
            text.pop();
        }
        text.push_str(TRUNCATION_MARKER);
    }
    let body = if text.is_empty() { None } else { Some(text) };
    (body, None)
}

#[cfg(test)]
mod tests {
    use super::{send_http, DeliveryInput};
    use crate::webhook_delivery::postgres::QueueMessage;
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use uuid::Uuid;

    fn input(url: String) -> DeliveryInput {
        DeliveryInput {
            message: QueueMessage {
                job_id: Uuid::new_v4(),
                webhook_id: Uuid::new_v4(),
                attempt_number: 1,
            },
            url,
            payload: "{\"event\":\"created\"}".to_owned(),
            payload_type: "json".to_owned(),
            max_attempts: 5,
            token: Uuid::new_v4(),
        }
    }

    async fn local_server() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local HTTP test server");
        let address = listener.local_addr().expect("local server address");
        (listener, address)
    }

    async fn request_headers(stream: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.expect("read HTTP request");
            assert!(read > 0, "request ended before headers");
            request.extend_from_slice(&chunk[..read]);
        }
        let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
        assert!(request.starts_with("post "));
        assert!(request.contains("content-type: application/json"));
    }

    fn http_client(timeout: Duration) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .expect("build test client")
    }

    #[tokio::test]
    async fn sends_post_without_following_redirects() {
        let (listener, address) = local_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            request_headers(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write redirect response");
        });
        let result = send_http(
            &http_client(Duration::from_secs(2)),
            &input(format!("http://{address}/hook")),
        )
        .await;
        assert_eq!(result.status, Some(302));
        assert_eq!(result.transport_error, None);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn truncates_large_response_body_at_sixty_four_kibibytes() {
        let (listener, address) = local_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            request_headers(&mut stream).await;
            let body = vec![b'a'; 70_000];
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
            stream.write_all(&body).await.expect("write response body");
        });
        let result = send_http(
            &http_client(Duration::from_secs(2)),
            &input(format!("http://{address}/hook")),
        )
        .await;
        let body = result.body.expect("body should be retained");
        assert_eq!(result.status, Some(500));
        assert!(body.ends_with("[response body truncated at 65536 bytes]"));
        assert!(body.len() <= 65_536);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn records_http_timeout_as_a_retryable_transport_failure() {
        let (listener, address) = local_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            request_headers(&mut stream).await;
            tokio::time::sleep(Duration::from_millis(150)).await;
        });
        let result = send_http(
            &http_client(Duration::from_millis(50)),
            &input(format!("http://{address}/hook")),
        )
        .await;
        assert_eq!(result.status, None);
        assert!(result.transport_error.is_some());
        server.await.expect("server task");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn publishes_sends_and_records_a_webhook_delivery(
        pool: sqlx::PgPool,
    ) -> Result<(), sqlx::Error> {
        use crate::webhook::{
            model::{NewWebhook, PayloadType},
            repository::WebhookRepository,
        };

        let (listener, address) = local_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept delivery request");
            request_headers(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\ndelivered",
                )
                .await
                .expect("write delivery response");
        });
        let user_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO users (name, api_key_hash) VALUES ('worker e2e user', $1) RETURNING id",
        )
        .bind(Uuid::new_v4().to_string())
        .fetch_one(&pool)
        .await?;
        let webhook_id = crate::webhook::repository::PostgresWebhookRepository::new(pool.clone())
            .insert(NewWebhook {
                user_id,
                url: format!("http://{address}/hook"),
                payload: "{\"event\":\"created\"}".to_owned(),
                payload_type: PayloadType::Json,
            })
            .await
            .expect("create webhook and its outbox job");
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_owned());
        let redis_client = redis::Client::open(redis_url).expect("valid Redis test URL");
        let redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
            .await
            .expect("connect to Redis test service");
        let queue = super::super::redis_queue::RedisDeliveryQueue::with_prefix(
            redis_client,
            redis_manager,
            &format!("webhook:delivery:e2e:{}", Uuid::new_v4()),
        );
        queue
            .ensure_group()
            .await
            .expect("create test consumer group");
        let worker = Arc::new(super::Worker {
            store: super::super::postgres::PostgresDeliveryStore::new(pool.clone()),
            queue,
            transport: super::ReqwestTransport(http_client(Duration::from_secs(2))),
            clock: super::SystemClock,
            jitter: super::RandomJitter,
            config: super::WorkerConfig {
                concurrency: 1,
                http_timeout: Duration::from_secs(2),
                backoff_base: 5,
                backoff_max: 300,
                jitter_max_percent: 20,
                poll_interval: Duration::from_millis(50),
                reconcile_interval: Duration::from_secs(60),
                batch_size: 10,
            },
        });

        tokio::time::timeout(Duration::from_secs(5), super::publish_tick(&worker))
            .await
            .expect("outbox publisher stalled")
            .expect("publish initial outbox job");
        let messages = worker
            .queue
            .read_new("e2e-consumer", 1, Duration::from_millis(100))
            .await
            .expect("read published delivery");
        assert_eq!(messages.len(), 1);
        let (stream_id, message, _) = messages.into_iter().next().expect("one message");
        let message = message.expect("valid queue message");
        assert_eq!(message.webhook_id, webhook_id);
        tokio::time::timeout(
            Duration::from_secs(5),
            super::process_message(worker, stream_id, message),
        )
        .await
        .expect("delivery processing stalled");
        let job_status: String = sqlx::query_scalar(
            "SELECT status FROM webhook_delivery_jobs WHERE webhook_id = $1 AND attempt_number = 1",
        )
        .bind(webhook_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(job_status, "completed");
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("local HTTP server stalled")
            .expect("HTTP server task");

        let delivery_status: String =
            sqlx::query_scalar("SELECT status FROM webhook_deliveries WHERE webhook_id = $1")
                .bind(webhook_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(delivery_status, "delivered");
        let attempt: (Option<i16>, Option<String>) = sqlx::query_as(
            "SELECT http_status, response_body FROM webhook_attempts WHERE webhook_id = $1",
        )
        .bind(webhook_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(attempt, (Some(200), Some("delivered".to_owned())));
        Ok(())
    }
}
