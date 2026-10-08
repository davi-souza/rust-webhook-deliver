use crate::webhook_delivery::postgres::QueueMessage;
use redis::{
    aio::ConnectionManager,
    streams::{StreamAutoClaimReply, StreamReadReply},
    Client, Script,
};
use std::time::Duration;
use uuid::Uuid;

const GROUP: &str = "webhook-delivery-workers";

const PUBLISH_SCRIPT: &str = r#"
local job_id = ARGV[1]
local member = ARGV[2]
local webhook_id = ARGV[3]
local attempt_number = ARGV[4]
local available_at = tonumber(ARGV[5])
local existing = redis.call('HGET', KEYS[3], job_id)
if existing == 'd' then
  if redis.call('ZSCORE', KEYS[2], member) then return 0 end
  redis.call('HDEL', KEYS[3], job_id)
elseif existing then
  local stream_id = string.sub(existing, 3)
  if redis.call('XRANGE', KEYS[1], stream_id, stream_id, 'COUNT', 1)[1] then return 0 end
  redis.call('HDEL', KEYS[3], job_id)
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if available_at > now then
  redis.call('ZADD', KEYS[2], available_at, member)
  redis.call('HSET', KEYS[3], job_id, 'd')
  return 2
end
local id = redis.call('XADD', KEYS[1], '*', 'job_id', job_id, 'webhook_id', webhook_id, 'attempt_number', attempt_number)
redis.call('HSET', KEYS[3], job_id, 's:' .. id)
return 1
"#;

const PROMOTE_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local promoted = 0
for _, member in ipairs(due) do
  local item = cjson.decode(member)
  local id = redis.call('XADD', KEYS[1], '*', 'job_id', item.job_id, 'webhook_id', item.webhook_id, 'attempt_number', item.attempt_number)
  redis.call('HSET', KEYS[3], item.job_id, 's:' .. id)
  redis.call('ZREM', KEYS[2], member)
  promoted = promoted + 1
end
return promoted
"#;

const ACK_SCRIPT: &str = r#"
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[3] ~= '' and redis.call('HGET', KEYS[2], ARGV[3]) == ('s:' .. ARGV[2]) then
  redis.call('HDEL', KEYS[2], ARGV[3])
end
return 1
"#;

#[derive(Clone)]
pub(crate) struct RedisDeliveryQueue {
    client: Client,
    manager: ConnectionManager,
    stream: String,
    delayed: String,
    references: String,
}

impl RedisDeliveryQueue {
    pub(crate) fn new(client: Client, manager: ConnectionManager) -> Self {
        Self::with_prefix(client, manager, "webhook:delivery")
    }

    pub(super) fn with_prefix(client: Client, manager: ConnectionManager, prefix: &str) -> Self {
        Self {
            client,
            manager,
            stream: format!("{prefix}:ready"),
            delayed: format!("{prefix}:scheduled"),
            references: format!("{prefix}:references"),
        }
    }

    pub(crate) async fn ensure_group(&self) -> Result<(), String> {
        let mut connection = self.manager.clone();
        let result: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream)
            .arg(GROUP)
            .arg("0-0")
            .arg("MKSTREAM")
            .query_async(&mut connection)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub(crate) async fn publish(
        &self,
        job: &crate::webhook_delivery::postgres::ReservedJob,
    ) -> Result<(), String> {
        let member = serde_json::to_string(&job.message).map_err(|error| error.to_string())?;
        let mut connection = self.manager.clone();
        Script::new(PUBLISH_SCRIPT)
            .key(&self.stream)
            .key(&self.delayed)
            .key(&self.references)
            .arg(job.message.job_id.to_string())
            .arg(member)
            .arg(job.message.webhook_id.to_string())
            .arg(job.message.attempt_number)
            .arg(job.available_at.timestamp_millis())
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn promote_due(&self, count: usize) -> Result<(), String> {
        let mut connection = self.manager.clone();
        Script::new(PROMOTE_SCRIPT)
            .key(&self.stream)
            .key(&self.delayed)
            .key(&self.references)
            .arg(count)
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn read_new(
        &self,
        consumer: &str,
        count: usize,
        block: Duration,
    ) -> Result<Vec<(String, Option<QueueMessage>, Option<Uuid>)>, String> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| error.to_string())?;
        let reply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block.as_millis() as u64)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async::<StreamReadReply>(&mut connection)
            .await
            .map_err(|error| error.to_string())?;
        Ok(reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids)
            .map(|entry| parse_entry(entry.id, entry.map))
            .collect())
    }

    pub(crate) async fn claim_idle(
        &self,
        consumer: &str,
        min_idle_time: Duration,
        cursor: &str,
        count: usize,
    ) -> Result<(String, Vec<(String, Option<QueueMessage>, Option<Uuid>)>), String> {
        let mut connection = self.manager.clone();
        let reply = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream)
            .arg(GROUP)
            .arg(consumer)
            .arg(min_idle_time.as_millis() as u64)
            .arg(cursor)
            .arg("COUNT")
            .arg(count)
            .query_async::<StreamAutoClaimReply>(&mut connection)
            .await
            .map_err(|error| error.to_string())?;
        let entries = reply
            .claimed
            .into_iter()
            .map(|entry| parse_entry(entry.id, entry.map))
            .collect();
        Ok((reply.next_stream_id, entries))
    }

    pub(crate) async fn acknowledge(
        &self,
        stream_id: &str,
        job_id: Option<Uuid>,
    ) -> Result<(), String> {
        let mut connection = self.manager.clone();
        Script::new(ACK_SCRIPT)
            .key(&self.stream)
            .key(&self.references)
            .arg(GROUP)
            .arg(stream_id)
            .arg(job_id.map(|id| id.to_string()).unwrap_or_default())
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn acknowledge_malformed(
        &self,
        stream_id: &str,
        job_id: Option<Uuid>,
    ) -> Result<(), String> {
        self.acknowledge(stream_id, job_id).await
    }
}

fn redis_string(value: &redis::Value) -> Option<String> {
    redis::from_redis_value(value).ok()
}

fn parse_entry(
    id: String,
    fields: std::collections::HashMap<String, redis::Value>,
) -> (String, Option<QueueMessage>, Option<Uuid>) {
    let job_id = fields
        .get("job_id")
        .and_then(redis_string)
        .and_then(|value| Uuid::parse_str(&value).ok());
    let message = job_id.and_then(|job_id| {
        let webhook_id = fields
            .get("webhook_id")
            .and_then(redis_string)
            .and_then(|value| Uuid::parse_str(&value).ok())?;
        let attempt_number = fields
            .get("attempt_number")
            .and_then(redis_string)
            .and_then(|value| value.parse().ok())?;
        Some(QueueMessage {
            job_id,
            webhook_id,
            attempt_number,
        })
    });
    (id, message, job_id)
}

#[cfg(test)]
#[tokio::test]
async fn stream_deduplicates_ready_work_and_promotes_due_sorted_set_members() {
    use chrono::{Duration as ChronoDuration, Utc};

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_owned());
    let client = Client::open(redis_url).expect("valid Redis test URL");
    let manager = ConnectionManager::new(client.clone())
        .await
        .expect("connect to Redis test service");
    let prefix = format!("webhook:delivery:test:{}", Uuid::new_v4());
    let queue = RedisDeliveryQueue::with_prefix(client.clone(), manager, &prefix);
    queue.ensure_group().await.expect("create consumer group");

    let ready = crate::webhook_delivery::postgres::ReservedJob {
        message: QueueMessage {
            job_id: Uuid::new_v4(),
            webhook_id: Uuid::new_v4(),
            attempt_number: 1,
        },
        available_at: Utc::now() - ChronoDuration::seconds(1),
        publish_token: Uuid::new_v4(),
    };
    queue.publish(&ready).await.expect("publish ready job");
    queue
        .publish(&ready)
        .await
        .expect("deduplicate repeated job");
    let ready_entries = queue
        .read_new("redis-integration-test", 10, Duration::from_millis(100))
        .await
        .expect("read ready entry");
    assert_eq!(ready_entries.len(), 1);
    assert_eq!(ready_entries[0].1.as_ref(), Some(&ready.message));
    queue
        .acknowledge(&ready_entries[0].0, Some(ready.message.job_id))
        .await
        .expect("acknowledge ready job");

    let scheduled = crate::webhook_delivery::postgres::ReservedJob {
        message: QueueMessage {
            job_id: Uuid::new_v4(),
            webhook_id: Uuid::new_v4(),
            attempt_number: 2,
        },
        available_at: Utc::now() + ChronoDuration::milliseconds(100),
        publish_token: Uuid::new_v4(),
    };
    queue
        .publish(&scheduled)
        .await
        .expect("schedule future job");
    queue.promote_due(10).await.expect("promote due jobs");
    let early = queue
        .read_new("redis-integration-test", 10, Duration::from_millis(50))
        .await
        .expect("check scheduled job is delayed");
    assert!(early.is_empty());
    tokio::time::sleep(Duration::from_millis(100)).await;
    queue
        .promote_due(10)
        .await
        .expect("promote job after its due time");
    let due_entries = queue
        .read_new("redis-integration-test", 10, Duration::from_millis(100))
        .await
        .expect("read promoted job");
    assert_eq!(due_entries.len(), 1);
    assert_eq!(due_entries[0].1.as_ref(), Some(&scheduled.message));
    queue
        .acknowledge(&due_entries[0].0, Some(scheduled.message.job_id))
        .await
        .expect("acknowledge promoted job");

    let abandoned = crate::webhook_delivery::postgres::ReservedJob {
        message: QueueMessage {
            job_id: Uuid::new_v4(),
            webhook_id: Uuid::new_v4(),
            attempt_number: 1,
        },
        available_at: Utc::now() - ChronoDuration::seconds(1),
        publish_token: Uuid::new_v4(),
    };
    queue
        .publish(&abandoned)
        .await
        .expect("publish reclaim test job");
    let delivered_to_failed_consumer = queue
        .read_new("failed-test-consumer", 1, Duration::from_millis(100))
        .await
        .expect("deliver to first consumer");
    assert_eq!(delivered_to_failed_consumer.len(), 1);
    tokio::time::sleep(Duration::from_millis(25)).await;
    let (_, reclaimed) = queue
        .claim_idle(
            "replacement-test-consumer",
            Duration::from_millis(20),
            "0-0",
            1,
        )
        .await
        .expect("reclaim abandoned consumer message");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].1.as_ref(), Some(&abandoned.message));
    queue
        .acknowledge(&reclaimed[0].0, Some(abandoned.message.job_id))
        .await
        .expect("acknowledge reclaimed job");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("reconnect to Redis test service");
    redis::cmd("DEL")
        .arg(&queue.stream)
        .arg(&queue.delayed)
        .arg(&queue.references)
        .query_async::<i64>(&mut connection)
        .await
        .expect("clean up test keys");
}
