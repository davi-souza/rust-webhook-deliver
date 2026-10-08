---
name: webhook-delivery
description: Describes the backend webhook worker, PostgreSQL outbox, Redis queue and retry scheduling, delivery semantics, and operating configuration.
tags:
  - webhooks
  - delivery
  - redis
  - retries
  - worker
---

# Webhook Delivery

## Responsibilities

The `webhook_delivery` backend domain runs asynchronous delivery in a standalone Rust binary. Webhook ingestion commits the webhook, a `webhook_deliveries` state row, and attempt 1 in `webhook_delivery_jobs` in a single PostgreSQL transaction. The `webhook_attempts` table remains append-only and records completed HTTP outcomes; the delivery job stores the logical attempt number and audit ID.

## Queue and Recovery

PostgreSQL is the durable source of truth. The worker reserves due outbox rows with `FOR UPDATE SKIP LOCKED`, publishes them to Redis outside the transaction, and releases each publication lease. Redis 7.4 uses a consumer-group stream (`webhook:delivery:ready`), a Sorted Set (`webhook:delivery:scheduled`), and a hash of active queue references (`webhook:delivery:references`). Lua scripts deduplicate publication, promote due retries, and acknowledge completed stream entries atomically.

The publisher promotes and publishes in bounded batches, while periodically reconciling pending PostgreSQL jobs against the Redis queue. Workers use unique consumer names, reclaim idle stream messages with `XAUTOCLAIM`, and claim each job in PostgreSQL with an expiring processing token. The token fences stale workers from committing results. The completed audit row, delivery state, and next retry job commit in one database transaction before `XACK`.

Delivery is at least once. If the destination accepted an HTTP request but the worker crashed before its database commit, the same logical attempt can be sent again. Redis message redelivery does not advance the attempt number; only a completed retryable HTTP outcome creates the next numbered job.

## HTTP and Retry Policy

Workers send POST with the persisted payload and JSON or text content type. Redirects and the HTTP client's automatic retries are disabled. The request timeout defaults to 30 seconds. Up to 64 KiB of response text is recorded, with a truncation marker.

Any 2xx status succeeds. Network failures, timeouts, 408, 429, and 5xx are retryable; other HTTP statuses are terminal. The default per-user limit is five total attempts, read from `users.configuration.delivery.max_attempts` when the webhook is created. Explicit values must be integers from 1 through 100. Backoff defaults to 5 seconds doubled for each retry, with up to 20% positive jitter and a 300-second cap. `Retry-After` is ignored.

## Local Operation

Run `docker compose exec backend cargo run --bin migrate` before using the new delivery flow. The `webhook-worker` Compose service is independent of the API and can be scaled with `docker compose up -d --scale webhook-worker=N`. Its concurrency, request timeout, backoff, poll and reconciliation intervals, and batch size are configured with the `DELIVERY_*` environment variables listed in `.env.example`. Existing webhooks are not automatically enqueued by the migration.
