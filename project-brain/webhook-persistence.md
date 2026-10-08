---
name: webhook-persistence
description: Describes PostgreSQL persistence for webhook requests, delivery coordination, and append-only attempt history.
tags:
  - postgres
  - webhooks
  - persistence
  - sqlx
---

# Webhook Persistence

## Language

Project code, identifiers, comments, error messages, and technical documentation use English.

## Initial Schema

- The current schema is represented by one baseline migration in `backend/migrations/`. During this early development phase, schema changes can be consolidated into that baseline; reset the local PostgreSQL database before applying a replaced baseline.
- `users` stores a UUID, name, JSONB configuration, a unique API key hash, and creation/update timestamps.
- `webhooks` stores the user, destination URL, raw text payload, payload category (`json` or `text`), and creation timestamp.
- `webhook_attempts` is append-only and references `webhooks`. PostgreSQL triggers reject updates, deletes, and truncation.
- Attempts record completed deliveries only. The caller supplies start/completion timestamps, measured latency, and an HTTP status and/or error; response text is optional. Attempt numbering and retry coordination belong to delivery jobs, not this append-only audit table. Repeated inputs create separate audit rows.
- `webhook_deliveries` stores the per-webhook retry limit and current delivery state. `webhook_delivery_jobs` stores each logical attempt, its due time, publication and processing leases, and the ID of its completed audit record.
- Webhook creation locks and reads the owning user's configuration, validates `delivery.max_attempts` (default 5; valid range 1–100), and commits the webhook, delivery state, and initial job together. Invalid delivery configuration rejects creation without partial persistence.

## Rust Boundary

The `webhook` domain is organized under `backend/src/webhook/`: its service validates webhook invariants and delegates persistence through a repository trait; the PostgreSQL repository maps the `webhooks.user_id` foreign-key violation to a missing-user error. The HTTP request parser validates the incoming body before invoking the service. User lookup and API-key authentication live under `backend/src/user/`; the authentication flow hashes the raw API key, and the user repository queries by that hash and returns the user's ID and configuration. Shared HTTP errors, API versioning, status endpoints, and route composition live under `backend/src/http/`. See [backend-module-structure](backend-module-structure.md) for guidance on adding modules. Database migrations are applied manually by running the `migrate` binary in the backend container. When replacing the baseline in local development, stop database clients, recreate only the configured PostgreSQL database, and apply the migration; do not remove the Compose volumes unless Redis and pgAdmin data should also be cleared.

`WebhookRepository::add_attempt` persists a validated `NewWebhookAttempt` and returns the PostgreSQL-generated UUID. The input's private fields and constructor preserve the database check constraints in Rust; a missing parent webhook produces `WebhookNotFound`. This internal capability has no HTTP route and is not invoked by ingestion. Its PostgreSQL integration tests use isolated databases via `sqlx::test`; validation and error-mapping unit tests require no external services.

## Webhook API

- The backend exposes `POST /api/webhooks` for creation and `GET /api/webhooks` for listing. The `X-API-Version` header defaults to `v1`; `/` and `/health` are not versioned.
- Requests require `X-API-Key`. The authentication flow computes the lowercase hexadecimal SHA-256 digest of the key and passes that hash to the user repository, which looks it up in `users.api_key_hash`. The authenticated user's UUID is attached to the request and is the only source of `webhooks.user_id`.
- Creation requests require `Content-Type: application/json` and accept only `url` and `payload` string fields plus optional `payload_type` (`json` or `text`). Missing `payload_type` defaults to `json`; JSON payloads are encoded as strings and validated as JSON before persistence. The previous singular `POST /api/webhook` path is not supported.
- The listing accepts optional `cursor` (a webhook UUID) and `count` (1–100, default 20). It returns `items` and `next_cursor`, orders by `created_at DESC, id DESC`, and scopes both items and cursor lookup to the authenticated user. Each item includes `id`, `url`, `created_at`, `status`, `attempt_count`, `http_status`, `error_message`, and `completed_at`; the last three fields are `null` before the first completed attempt. The listing omits `payload_type` and does not nest the last attempt.
- Require JSON request content types only for routes that accept JSON bodies. Middleware order for webhook creation is API version, Content-Type, authentication, then handler, so an invalid content type returns 415 before API-key lookup. Listing requests do not require a Content-Type header.
- Successful creation returns HTTP 201 and the new webhook ID once the webhook and outbox job are committed. HTTP delivery remains asynchronous, and no completed attempt exists until a worker records a result.
- `GET /api/webhooks/{id}` returns an authenticated user's webhook configuration and all completed attempts in chronological order. The response includes the stored payload as `body`, the effective method (`POST`) and configured `Content-Type`, delivery status/limit, and the next pending job time (or `null` while processing or after completion). Attempts expose timestamps, HTTP status, latency, response body, error message, and a `success`/`failure` result; success requires a 2xx status and no recorded error. A malformed UUID returns `400`; missing and other users' webhooks both return `404`.

## Delivery Worker

- The standalone `webhook-worker` binary shares the backend crate and runs as its own Docker Compose service. Scale it with `docker compose up -d --scale webhook-worker=N`.
- PostgreSQL is the durable source of truth and outbox. Redis 7.4 uses `webhook:delivery:ready` as the consumer-group stream, `webhook:delivery:scheduled` as the retry sorted set, and `webhook:delivery:references` to deduplicate current queue entries.
- Delivery uses at-least-once processing. A crash after a destination receives HTTP but before PostgreSQL commits the result can cause the same logical attempt to be sent again. Redis redelivery does not itself increment the logical attempt number.
- Retryable outcomes are network/timeout failures and HTTP 408, 429, and 5xx. Any 2xx status succeeds; other HTTP statuses end the delivery. Backoff uses exponential delay, bounded jitter, and a 300-second cap.
- Worker configuration is supplied through `DELIVERY_CONCURRENCY`, `DELIVERY_HTTP_TIMEOUT_SECS`, `DELIVERY_BACKOFF_BASE_SECS`, `DELIVERY_BACKOFF_MAX_SECS`, `DELIVERY_BACKOFF_JITTER_PERCENT`, `DELIVERY_POLL_INTERVAL_MS`, `DELIVERY_RECONCILE_INTERVAL_SECS`, and `DELIVERY_BATCH_SIZE`.
- Apply PostgreSQL migrations in the backend container before creating webhooks or starting workers. Existing webhook rows are intentionally not backfilled.
- Request errors use an `errors` array containing `field` and `reason`. Unknown, duplicate, and malformed fields are rejected.
