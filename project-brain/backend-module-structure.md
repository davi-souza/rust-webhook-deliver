---
name: backend-module-structure
description: Backend domain module layout and conventions for adding routes, handlers, services, and repositories.
tags:
  - backend
  - rust
  - architecture
  - modules
---

# Backend Module Structure

## Organize by Domain

Keep backend capabilities in domain folders under `backend/src/`, such as `user/` and `webhook/`. Add a new domain folder for a distinct capability instead of placing unrelated code in shared `handlers/`, `services/`, `models/`, or `repositories/` directories. Create only the files needed by that domain; do not add empty placeholder files.

Each domain's `mod.rs` should declare its child modules and expose only the items needed by other modules. Keep implementation in focused files rather than growing `lib.rs`, `main.rs`, or a domain's `mod.rs` into a catch-all.

## Responsibilities

- `model.rs`: domain types shared by the use case and its collaborators.
- `repository.rs`: a repository trait and its persistence implementation. Keep the trait usable with in-memory mocks in unit tests.
- `service.rs`: a domain use case, its invariants, and calls to repository traits. Avoid Axum request/response types here.
- `request.rs`: HTTP input types, parsing, and transport-specific field validation when a route needs them.
- `handler.rs`: adapt Axum extractors and domain results to HTTP responses; translate domain errors to shared HTTP errors.
- `routes.rs`: register routes for this domain and attach route-specific middleware.
- `auth.rs`: user identity lookup and authentication behavior in the `user` domain.

Repository traits that require thread-safe async work expose `impl Future + Send`; their implementations and in-memory mocks may use `async fn`.

These names describe common responsibilities, not a required checklist. Keep a responsibility in the smallest relevant file when it does not justify another module.

## Shared HTTP and Application Startup

Put behavior shared across domains, such as API versioning and the common error response format, in `backend/src/http/`. Keep domain routes beside their handlers, then compose them in `http/routes.rs`. Place root and health status endpoints in `http/status.rs`. Keep `main.rs` responsible for configuration, dependency connections, application state construction, and starting the HTTP server. Keep `lib.rs` for crate-level module declarations and shared application state.

## Adding a Route or Capability

1. Identify the domain that owns the capability and add it under that domain's folder; create a new domain folder when none owns it.
2. Define domain types and a service only when the capability has a use case. Add repository traits for persistence so service tests do not need PostgreSQL or Redis.
3. For HTTP access, add request parsing/validation as needed, a handler that calls the service, and a route registration in the domain's `routes.rs`.
4. Put shared transport behavior in `http/`, and add the domain router to the application composition in `http/routes.rs`.
5. Add focused unit tests alongside the responsible module. Run formatting, tests, and linting inside the designated Docker container.

Preserve existing routes and response contracts unless a feature explicitly requires a contract change. Do not put database queries, domain rules, and Axum response construction together in one file.
