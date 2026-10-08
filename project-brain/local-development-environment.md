---
name: local-development-environment
description: Architecture, configuration, lifecycle, and operations for the containerized local development environment.
tags:
  - docker
  - development
  - postgres
  - redis
  - pgadmin
  - rust
  - react
  - frontend
---

# Local Development Environment

The entire development workflow runs exclusively inside Docker containers via Docker Compose. No local installations of Rust, Node.js, PostgreSQL, or Redis are required on the host system.

## Services Architecture

| Service | Base Image | Port (Host:Container) | Purpose |
| :--- | :--- | :--- | :--- |
| **`frontend`** | `node:22.23-alpine` | `5173:5173` | React 19 + TypeScript + Vite UI with HMR polling and `/api` reverse proxy |
| **`backend`** | `rust:1.98.1-alpine` | `8000:8000` | Axum HTTP backend with hot-reload via `cargo-watch` |
| **`webhook-worker`** | `rust:1.98.1-alpine` | — | Independently scalable Rust consumer for asynchronous webhook delivery |
| **`postgres`** | `postgres:17.4-alpine` | `5432:5432` | Relational database for webhooks and delivery audit logs |
| **`redis`** | `redis:7.4-alpine` | `6379:6379` | Queue storage, broker, and fast key-value caching |
| **`pgadmin`** | `dpage/pgadmin4:9.1` | `5050:80` | Web UI for PostgreSQL inspection and management |

## Image Optimization & Version Pinning

To guarantee minimal disk footprint and build consistency across team members:
- All base images utilize **Alpine Linux**.
- Exact software versions are pinned:
  - Frontend: `node:22.23-alpine` (Node 22 LTS)
  - Rust Backend: `rust:1.98.1-alpine`
  - PostgreSQL: `postgres:17.4-alpine`
  - Redis: `redis:7.4-alpine`
  - pgAdmin 4: `dpage/pgadmin4:9.1`

### Pre-installed Tooling
- **Rust Backend**:
  - `rustfmt`: Code style consistency.
  - `clippy`: Static analysis and idiomatic linting.
  - `cargo-watch`: Watches source files on the host and automatically recompiles the application upon file changes.
  - `musl-dev`, `gcc`, `openssl-dev`, `pkgconfig`: Native compilation dependencies.
- **Frontend**:
  - `npm`, `vite`, `typescript`: Hot Module Replacement (HMR) with file polling enabled for container volume synchronicity.

## Volume Mounts and Caching Strategy

The Docker Compose configuration isolates build artifacts and caches dependencies in named volumes to preserve build performance and prevent host pollution:
- `./backend:/app`: Bind mount exposing backend source code for instant hot-reload.
- `backend_target:/app/target`: Prevents compiled Linux binaries from synchronizing back to macOS/host filesystem, speeding up I/O.
- `cargo_cache:/usr/local/cargo/registry` & `cargo_git:/usr/local/cargo/git`: Caches downloaded crates across restarts and rebuilds.
- `./frontend:/app`: Bind mount exposing frontend source code for instant HMR.
- `frontend_node_modules:/app/node_modules`: Preserves containerized Linux dependencies, avoiding host pollution.
- `postgres_data:/var/lib/postgresql/data`: Persists database tables across container restarts.
- `redis_data:/data`: Persists Redis append-only file (AOF).

## Preconfigured pgAdmin 4

pgAdmin automatically imports the `RustWebhook PostgreSQL` server connection upon container initialization through [servers.json](file:///Users/davisouza/Projects/RustWebhook/docker/pgadmin/servers.json).

- **URL**: [http://localhost:5050](http://localhost:5050)
- **Login Email**: `admin@example.com` (configurable via `.env`)
- **Login Password**: `admin` (configurable via `.env`)
- **Database Password**: `postgres` (or as set in `POSTGRES_PASSWORD`)

## Operational Workflows

### 1. Starting the Environment
Start all containers in the background and trigger image builds if necessary:
```bash
docker compose up -d --build
```
Or start in foreground to view all service logs in real time:
```bash
docker compose up
```

### 2. Dependency Healthchecks & Communication
The `backend` container waits for both `postgres` (`pg_isready`) and `redis` (`redis-cli ping`) healthchecks to pass before starting. The `frontend` proxies all requests starting with `/api` directly to `backend:8000`.

Verify the status of all services:
```bash
docker compose ps
```

The `webhook-worker` service runs independently from the HTTP API and can be scaled without changing the API service:
```bash
docker compose up -d --scale webhook-worker=3
```

Apply new database migrations before relying on webhook delivery:
```bash
docker compose exec backend cargo run --bin migrate
```

Verify service connectivity through the HTTP health endpoint:
```bash
curl http://localhost:8000/health
# Or through frontend proxy:
curl http://localhost:5173/api/health
```
Expected output:
```json
{"dependencies":{"postgres":"connected","redis":"connected"},"status":"healthy"}
```

### 3. Code Quality and Testing inside Containers

In accordance with project rules, all CLI commands must run inside containers:

- **Backend Format**:
  ```bash
  docker compose exec backend cargo fmt
  ```
- **Backend Lint (Clippy)**:
  ```bash
  docker compose exec backend cargo clippy -- -D warnings
  ```
- **Backend Tests**:
  ```bash
  docker compose exec backend cargo test
  ```
- **Worker Tests** (including Redis queue integration, with the Compose Redis service running):
  ```bash
  docker compose exec backend cargo test webhook_delivery
  ```
- **Frontend Typecheck & Build**:
  ```bash
  docker compose exec frontend npm run build
  ```

### 4. Stopping the Environment
Stop running containers without destroying data volumes:
```bash
docker compose down
```

To stop containers and wipe volumes (database & redis clean slate):
```bash
docker compose down -v
```
