# Frontend Setup (Upload Service + UI)

## Prerequisites

- Java 17+
- Maven
- Docker & Docker Compose
- FFmpeg + FFprobe in PATH

## One-Time Setup

### 1) Configure Environment Variables

Copy the example `.env` file and configure your secrets:

```bash
cp .env.example .env
```

Edit `.env` with your credentials. See `.env` for required variables:
- MinIO credentials (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`)
- RabbitMQ credentials (`RABBITMQ_USER`, `RABBITMQ_PASS`)
- PostgreSQL credentials (`PG_USER`, `PG_PASSWORD`, `PG_DB`)
- Browser-facing MinIO endpoint for playback (`MINIO_PUBLIC_ENDPOINT`, usually `http://localhost:9000` in local dev)

### 2) Start All Services (Choose One)

Use **Docker Compose** *or* **Docker Swarm**. You do not need both.

#### Option A: Docker Compose
```bash
docker compose up -d --build
```

This starts:
- **MinIO** at `http://localhost:9000` (console at `http://localhost:9001`)
- **RabbitMQ** at `http://localhost:15672`
- **PostgreSQL** at `localhost:5432`
- **Upload Service** at `http://localhost:8080` (container `upload-service`)
- **Status Service** at `http://localhost:8081` (container `status-service`)
- **Processing Service** at `http://localhost:8082` (container `processing-service`)
- **Streaming Service** at `http://localhost:8083` (container `streaming-service`)
- **Node Watcher** as a background container for terminal failure detection

RabbitMQ is used in two separate ways:
- status event bus: upload/status/progress fan-out for the browser and status service
- transcode task bus: distributed `(segment, profile)` work queue consumed by processing-service

#### Option B: Docker Swarm
See **Run With Docker Swarm (Single Machine)** below.

### 3) Postgres Schema (Auto-Loaded)

The schema is mounted into the Postgres container and **automatically applied on first startup**:
```
upload-service/docs/db/schema.sql -> /docker-entrypoint-initdb.d/schema.sql
```

If you already have a `postgres_data` volume and need to re-run the schema:
Note: `docker compose down -v` deletes **all** Postgres data, not just the schema.

A safer alternative for existing installations is to apply the schema manually:
```bash
source .env
psql -h localhost -U "$PG_USER" -d "$PG_DB" -f upload-service/docs/db/schema.sql
```

If you are OK with wiping all data, you can reset the volume:
```bash
docker compose down -v
docker compose up -d --build
```

## Run With External Shared Infrastructure

Use this mode when you want the application services to connect to a Postgres, RabbitMQ, and MinIO instance that are outside the swarm stack. This is the easiest way to simulate "many machines connect to one shared database" on a single computer.

### 1) Start infrastructure separately

You can use plain Docker containers, Docker Compose, or services installed directly on your host. The important part is that these ports are reachable from the application containers:

- PostgreSQL on `5432`
- RabbitMQ on `5672`
- MinIO on `9000`

### 2) Point `.env` at the external endpoints

For Docker Desktop on macOS, `host.docker.internal` is usually the easiest choice:

```env
PG_URL=jdbc:postgresql://host.docker.internal:5432/videostreaming
PG_USER=postgres
PG_PASSWORD=postgres
PG_DB=videostreaming

RABBITMQ_HOST=host.docker.internal
RABBITMQ_PORT=5672

MINIO_ENDPOINT=http://host.docker.internal:9000
MINIO_PUBLIC_ENDPOINT=http://host.docker.internal:9000
```

If you want to mimic the real multi-machine setup more closely, replace `host.docker.internal` with your laptop's LAN IP instead.

### 3) Apply the schema once

```bash
source .env
psql -h localhost -U "$PG_USER" -d "$PG_DB" -f upload-service/docs/db/schema.sql
```

If your Postgres instance is not published on `localhost`, replace the host accordingly.

### 4) Deploy only the application stack

```bash
./deploy_swarm_external.sh
```

This deploys:
- `upload-service`
- `status-service`
- `processing-service`
- `streaming-service`
- `node-watcher`

It does not deploy local `postgres`, `rabbitmq`, or `minio` services into the swarm.

### 5) Stop the external-infra test stack

```bash
docker stack rm video-external
```

## Run (Frontend + Upload Service)

With Docker Compose running, open:
- `http://localhost:8080/`

To stream a completed video:
1. Upload a video and wait until it is processed.
2. In the UI, use the "Ready Videos" list and click **Play Selected**.
3. The player uses HLS (Safari native, `hls.js` for other browsers) and streams from `http://localhost:8083`.

## Run With Docker Swarm (Single Machine)

### 1) Deploy the Swarm stack
```bash
./deploy_swarm.sh
```

This will:
- Create the `video_default` overlay network (if missing)
- Deploy the stack from `docker_compose.swarm.yaml`
- Start 3 replicas each of the upload, processing, and streaming services

Note: `docker stack deploy` does not automatically load `.env`, so `deploy_swarm.sh`
exports `.env` into the environment before deploying.

Swarm ports:
- Upload Service: `http://localhost:8080`
- Status Service: `http://localhost:8081`
- Processing Service: `http://localhost:8082`
- Streaming Service: `http://localhost:8083`

### 3) Stop the Swarm stack
```bash
docker stack rm video
```

## Notes

- Frontend assets live in `/frontend` and are served by the upload service.
- Logs are written to `logs/upload-service.log` (and the service also prints to console).
- All data is persisted in Docker named volumes (`rabbitmq_data`, `minio_data`, `postgres_data`).
- The application reads credentials from the `.env` file in the project root.
- For Swarm, `.env` should use service hostnames (`postgres`, `rabbitmq`, `minio`) rather than `localhost`.
- Status endpoints are served separately at `http://localhost:8081`.
- `SERVICE_MODE` is set in `docker-compose.yaml` to start either the upload or status service.
- The browser connects only to the status service for progress updates.
- `upload-service` publishes both:
  - status events for UI/WebSocket updates
  - transcode task events for processing workers
- `processing-service` consumes only the transcode task queue, writes completed profile outputs into a local spool, and a same-node uploader pushes those files to object storage.
- `processing-service` keeps its durable local-upload handoff in Postgres (`processing_upload_task`) and stores spool files under `PROCESSING_SPOOL_ROOT` (default `processing-spool/`).
- `status-service` now declares a replica-local RabbitMQ queue per instance, so multiple status replicas each receive the full event stream and can safely fan out updates to the WebSockets connected to that replica.
- `status-service` also exposes `GET /upload-info/{videoId}` for a DB-backed progress snapshot.
- `streaming-service` serves manifests only; the browser fetches segments directly from MinIO through presigned URLs.
