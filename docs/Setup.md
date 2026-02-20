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

### 2) Start All Services (MinIO, RabbitMQ, PostgreSQL)

```bash
docker compose -f docker_compose.yaml up -d --build
```

This starts:
- **MinIO** at `http://localhost:9000` (console at `http://localhost:9001`)
- **RabbitMQ** at `http://localhost:15672`
- **PostgreSQL** at `localhost:5432`
- **Upload Service** at `http://localhost:8080` (container `upload-service`)
- **Status Service** at `http://localhost:8081` (container `status-service`)

### 3) Postgres Schema (Auto-Loaded)

The schema is mounted into the Postgres container and **automatically applied on first startup**:
```
upload-service/docs/db/schema.sql -> /docker-entrypoint-initdb.d/schema.sql
```

If you already have a `postgres_data` volume and need to re-run the schema:
```bash
docker compose -f docker_compose.yaml down -v
docker compose -f docker_compose.yaml up -d --build
```

## Run (Frontend + Upload Service)

With Docker Compose running, open:
- `http://localhost:8080/`

## Notes

- Frontend assets live in `/frontend` and are served by the upload service.
- Logs are written to `logs/upload-service.log` (and the service also prints to console).
- All data is persisted in Docker named volumes (`rabbitmq_data`, `minio_data`, `postgres_data`).
- The application reads credentials from the `.env` file in the project root.
- Status endpoints are served separately at `http://localhost:8081`.
- `SERVICE_MODE` is set in `docker_compose.yaml` to start either the upload or status service.
