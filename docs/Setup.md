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
- PostgreSQL credentials (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`)

### 2) Start All Services (MinIO, RabbitMQ, PostgreSQL)

```bash
docker-compose -f docker_compose.yaml up -d
# OR
docker compose -f docker_compose.yaml up -d
```

This starts:
- **MinIO** at `http://localhost:9000` (console at `http://localhost:9001`)
- **RabbitMQ** at `http://localhost:15672`
- **PostgreSQL** at `localhost:5432`

### 3) Set Up Postgres Tables

Once PostgreSQL is running, apply the schema:

```bash
docker exec -i video-streaming-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB < upload-service/docs/db/schema.sql
```

Or source your `.env` first and use psql directly:
```bash
source .env
psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -f upload-service/docs/db/schema.sql
```

## Run (Frontend + Upload Service)

From the repo root:
```bash
mvn install -DskipTests
mvn exec:java -pl upload-service -Dexec.mainClass="com.distributed26.videostreaming.upload.upload.UploadServiceApplication"
```

Then open:
- `http://localhost:8080/`

## Notes

- Frontend assets live in `/frontend` and are served by the upload service.
- Logs are written to `logs/upload-service.log` (and the service also prints to console).
- All data is persisted in Docker named volumes (`rabbitmq_data`, `minio_data`, `postgres_data`).
- The application reads credentials from the `.env` file in the project root.
