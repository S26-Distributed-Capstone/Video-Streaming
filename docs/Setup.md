# Frontend Setup (Upload Service + UI)

## Prerequisites

- Java 17+
- Maven
- Docker & Docker Compose
- FFmpeg + FFprobe in PATH
- Postgres (for upload info + totalSegments)

## One-Time Setup

### 1) Start Object Storage (MinIO)

```bash
docker-compose -f minio_docker_compose.yaml up -d OR 
docker compose -f minio_docker_compose.yaml up -d
```

MinIO runs at `http://localhost:9000` (default creds: `minioadmin` / `minioadmin`).

### 2) Set Up Postgres and Tables

```bash
createdb videostreaming
psql -d videostreaming -f upload-service/docs/db/schema.sql
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
