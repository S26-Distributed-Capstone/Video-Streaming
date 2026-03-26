# Installation Guide

This document provides step-by-step instructions for installing, configuring, and running the system for the first time.

## Prerequisites

Install the following before starting:

- Java 17 or newer
- Maven
- Docker
- Docker Compose
- FFmpeg
- FFprobe

Make sure `ffmpeg` and `ffprobe` are available on your `PATH`.

## Step 1: Clone The Project

```bash
git clone https://github.com/S26-Distributed-Capstone/Video-Streaming.git
cd Video-Streaming
```

## Step 2: Create The Environment File

Copy the example configuration:

```bash
cp .env.example .env
```

Then edit `.env` and provide values for:

- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET_NAME`
- `MINIO_PUBLIC_ENDPOINT`
- `RABBITMQ_USER`
- `RABBITMQ_PASS`
- `PG_USER`
- `PG_PASSWORD`
- `PG_DB`
- `PG_URL`

For normal local Docker Compose use, the defaults should point to service hostnames inside the Docker network:

```env
MINIO_ENDPOINT=http://minio:9000
MINIO_PUBLIC_ENDPOINT=http://localhost:9000

PG_URL=jdbc:postgresql://postgres:5432/videostreaming
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_DB=videostreaming

RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=your_rabbitmq_user
RABBITMQ_PASS=your_rabbitmq_password
```

## Step 3: Start The System With Docker Compose

For the standard local installation, start the full stack with:

```bash
docker compose up -d --build
```

This starts:

- Postgres on `localhost:5432`
- RabbitMQ on `localhost:15672` and `localhost:5672`
- MinIO on `localhost:9000`
- MinIO console on `localhost:9001`
- upload-service on `localhost:8080`
- status-service on `localhost:8081`
- processing-service on `localhost:8082`
- streaming-service on `localhost:8083`
- node-watcher in the background

## Step 4: Verify Startup

Check that the containers are running:

```bash
docker compose ps
```

You can also verify the main endpoints:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8082/health
curl http://localhost:8083/stream/ready
```

Open these in your browser if needed:

- Upload UI: `http://localhost:8080`
- RabbitMQ UI: `http://localhost:15672`
- MinIO console: `http://localhost:9001`

## Step 5: Initialize Or Reapply The Database Schema

On first startup, the Postgres schema is auto-loaded from:

```text
https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/upload-service/docs/db/schema.sql
```

If you need to reapply the schema manually:

```bash
source .env
psql -h localhost -U "$PG_USER" -d "$PG_DB" -f upload-service/docs/db/schema.sql
```

If you want a full local reset and do not need existing data:

```bash
docker compose down -v
docker compose up -d --build
```

## Step 6: Use The System

1. Open `http://localhost:8080`
2. Upload a video file
3. Watch progress updates in the UI
4. Wait until the video reaches `READY`
5. Play it through the ready-video list

## Optional: Run With Docker Swarm

If you want to run the replicated single-machine swarm version:

```bash
./deploy_swarm.sh
```

This deploys the stack from `docker_compose.swarm.yaml` and starts multiple replicas of the main services.

Swarm endpoints:

- Upload service: `http://localhost:8080`
- Status service: `http://localhost:8081`
- Processing service: `http://localhost:8082`
- Streaming service: `http://localhost:8083`

To stop the swarm stack:

```bash
docker stack rm video
```

## Optional: Run Against External Shared Infrastructure

If you want the application services to use external Postgres, RabbitMQ, and MinIO instead of the in-stack containers:

1. Start Postgres, RabbitMQ, and MinIO separately.
2. Point `.env` at those hosts.
3. Apply the schema once.
4. Deploy the app-only stack.

Example `.env` values for local testing with host-exposed infrastructure:

```env
PG_URL=jdbc:postgresql://host.docker.internal:5432/videostreaming
PG_USER=postgres
PG_PASSWORD=postgres
PG_DB=videostreaming

RABBITMQ_HOST=host.docker.internal
RABBITMQ_PORT=5672

MINIO_ENDPOINT=http://host.docker.internal:9000
MINIO_PUBLIC_ENDPOINT=http://localhost:9000
```

Then run:

```bash
source .env
psql -h localhost -U "$PG_USER" -d "$PG_DB" -f upload-service/docs/db/schema.sql
./deploy_swarm_external.sh
```

To stop the external-infrastructure stack:

```bash
docker stack rm video-external
```

## Troubleshooting

- If upload works but playback does not, check `MINIO_PUBLIC_ENDPOINT`.
- If uploads are accepted but appear paused, check whether the UI or `/upload-info/{videoId}` shows `Retrying MinIO connection`. This means the upload-service is waiting for MinIO and will resume automatically when storage recovers.
- If `http://localhost:8080/ready` returns `503`, the upload-service is running but MinIO is not currently reachable.
- If services start but cannot talk to each other, check `.env` hostnames and ports.
- If Postgres schema errors appear, reapply [upload-service/docs/db/schema.sql](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/upload-service/docs/db/schema.sql).
- If processing does not start, inspect `processing-service` logs and RabbitMQ connectivity.
- If browser progress is missing, inspect `status-service` logs and the WebSocket connection.

## Related Documents

- [docs/api.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/api.md)
- [docs/architecture.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/architecture.md)
- [docs/challenges.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/challenges.md)
