# CLAUDE.md — Video Streaming System Reference

Quick-reference guide for AI agents working in this codebase. See `AGENTS.md` for the full project overview.

---

## Project Layout

```
shared/                  — interfaces, domain types, RabbitMQ implementations (used by all services)
upload-service/          — HTTP ingest, segmentation, task dispatch (port 8080; status mode port 8081)
processing-service/      — FFmpeg transcoding workers (port 8082)
streaming-service/       — HLS manifest + presigned URL serving (port 8083)
autoscaler/              — Python Kubernetes autoscaler (port 8084)
node-watcher/            — Python Docker crash detector (publishes failure events)
frontend/                — Static HTML/JS UI
helm/video-streaming/    — Helm chart for Kubernetes
```

No DI framework — services wire everything by hand in `main()`. Config via env vars → `.env` file → hardcoded defaults.

---

## RabbitMQ Architecture

### Single Exchange
All messaging uses one **topic exchange**: `upload.events`

Three logical channels (queues/routing-key groups) live within it:

---

### 1. TranscodeTaskBus — Work Queue
**Purpose:** Distribute one `(chunk × profile)` transcode job to exactly one processing worker.

| Property | Value |
|---|---|
| Queue | `upload.task.queue` (type: **quorum**, durable) |
| Routing key | `upload.task.transcode` (exact) |
| Publisher | `upload-service` — `SegmentUploadCoordinator.publishTranscodeTasks()` |
| Consumer | `processing-service` only (when `SERVICE_MODE=processing`) |

**How exactly-once-per-worker delivery works:**
- `autoAck=false` — message stays in-flight until explicitly acked.
- `channel.basicQos(prefetch)` — each processing container limits concurrent in-flight tasks to `max(1, CPUs * 3/4)` (override with `RABBITMQ_TASK_PREFETCH` or `WORKER_POOL_SIZE`).
- On success → `basicAck(deliveryTag)`.
- On failure → `basicNack(deliveryTag, requeue=true)` — message goes back to the queue for another worker.
- Tasks are **idempotent**: `TranscodingTask.execute()` calls `storageClient.fileExists(outputKey)` before transcoding, so redeliveries are safe.

**Message: `TranscodeTaskEvent`** (type=`transcode_task`)
```json
{
  "type": "transcode_task",
  "jobId": "<videoId>",
  "taskId": "transcode:<profile>:<segmentNumber>",
  "chunkKey": "uploads/<videoId>/chunks/<n>.ts",
  "profile": "LOW|MEDIUM|HIGH",
  "segmentNumber": 3,
  "outputTsOffsetSeconds": 18.0
}
```

---

### 2. StatusEventBus — Fan-out / Progress Events
**Purpose:** Broadcast progress and state changes to all interested parties (UI, processing coordinator, failure handler).

| Property | Value |
|---|---|
| Exchange | `upload.events` (same topic exchange) |
| Routing key | `upload.status.<jobId>` (per-video), ALSO `upload.failure` for `UploadFailedEvent` |
| Status binding | `upload.status.*` (wildcard matches all jobs) |
| Failure binding | `upload.failure` (exact) |
| `autoAck` | **true** — fire-and-forget, no manual ack |

**Queue declarations depend on `SERVICE_MODE`:**
- `status` or `upload` mode → **exclusive auto-delete temporary queue** per replica. Each replica gets every event independently (true fan-out).
- `processing` mode → same exclusive queue for `upload.status.*`, PLUS a separate exclusive queue bound to `upload.failure` (to catch `UploadFailedEvent` for failed-video short-circuit logic).

**Publishers:**
| Publisher | What it sends |
|---|---|
| `upload-service` | `UploadProgressEvent`, `UploadMetaEvent`, `UploadStorageStatusEvent` |
| `processing-service` | `TranscodeProgressEvent`, `UploadStorageStatusEvent` |
| `node-watcher` (Python) | `UploadFailedEvent` (also marks DB `FAILED`) |

**Consumers:**
| Consumer | What it does |
|---|---|
| `status-service` (`SERVICE_MODE=status`) | Forwards all events to browser WebSocket clients |
| `upload-service` (`SERVICE_MODE=upload`) | Updates DB, feeds `UploadStatusWebSocket` |
| `processing-service` | Listens for `UploadMetaEvent` (learns total segment count) and `UploadFailedEvent` (adds video to `FailedVideoRegistry` to skip work) |

---

### 3. DevLog Bus — Operational Logs
**Purpose:** Human-readable service lifecycle messages surfaced in the dev-logs UI page.

| Property | Value |
|---|---|
| Queue | `dev.logs.queue` (durable) |
| Routing key | `dev.log` (exact) |
| Publisher | Any service via `RabbitMQDevLogPublisher` |
| Consumer | `status-service` via `RabbitMQDevLogReader` |

**Message: `DevLogMessage`** (type=`dev_log`)
```json
{
  "type": "dev_log",
  "level": "INFO|WARN|ERROR",
  "service": "upload-service",
  "message": "Upload service started",
  "formattedMessage": "[upload-service]: Upload service started",
  "emittedAt": "2024-01-01T00:00:00Z"
}
```

---

## All Message Types (Canonical Reference)

### StatusEventBus messages (all extend `JobEvent`)

| Type string | Java class | Publisher | Payload highlights |
|---|---|---|---|
| `progress` | `UploadProgressEvent` | upload-service | `completedSegments` |
| `meta` | `UploadMetaEvent` | upload-service | `totalSegments` |
| `storage_status` | `UploadStorageStatusEvent` | upload-service, processing-service | `state`, `reason` |
| `transcode_progress` | `TranscodeProgressEvent` | processing-service | `profile`, `segmentNumber`, `state` (DONE/FAILED), `doneSegments`, `totalSegments` |
| `failed` | `UploadFailedEvent` | node-watcher | `reason`, `machineId`, `containerId` — also published to `upload.failure` routing key |

### TranscodeTaskBus message

| Type string | Java class | From → To | Payload highlights |
|---|---|---|---|
| `transcode_task` | `TranscodeTaskEvent` | upload-service → processing-service | `chunkKey`, `profile`, `segmentNumber`, `outputTsOffsetSeconds` |

---

## RabbitMQ Config (env vars)

| Env var | Default | Purpose |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Broker host |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_USER` | `guest` | Username |
| `RABBITMQ_PASS` | `guest` | Password |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `RABBITMQ_EXCHANGE` | `upload.events` | Topic exchange name |
| `RABBITMQ_TASK_QUEUE` | `processing.tasks.queue` | Work queue name |
| `RABBITMQ_TASK_BINDING` | `upload.task.transcode` | Task routing key |
| `RABBITMQ_STATUS_QUEUE` | `upload.status.queue` | Status queue name (only used in shared-queue modes) |
| `RABBITMQ_STATUS_BINDING` | `upload.status.*` | Status wildcard binding |
| `RABBITMQ_FAILURE_BINDING` | `upload.failure` | Failure-event routing key |
| `RABBITMQ_DEV_LOG_QUEUE` | `dev.logs.queue` | Dev log queue name |
| `RABBITMQ_DEV_LOG_BINDING` | `dev.log` | Dev log routing key |
| `RABBITMQ_TASK_PREFETCH` | `CPUs * 3/4` | Max in-flight tasks per processing container |

---

## Data Flow Summary

```
Client video upload
    ↓
upload-service (port 8080)
  • segments video into 6s .ts chunks → MinIO uploads/{id}/chunks/
  • publishes UploadProgressEvent + UploadMetaEvent → StatusEventBus
  • publishes TranscodeTaskEvent (per chunk × 3 profiles) → TranscodeTaskBus

TranscodeTaskBus (quorum queue, manual ack, prefetch-controlled)
    ↓ exactly one processing container per message
processing-service (port 8082)
  • downloads chunk from MinIO
  • runs FFmpeg → local spool dir
  • uploads to MinIO uploads/{id}/processed/{profile}/
  • generates HLS manifests (playlist.m3u8 + master.m3u8)
  • publishes TranscodeProgressEvent → StatusEventBus
  • acks message

StatusEventBus (fan-out, autoAck)
    ↓ to all subscribers
status-service (port 8081) → WebSocket → browser UI
upload-service            → DB updates
processing-service        → FailedVideoRegistry, segment count tracking

node-watcher (Python, Docker socket)
  • detects container die/stop events
  • marks video FAILED in Postgres
  • publishes UploadFailedEvent → StatusEventBus → all services stop work on that video

streaming-service (port 8083)
  • does NOT use RabbitMQ
  • serves HLS manifests and presigned MinIO URLs directly to browsers
```

---

## Key Source Files

| File | Purpose |
|---|---|
| `shared/.../RabbitMQTranscodeTaskBus.java` | Work queue impl (manual ack, prefetch, nack+requeue on failure) |
| `shared/.../RabbitMQStatusEventBus.java` | Fan-out status bus (autoAck, replica-local queues) |
| `shared/.../RabbitMQBusConfig.java` | All RabbitMQ config loaded from env/.env |
| `shared/.../RabbitMQDevLogPublisher.java` | Dev-log structured message publisher |
| `shared/.../events/` | All message POJOs |
| `upload-service/.../SegmentUploadCoordinator.java` | Publishes `TranscodeTaskEvent` after chunk upload |
| `processing-service/.../ProcessingRuntime.java` | Consumes tasks, publishes progress events |
| `processing-service/.../ProcessingServiceApplication.java` | Wires all processing-service dependencies |
| `upload-service/.../UploadStatusWebSocket.java` | Bridges status events to browser WebSocket |
| `node-watcher/watcher.py` | Python crash detector, publishes `UploadFailedEvent` via pika |

---

## Processing Service Internals

- `ProcessingRuntime` — central orchestrator, holds mutable state, dispatches tasks.
- `TranscodingProfile` — `LOW` (480p/800kbps), `MEDIUM` (720p/2.5Mbps), `HIGH` (1080p/5Mbps). Every chunk gets all 3 profiles.
- Local spool pattern: FFmpeg writes to `PROCESSING_SPOOL_ROOT` first, then `LocalSpoolUploadWorkerPool` uploads to MinIO.
- `StartupRecoveryService` — on restart: resets stuck `UPLOADING→PENDING` tasks, scans spool for orphans, re-queues incomplete segments.
- `FailedVideoRegistry` — in-memory set; populated via `UploadFailedEvent`; all services skip work for listed video IDs.

---

## Database Tables

| Table | Purpose |
|---|---|
| `video_upload` | Per-video status (`PROCESSING`, `READY`, `FAILED`), container tracking |
| `segment_upload` | Source segment upload state |
| `transcoded_segment_status` | Per `(video, profile, segment)` transcode tracking |
| `processing_upload_task` | Spool→MinIO upload queue (`PENDING`, `UPLOADING`, `DONE`) |
| `processing_task_claim` | Which processing instance owns a task (used by node-watcher + autoscaler) |
| `autoscaler_event` | Autoscaler scale events (cooldown survives leader failover) |

Schema: `upload-service/docs/db/schema.sql`

---

## Build & Run Cheatsheet

```bash
# Build all service JARs
mvn -pl upload-service,processing-service,streaming-service -am -DskipTests install

# Run everything via Docker Compose
docker compose up -d --build

# Run tests for one module
mvn -pl processing-service test

# Run a service locally (needs RabbitMQ + MinIO + Postgres)
mvn -pl upload-service -DskipTests exec:java -Dexec.mainClass=com.distributed26.videostreaming.upload.upload.UploadServiceApplication
```

`SERVICE_MODE` env var controls `UploadServiceApplication` behaviour: `upload` (port 8080) or `status` (port 8081).

