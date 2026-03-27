# Workflow

This document lists the workflow diagrams for each supported use case in the system.

For the supported scenarios and failure expectations, see [docs/scope.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/scope.md).
For system structure, service boundaries, and redundancy, see [docs/architecture.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/architecture.md).

## Supported Use Cases And Workflow Diagrams

### 1. Upload A Video

This workflow covers:

- user submits a file and display name
- upload-service segments the source video
- source chunks are persisted
- a `videoId` is returned

Workflow diagram:

- [docs/diagrams/upload-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/upload-service.drawio)
- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)

### 2. Observe Upload And Processing Progress

This workflow covers:

- client connects to the status path
- status-service sends a DB-backed snapshot
- live upload and transcode progress events are streamed to the client

Workflow diagram:

- [docs/diagrams/status-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/status-service.drawio)
- [docs/diagrams/bpmn-status-reconnect-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-status-reconnect-workflow.bpmn)

### 3. Process And Transcode Uploaded Video

This workflow covers:

- processing-service consumes transcode tasks
- segments are transcoded into output profiles
- upload handoff state is persisted
- processed outputs are uploaded to object storage

#### MinIO Failure Resilience

When MinIO becomes unreachable during processing, the service degrades gracefully instead of stopping:

- **Transcoding continues** — source chunks already downloaded are transcoded and spooled locally. The `fileExists` idempotency check falls through safely (`safeFileExists` returns `false` on error) so workers are never blocked waiting for MinIO.
- **Downloads use bounded retry** — source chunk downloads are a pass-through in `ResilientStorageClient`; `TranscodingTask.downloadChunkWithRetry()` owns retry logic with its own bounded exponential backoff (default 5 attempts). Failed downloads surface a concise root-cause message (e.g. "Connection refused") rather than full stack traces.
- **Uploads are not retried at the storage layer** — `uploadFile` is a pass-through in `ResilientStorageClient` because the `InputStream` is consumed on the first attempt; retrying would upload corrupt data. Instead, `LocalSpoolUploadWorkerPool` resets failed tasks to `PENDING` and opens a fresh stream on the next poll. The spool directory is Docker-volume-backed, so transcoded files survive restarts and are uploaded once MinIO recovers.
- **Stateless S3 operations** (`deleteFile`, `listFiles`, `generatePresignedUrl`) are wrapped by `ResilientStorageClient` with retry + exponential backoff (500 ms → 30 s cap, configurable via `STORAGE_RETRY_*` env vars). Non-transient errors (4xx S3 status codes, except 408/429) are thrown immediately — retrying won't fix a missing key or access denied.
- **Startup bucket check** — `ensureBucketExists()` is best-effort (single attempt, no retry). If it fails, a background daemon thread retries with exponential backoff until the bucket is confirmed. Worker pools, recovery, and the health endpoint start normally regardless.
- **Logging** — `S3StorageClient` logs terminal failures at `WARN` with `ex.toString()` (one line), plus full stack traces at `DEBUG`. Callers (`ResilientStorageClient`, `safeFileExists`, `downloadChunkWithRetry`) add their own concise `WARN`-level context. No stack traces appear at default log levels.

For full details on failure handling see [docs/challenges.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/challenges.md#minio--object-storage-outage).

Workflow diagram:

- [docs/diagrams/processing-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/processing-service.drawio)
- [docs/diagrams/bpmn-processing-failure-recovery.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-processing-failure-recovery.bpmn)

### 4. Play A Ready Video

This workflow covers:

- user requests ready videos
- streaming-service validates readiness
- manifests are returned
- the browser fetches segments through presigned object-storage URLs

Presigned URL flow:

- streaming-service reads the variant `playlist.m3u8` from MinIO via the internal endpoint (`S3Client`)
- for each `.ts` segment entry, it calls `generatePresignedUrl()` on the `S3Presigner`, which uses the public endpoint (`MINIO_PUBLIC_ENDPOINT`)
- the rewritten playlist (with presigned URLs) is returned to the browser
- the browser fetches segment bytes directly from MinIO — media data does not pass through streaming-service

Workflow diagram:

- [docs/diagrams/streaming-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/streaming-service.drawio)
- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)
