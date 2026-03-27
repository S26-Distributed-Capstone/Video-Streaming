# Failure Handling

This project has two main failure-handling paths:

- `node-watcher` detects upload or processing containers that die while a video is still in progress.
- `processing-service` performs startup recovery for videos that were left incomplete after a restart.

Related BPMN workflow diagrams:

- [docs/diagrams/bpmn-processing-failure-recovery.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-processing-failure-recovery.bpmn)
- [docs/diagrams/bpmn-status-reconnect-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-status-reconnect-workflow.bpmn)

## Node Watcher

`node-watcher` listens for Docker container events for the upload and processing services.

If a watched container dies:

1. It finds videos still associated with that container.
2. It waits briefly to see whether the service has recovered.
3. If recovery does not happen, it:
   - marks the video `FAILED` in Postgres
   - publishes a `failed` status event to RabbitMQ

That `failed` event is then forwarded to the browser by `status-service`.

This prevents videos from being stuck forever in `PROCESSING` after a container crash.

## Failure Situations

### Upload Service

1. Upload Service dies before returning videoId to the client. If no healthy instance is available, the request fails with a service-unavailable error from the routing layer; otherwise, the request is routed to another healthy upload-service instance.
2. Upload Service dies while segmenting the video - client immediately retries the upload with the same videoId, skipping previously uploaded segments. If retries exceed the client threshold, the upload is treated as failed.

### Status Service

1. Status-service dies mid-upload - client reconnects or retries against a healthy status-service instance
  - Recovery: New status-service instance restores the latest known progress for that videoId from Postgres, sends the latest snapshot to the client, then resumes consuming live status events from RabbitMQ
2. All status-service instances are unavailable during uploading / processing
  - Recovery: client shows "status unavailable" and retries a few times before giving up

### Processing Service

1. Processing Service fails / dies mid-transcoding a segment - needs to be downloaded again from S3 and retranscoded. (Never acked from RabbitMQ)
2. Processing Service fails / dies mid-uploading a segment - checks local DB for previously transcoded segments and uploads them with the local queue. Tried to sping up for ~10 seconds. If it can't spin it up, have another processing service instance pick it up using RabbitMQ ack timeout

#### Mid-Upload Recovery Details

When the processing service restarts after a mid-upload crash, the following recovery sequence runs:

1. **Reset UPLOADING → PENDING**: Any `processing_upload_task` rows left in `UPLOADING` state are reset to `PENDING` so the upload workers will retry them.
2. **Spool orphan scan** (`recoverOrphanedSpoolFiles`): The local spool directory (`/app/processing-spool`) is scanned for `.ts` files that were transcoded but never registered as upload tasks (crash between `transcodeToSpool()` and `upsertPending()`). For each orphaned file:
   - If already in object storage → clean up spool file and mark DONE
   - If an upload task already exists → skip
   - Otherwise → create a PENDING upload task so the upload workers pick it up
3. **Incomplete video recovery** (`recoverIncompleteVideos`): Videos still in `PROCESSING` status are inspected. Any segments that are neither `DONE` nor have an open upload task are re-queued via RabbitMQ for re-transcoding.
4. **Upload workers start** and begin polling the `processing_upload_task` table for PENDING tasks, uploading them to object storage.

The spool directory is backed by a Docker volume (`processing_spool`) so transcoded files survive container restarts.

### Streaming Service

1. A manifest request hits a streaming-service instance that dies or drops the connection mid-request
  - Recovery: client retries the manifest request, and Swarm should route it to another healthy replica
2. All streaming-service instances are unavailable when the client tries to fetch the manifest
  - Recovery: client shows "stream unavailable" and retries a few times before giving up

### MinIO / Object Storage Outage

When MinIO becomes unreachable, the processing service continues transcoding any source chunks it already has and spools results locally for upload once MinIO recovers.

1. **`ResilientStorageClient` wrapper** — Stateless S3 operations (`deleteFile`, `listFiles`, `ensureBucketExists`, `generatePresignedUrl`) go through `ResilientStorageClient` (`shared` module), which wraps each call with retry + exponential backoff (500 ms initial → doubles each attempt → caps at 30 s). By default retries are unlimited; the loop only stops when the operation succeeds or the container shuts down (thread interrupted). Tunable via env vars:
   - `STORAGE_RETRY_INITIAL_DELAY_MS` (default 500)
   - `STORAGE_RETRY_MAX_DELAY_MS` (default 30 000)
   - `STORAGE_RETRY_MAX_ATTEMPTS` (default 0 = unlimited)

   **Pass-through (no retry):** `fileExists`, `downloadFile`, and `uploadFile` are delegated directly:
   - `fileExists` — optimistic pre-check handled by `safeFileExists`; retrying would block the worker for no benefit
   - `downloadFile` — `TranscodingTask.downloadChunkWithRetry()` has its own bounded retry; a second retry layer would make that logic dead code
   - `uploadFile` — the `InputStream` argument is consumed on the first attempt; if that attempt partially reads the stream before failing, retrying with the same (now exhausted) stream would upload corrupt/truncated data. Every caller already handles failure by reopening the stream from its source (`LocalSpoolUploadWorkerPool` resets to PENDING and opens a fresh `FileInputStream` on the next poll; `TranscodingTask` re-queues via RabbitMQ; `AbrManifestService` regenerates the manifest)

   **Non-transient errors are never retried.** If the cause chain contains an `S3Exception` with a 4xx status code (other than 408 Request Timeout or 429 Too Many Requests), the error is thrown immediately — retrying will not fix a missing key, missing bucket, or access-denied error.

2. **`fileExists` checks degrade gracefully** — Before transcoding, the service checks if the output already exists in MinIO (idempotency optimization). If MinIO is unreachable, the check returns `false` and transcoding proceeds anyway since all MinIO writes are idempotent. This `safeFileExists` pattern is applied in:
   - `TranscodingTask.execute()` and `transcodeToSpool()`
   - `LocalSpoolUploadWorkerPool.uploadSpoolTask()`
   - `StartupRecoveryService.recoverOrphanedSpoolFiles()`

3. **Source chunk downloads** — `downloadFile` is a pass-through in `ResilientStorageClient` — the caller (`TranscodingTask.downloadChunkWithRetry()`) owns retry logic with its own bounded exponential backoff (configurable via `DOWNLOAD_MAX_ATTEMPTS`, default 5). Failed downloads produce readable error messages that surface the root cause (e.g. "Connection refused") rather than generic wrapper exceptions.

4. **Local spool upload workers** — When an upload to MinIO fails, the upload task is reset to `PENDING` and retried on the next poll cycle. The spool directory is backed by a Docker volume (`processing_spool`), so transcoded files survive container restarts and will be uploaded once MinIO returns.

5. **Startup bucket check** — `ensureBucketExists()` is called on the raw `S3StorageClient` (single attempt, no retry wrapper) at startup. If MinIO is unreachable, the failure is logged at `WARN` and startup continues — worker pools, recovery logic, and the health endpoint all start normally. The bucket almost always already exists from a prior run; if not, subsequent operations through `ResilientStorageClient` will retry until MinIO is back.

6. **Concise logging** — `S3StorageClient` logs all errors at `DEBUG` level only, so the verbose AWS SDK stack traces do not appear at default log levels. User-facing messages are owned by the callers:
   - `ResilientStorageClient` logs each retry attempt at `WARN` with `e.toString()` (one line)
   - `safeFileExists` logs the graceful fall-through at `WARN` with a short explanation
   - `downloadChunkWithRetry` unwraps exception chains to surface the root cause (e.g. "Connection refused") without a full stack trace

### Presigned URL Resolution In Docker

Presigned URLs must be reachable by the browser, not just by services inside the Docker network. The `S3StorageClient` solves this by using two separate endpoints:

- The `S3Client` (server-side operations) connects to `MINIO_ENDPOINT` (e.g. `http://minio:9000`), which resolves inside the Docker network.
- The `S3Presigner` (presigned URL generation) uses `MINIO_PUBLIC_ENDPOINT` (e.g. `http://localhost:9000`), which resolves from the host machine.

If `MINIO_PUBLIC_ENDPOINT` is misconfigured or points to the internal hostname, presigned URLs will contain `minio:9000` in the host portion, causing browser requests to fail with DNS resolution errors. This is the most common cause of "upload works but playback does not" in local development. Both endpoints must use path-style access (`pathStyleAccessEnabled(true)`) because MinIO does not support virtual-hosted-style bucket addressing.

For the broader user workflow that reaches playback after successful upload and processing, see:

- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)
