# User API Guide

This document describes the system from a user's perspective: which APIs are available and which configuration settings must be supplied before use.

Supported use cases and delivery scope are defined in [docs/scope.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/scope.md).

## Default Service Addresses

In the default local setup, the services are exposed at:

- Upload service: `http://localhost:8080`
- Status service: `http://localhost:8081`
- Processing service: `http://localhost:8082`
- Streaming service: `http://localhost:8083`
- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- RabbitMQ management UI: `http://localhost:15672`

## Required User Configuration

Before starting the system, copy the example environment file:

```bash
cp .env.example .env
```

The following settings are expected to be configured by the user.

### Object Storage

- `MINIO_ENDPOINT`: internal endpoint used by services to reach object storage
- `MINIO_PUBLIC_ENDPOINT`: browser-reachable endpoint used in presigned playback URLs
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_BUCKET_NAME`: bucket used to store uploaded and processed media
- `MINIO_REGION`: object storage region string

### PostgreSQL

- `POSTGRES_USER`: database bootstrap username for the Postgres container
- `POSTGRES_PASSWORD`: database bootstrap password for the Postgres container
- `POSTGRES_DB`: database name for the Postgres container
- `PG_URL`: JDBC connection string used by the application
- `PG_USER`: application database username
- `PG_PASSWORD`: application database password
- `PG_DB`: application database name

### RabbitMQ

- `RABBITMQ_HOST`: RabbitMQ hostname
- `RABBITMQ_PORT`: RabbitMQ port, usually `5672`
- `RABBITMQ_USER`: RabbitMQ username
- `RABBITMQ_PASS`: RabbitMQ password
- `RABBITMQ_VHOST`: RabbitMQ virtual host
- `RABBITMQ_EXCHANGE`: topic exchange used for upload and status events

### Queue Bindings

- `RABBITMQ_STATUS_QUEUE`: queue used for upload/status event consumption
- `RABBITMQ_STATUS_BINDING`: routing key pattern for status events
- `RABBITMQ_STATUS_QUEUE_PROCESSING`: status queue used by processing-service
- `RABBITMQ_FAILURE_BINDING`: routing key used for terminal failure events
- `RABBITMQ_TASK_QUEUE`: queue used for distributed transcode work items
- `RABBITMQ_TASK_BINDING`: routing key used for transcode tasks

### Service Behavior

- `STATUS_PORT`: status service port used to build WebSocket URLs
- `PROCESSING_PORT`: processing service HTTP port
- `STREAMING_PORT`: streaming service HTTP port
- `WORKER_POOL_SIZE`: number of processing workers
- `THREADS_PER_WORKER`: FFmpeg thread count per worker
- `FFMPEG_PRESET`: FFmpeg encoding preset
- `CHUNK_DURATION_SECONDS`: upload chunk duration
- `MACHINE_ID`: identifier recorded in DB and failure events

### Failure Detection

- `WATCH_CONTAINER_NAME_PREFIX`: container/service names watched by `node-watcher`
- `FAILURE_REASON`: failure reason emitted on terminal failure
- `UPDATE_DB_STATUS`: whether `node-watcher` updates DB status on failure
- `PYTHONUNBUFFERED`: enables unbuffered watcher logs
- `DEBUG_EVENTS`: enables debug event logging in the watcher
- `DEDUP_WINDOW_SECONDS`: duplicate-event suppression window

## Typical User Flow

1. Install and start the system as described in [docs/installation.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/installation.md).
2. Open the upload UI at `http://localhost:8080`.
3. Upload a video file and provide a display name.
4. Watch progress updates through the status WebSocket.
5. Once processing finishes, open the ready-video list and start playback.

## API Reference

## 1. Upload API

### `POST /upload`

Uploads a video and starts segmentation plus downstream processing.

Request:
- Method: `POST`
- Content type: `multipart/form-data`
- Form fields:
  - `file`: video file
  - `name`: user-facing video title

Success response:
- Status: `202 Accepted`
- Body:

```json
{
  "videoId": "123e4567-e89b-12d3-a456-426614174000",
  "uploadStatusUrl": "ws://localhost:8081/upload-status?jobId=123e4567-e89b-12d3-a456-426614174000"
}
```

Possible errors:
- `400 Bad Request`: missing file or name
- `500 Internal Server Error`: upload, storage, or initialization failure

Example:

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@/path/to/video.mp4" \
  -F "name=Demo Video"
```

## 2. Status API

### `GET /upload-status?jobId={videoId}`

Opens a WebSocket that streams upload and transcoding progress for a single video.

Behavior:
- the connection begins with a DB-backed snapshot
- live status events are then forwarded from RabbitMQ

Event types:
- `progress`: snapshot of uploaded source segments
- `task`: live upload chunk progress
- `meta`: total segment count for the upload
- `transcode_progress`: per-profile transcode state changes
- `failed`: terminal failure notification

Representative `transcode_progress` event:

```json
{
  "jobId": "123e4567-e89b-12d3-a456-426614174000",
  "profile": "high",
  "segmentNumber": 7,
  "state": "UPLOADING",
  "doneSegments": 5,
  "totalSegments": 12,
  "type": "transcode_progress"
}
```

### `GET /upload-info/{videoId}`

Returns a DB-backed progress snapshot for the upload/status UI.

Success response:
- Status: `200 OK`

```json
{
  "videoId": "123e4567-e89b-12d3-a456-426614174000",
  "videoName": "Demo Video",
  "status": "PROCESSING",
  "totalSegments": 12,
  "machineId": "node-a",
  "containerId": "abc123",
  "uploadedSegments": 7,
  "transcode": {
    "lowDone": 5,
    "mediumDone": 4,
    "highDone": 4
  }
}
```

Possible errors:
- `404 Not Found`: unknown video ID
- `500 Internal Server Error`: upload info store unavailable

## 3. Streaming API

### `GET /stream/ready`

Returns videos that are ready for playback.

Success response:
- Status: `200 OK`

```json
[
  { "videoId": "uuid-1", "videoName": "Intro to Streaming" },
  { "videoId": "uuid-2", "videoName": "Advanced Encoding" }
]
```

### `GET /stream/{videoId}/manifest`

Returns the master HLS manifest for a video that is in `READY` state.

Success response:
- Status: `200 OK`
- Content-Type: `application/vnd.apple.mpegurl`

Possible errors:
- `404 Not Found`: unknown video
- `409 Conflict`: video exists but is not ready

### `GET /stream/{videoId}/variant/{profile}/playlist.m3u8`

Returns the HLS variant playlist for one bitrate profile.

Behavior:
- the streaming service rewrites segment URLs into presigned MinIO URLs
- segment bytes are fetched directly by the browser from object storage

Possible errors:
- `400 Bad Request`: invalid profile
- `404 Not Found`: unknown video or missing variant playlist

## 4. Processing API

These endpoints are primarily operational, but they are still externally exposed.

### `GET /health`

Returns a simple liveness response.

```json
{ "status": "ok" }
```

### `GET /workers`

Returns a snapshot of the processing worker pool.

```json
{
  "queued": 12,
  "workers": [
    { "id": "worker-0", "status": "BUSY" },
    { "id": "worker-1", "status": "IDLE" }
  ]
}
```

Worker status values:
- `IDLE`
- `BUSY`
- `OFFLINE`
