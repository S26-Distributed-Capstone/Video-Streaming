# Failure Handling

This project has two main failure-handling paths:

- `node-watcher` detects upload or processing containers that die while a video is still in progress.
- `processing-service` performs startup recovery for videos that were left incomplete after a restart.

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

1. Upload Service dies before returning videoId to client - HTTP 501 error (Service Unavailable)
2. Upload Service fails / dies while it's segmenting - retry the upload with same videoId, skipping previously uploaded segments. Try to spin up with same instance for ~10 seconds
3. Upload Service dies after upload but during processing - don't tell client anything

### Status Service

1. Status Service dies mid-upload - show client statuses is unavailable. Retry. 
  - Recovery: New Status Service checks Postgres for current status of the videoId. Then start listening on RabbitMQ and write to Postgres as normal
  - Only consumes from RabbitMQ once confirmed written to Postgres - therefore if Status Service fails mid-write, we don't lose updates 

### Processing Service

1. Processing Service fails / dies mid-transcoding a segment - needs to be downloaded again from S3 and retranscoded. (Never acked from RabbitMQ)
2. Processing Service fails / dies mid-uploading a segment - checks local DB for previously transcoded segments and uploads them with the local queue. Tried to sping up for ~10 seconds. If it can't spin it up, have another processing service instance pick it up using RabbitMQ ack timeout

### Streaming Service

1. Streaming Service doesn't respond - tell client it's unavailable

