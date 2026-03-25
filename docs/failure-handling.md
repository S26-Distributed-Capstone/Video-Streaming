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

### Streaming Service

1. A manifest request hits a streaming-service instance that dies or drops the connection mid-request
  - Recovery: client retries the manifest request, and Swarm should route it to another healthy replica
2. All streaming-service instances are unavailable when the client tries to fetch the manifest
  - Recovery: client shows "stream unavailable" and retries a few times before giving up
