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

## Processing Restart Recovery

When `processing-service` (re)-starts, it checks for videos that are still recoverable:

- videos still marked `PROCESSING`
- videos with open local upload tasks in `processing_upload_task`

For each recoverable video, it:

1. Lists source chunks from object storage.
2. Checks which `(segment, profile)` outputs are already `DONE`.
3. Checks which segments already have open local upload tasks.
4. Re-publishes only the missing transcode tasks.

If all profiles are already complete, it can also trigger manifest generation.

## Terminal Failure Signal

The browser sees terminal failures through the WebSocket `failed` event:

```json
{
  "jobId": "<uuid>",
  "reason": "upload_container_died",
  "machineId": "node-a",
  "containerId": "abc123",
  "type": "failed"
}
```

The exact `reason` depends on which watched service died.
