# Video Upload/Processing/Playback System
> Distributed Systems Spring 2026 Capstone Project

## Runtime Flow

- `upload-service` accepts the source video, segments it locally with FFmpeg, uploads source `.ts` chunks to object storage, and publishes:
  - one status event per source chunk for the UI
  - three transcode task messages per source chunk (`low`, `medium`, `high`) for processing workers
- `processing-service` consumes one transcode task per RabbitMQ message, downloads that source chunk from object storage, renders one bitrate profile into a local spool, hands the result to a same-node uploader, and publishes progress updates
- `status-service` is the browser-facing progress fanout layer:
  - sends an initial DB-backed progress snapshot when the socket connects
  - forwards live RabbitMQ status events over WebSocket
- `streaming-service` serves completed manifests and segments from object storage once Postgres reports the video as ready

## Processing Notes

- `DONE` still means the processed segment is present in object storage.
- `TRANSCODED` means the worker finished FFmpeg and persisted a same-node upload task in Postgres.
- The processing node now uses a local spool directory, configured with `PROCESSING_SPOOL_ROOT` and defaulting to `processing-spool/`.
- `processing-service` now requires both Postgres and the local spool directory at startup; there is no inline-upload fallback path.

## Client Contract

- `POST /upload` returns:
  - `videoId`
  - `uploadStatusUrl`
- the browser opens `uploadStatusUrl` on `status-service`
- the socket currently emits:
  - `task` for source chunk upload progress
  - `meta` for total segment count
  - `transcode_progress` for per-profile transcoding progress
  - `failed` for terminal failures
- the browser does not connect to `processing-service` directly for progress
