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

### 2. Observe Upload And Processing Progress

This workflow covers:

- client connects to the status path
- status-service sends a DB-backed snapshot
- live upload and transcode progress events are streamed to the client

Workflow diagram:

- [docs/diagrams/status-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/status-service.drawio)

### 3. Process And Transcode Uploaded Video

This workflow covers:

- processing-service consumes transcode tasks
- segments are transcoded into output profiles
- upload handoff state is persisted
- processed outputs are uploaded to object storage

Workflow diagram:

- [docs/diagrams/processing-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/processing-service.drawio)

### 4. Play A Ready Video

This workflow covers:

- user requests ready videos
- streaming-service validates readiness
- manifests are returned
- the browser fetches segments through presigned object-storage URLs

Workflow diagram:

- [docs/diagrams/streaming-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/streaming-service.drawio)
