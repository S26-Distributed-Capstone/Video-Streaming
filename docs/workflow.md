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
- if MinIO is unavailable, upload-service waits and retries storage instead of immediately failing the upload

Workflow diagram:

- [docs/diagrams/upload-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/upload-service.drawio)
- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)

### 2. Observe Upload And Processing Progress

This workflow covers:

- client connects to the status path
- status-service sends a DB-backed snapshot
- live upload and transcode progress events are streamed to the client
- when MinIO is unavailable, the client can observe a non-terminal storage-waiting state rather than a terminal failure

Workflow diagram:

- [docs/diagrams/status-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/status-service.drawio)
- [docs/diagrams/bpmn-status-reconnect-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-status-reconnect-workflow.bpmn)

### 3. Process And Transcode Uploaded Video

This workflow covers:

- processing-service consumes transcode tasks
- segments are transcoded into output profiles
- upload handoff state is persisted
- processed outputs are uploaded to object storage
- once source chunks are persisted, downstream transcoding is treated as server-side work rather than a new client upload responsibility

Workflow diagram:

- [docs/diagrams/processing-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/processing-service.drawio)
- [docs/diagrams/bpmn-processing-failure-recovery.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-processing-failure-recovery.bpmn)

### 4. Play A Ready Video

This workflow covers:

- user requests ready videos
- streaming-service validates readiness
- manifests are returned
- the browser fetches segments through presigned object-storage URLs

Workflow diagram:

- [docs/diagrams/streaming-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/streaming-service.drawio)
- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)
