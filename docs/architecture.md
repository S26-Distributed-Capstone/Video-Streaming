# Architecture

This document defines the system architecture using C4-equivalent diagrams. It focuses on system boundaries, major containers, deployment structure, and where redundancy is used.

For supported scenarios and delivery scope, see [docs/scope.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/scope.md).
For failure modes, coordination risks, and recovery behavior, see [docs/challenges.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/challenges.md).

## Architecture Summary

The system is a distributed video upload, processing, and playback platform composed of independently deployable services.

Core responsibilities:
- upload-service accepts uploads, segments source video, stores chunks, and emits work/status events
- status-service provides real-time progress updates to the browser
- processing-service consumes distributed transcoding work and uploads processed outputs
- streaming-service serves HLS manifests and playback metadata for ready videos
- node-watcher detects container failures and converts unrecovered in-progress work into terminal failure events

Shared infrastructure:
- Postgres stores workflow state, progress state, and processing coordination data
- RabbitMQ carries status and transcoding events between services
- MinIO stores source chunks, manifests, and transcoded media artifacts

## C4 Diagrams

The architecture is captured in these diagram sources:

- [docs/diagrams/architecture.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/architecture.drawio)
- [docs/diagrams/processing-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/processing-service.drawio)
- [docs/diagrams/upload-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/upload-service.drawio)
- [docs/diagrams/streaming-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/streaming-service.drawio)
- [docs/diagrams/status-service.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/status-service.drawio)

## BPMN Workflow Diagrams

The main workflow and failure-handling behavior is also captured in BPMN diagrams:

- [docs/diagrams/bpmn-end-to-end-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-end-to-end-workflow.bpmn)
- [docs/diagrams/bpmn-processing-failure-recovery.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-processing-failure-recovery.bpmn)
- [docs/diagrams/bpmn-status-reconnect-workflow.bpmn](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/bpmn-status-reconnect-workflow.bpmn)

### Context Notes

- The user interacts only through HTTP and WebSocket endpoints.
- The system depends on three shared infrastructure services: Postgres, RabbitMQ, and MinIO.
- Stateful coordination is intentionally centralized in shared infrastructure rather than embedded in service memory.

### Container Notes

- upload-service and processing-service are decoupled by RabbitMQ, which lets processing scale independently of upload traffic.
- status-service is intentionally separate so progress delivery can scale independently of upload and processing.
- streaming-service is read-oriented and depends on Postgres only for readiness checks and metadata lookup.
- node-watcher exists to bridge the gap between container failure events and user-visible workflow state.

## Processing Coordination View

The most distribution-sensitive part of the system is processing-service, because it coordinates many independent workers while avoiding duplicate processing.

### Coordination Notes

- Postgres is the source of truth for upload status, video readiness, transcode progress, and local upload handoff.
- RabbitMQ distributes work, but Postgres prevents replicas from stepping on each other during claims and recovery.
- Local spool storage allows a processing replica to survive restart boundaries without losing already-transcoded segment files.

## Deployment View

The deployment model supports multiple replicas of stateless services while reusing shared infrastructure. The deployment relationships are shown in [docs/diagrams/architecture.drawio](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/diagrams/architecture.drawio).

## Redundancy

Redundancy is used in the following places:

- upload-service can run with multiple replicas so upload traffic is not tied to a single instance
- status-service can run with multiple replicas, each with replica-local RabbitMQ consumption, so WebSocket clients can reconnect to another healthy instance
- processing-service can run with multiple replicas so transcode work is distributed across nodes
- streaming-service can run with multiple replicas so manifest and playback metadata requests remain available if one instance fails
- node-watcher can run globally so each node can observe failures for the containers running on that node

Redundancy is not primarily provided by in-memory replication between services. Instead, it is achieved by:

- keeping service instances mostly stateless
- storing shared workflow state in Postgres
- storing durable media artifacts in MinIO
- using RabbitMQ to decouple producers from consumers

## Where Redundancy Matters Most

### Upload Path

- multiple upload-service replicas improve availability for new requests
- upload progress state is persisted so a failed instance does not erase workflow state

### Status Path

- multiple status-service replicas reduce the chance that a browser loses progress visibility permanently
- a reconnecting client can rebuild its view from Postgres before resuming live updates from RabbitMQ

### Processing Path

- multiple processing-service replicas are the main horizontal-scaling mechanism
- task distribution is shared through RabbitMQ
- duplicate or orphaned work is bounded through Postgres-backed claim and recovery logic

### Streaming Path

- multiple streaming-service replicas provide read availability for manifests
- direct browser fetches to MinIO reduce load on streaming-service replicas

## Distributed Challenges

This architecture is shaped by several distributed-systems challenges:

- coordinating multiple processing replicas without duplicating work
- recovering cleanly when a processing node dies after transcoding but before upload completion
- keeping browser-visible progress consistent across reconnects and service restarts
- ensuring playback only begins after all required outputs are complete
- handling partial failure where the app cluster is healthy but one specific service replica is not

The main mitigations are:

- shared durable state in Postgres
- asynchronous task/event delivery through RabbitMQ
- durable artifact storage in MinIO
- startup recovery and orphan cleanup in processing-service
- explicit failure detection in node-watcher

See [docs/scope.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/scope.md) for the supported scenarios these concerns apply to.
See [docs/challenges.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/challenges.md) for the concrete recovery behaviors currently implemented.

## Architectural Boundaries

The system intentionally does not attempt to:

- build a distributed database of its own
- replicate object storage internally
- provide live streaming workflows
- provide authentication, authorization, or multi-tenant isolation

Those concerns are outside the current system boundary and should be addressed by external infrastructure or future work if needed.
