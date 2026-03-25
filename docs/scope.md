# Scope

This document defines the scope of the project, the use cases the system supports, and the failure and recovery behavior expected within that scope.

For API details, see `docs/api.md` and `docs/api.md`.
For installation and deployment steps, see `docs/installation.md`.
For architecture and redundancy, see `docs/architecture.md`.
For detailed failure and recovery behavior, see `docs/challenges.md`.

## Deliverables

This project delivers a distributed video upload, processing, and playback system with the following functional areas:

- a browser-accessible upload interface
- an HTTP upload API
- a WebSocket-based status path for real-time progress visibility
- a distributed processing pipeline for adaptive bitrate transcoding
- a playback path for videos that have completed processing
- failure detection and recovery support for upload and processing interruptions

## In-Scope Use Cases

The system is expected to support these scenarios.

### 1. Upload A Video

Supported behavior:

- a user submits a video and display name
- the system segments the source video during upload
- the source chunks are persisted to object storage
- a `videoId` is returned after durable upload initialization completes

Expected result:

- the upload is accepted and tracked as a known video in the system

### 2. Observe Upload Progress

Supported behavior:

- a user connects to the status endpoint for a specific `videoId`
- the system sends an initial progress snapshot from durable state
- the system streams live upload progress updates after the snapshot

Expected result:

- the user can observe upload progress without polling

### 3. Observe Processing Progress

Supported behavior:

- once upload is complete, processing work is distributed across processing-service replicas
- the system emits per-segment and per-profile transcode progress updates
- the user can continue to observe progress through the same status channel

Expected result:

- the user can see the video move from uploaded to processing to ready

### 4. Play A Ready Video

Supported behavior:

- a user requests the list of videos in `READY` state
- a user requests a master manifest for a ready video
- a user requests or plays variant playlists for specific bitrate profiles
- segment bytes are fetched from object storage through presigned URLs

Expected result:

- a ready video can be played through the browser as HLS content

### 5. Scale Services Across Multiple Nodes

Supported behavior:

- multiple upload-service replicas may accept requests
- multiple status-service replicas may serve WebSocket clients
- multiple processing-service replicas may consume distributed transcode work
- multiple streaming-service replicas may serve playback metadata

Expected result:

- the system continues to operate correctly when the workload is spread across service replicas

## Error Cases And Recovery In Scope

The error scenarios in `docs/challenges.md` are explicitly in scope and are part of the expected delivered behavior.


## Out Of Scope

The following scenarios are not part of the current delivery scope:

- authentication and authorization
- CDN integration or edge delivery
- live streaming
- custom distributed database design
- internal replication of object storage
- production-grade media optimization beyond the implemented profiles

## Supported Scenario Summary

The project delivers support for:

- upload
- upload progress visibility
- processing progress visibility
- ready-video discovery
- HLS playback for ready videos
- multi-replica service execution
- recovery from upload-service, status-service, processing-service, and streaming-service interruptions within the implemented failure model
