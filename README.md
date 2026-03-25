# Video Upload/Processing/Playback System
> Distributed Systems Spring 2026 Capstone Project

This project implements a **distributed video upload, processing, and streaming system** designed to handle large video files, prepare them for adaptive bitrate streaming, and serve them to clients efficiently. The system demonstrates key distributed systems concepts including asynchronous processing, multi-node coordination, fault tolerance, and observable behavior under concurrency and failure.

The emphasis is on **correctness and explainability** rather than production-scale performance or media quality. Every design decision is made with the goal of creating a system whose behavior can be understood, verified, and debugged through logs and state inspection.

## Summary

Users upload video files through an HTTP API or the demo's provided frontend. The system automatically:
1. **Chunks** the incoming video into time-based segments during upload
2. **Transcodes** each segment into multiple quality profiles (480p, 720p, 1080p) for adaptive streaming
3. **Stores** all processed outputs in S3-compatible object storage (MinIO)
4. **Serves** adaptive video streams to clients via HLS manifests and segment endpoints

The system comprises three independent services:
- **Upload Service**: Accepts video uploads, segments them, and stores chunks in MinIO
- **Processing Service**: Listens for upload events, transcodes segments into multiple bitrates
- **Streaming Service**: Serves HLS manifests and video segments to clients

Services coordinate through **RabbitMQ** for event-driven processing and **MinIO** for durable storage. State transitions are observable via WebSocket status streams and system logs.

## Functional Overview

### Upload Flow
1. Client sends a video file via `POST /upload`
2. Upload service generates a unique Video ID (UUID)
3. Video is segmented into 6-second chunks using FFmpeg
4. Chunks are written to MinIO at `uploads/{videoId}/chunks/{chunkIndex}`
5. Upload service publishes a `VIDEO_UPLOADED` event to RabbitMQ
6. Client receives the Video ID and can monitor status via WebSocket

### Processing Flow
1. Processing service receives `VIDEO_UPLOADED` event from RabbitMQ
2. For each chunk, creates transcoding tasks for three quality profiles:
   - Low: 480p @ 800 kbps
   - Medium: 720p @ 2.5 Mbps
   - High: 1080p @ 5 Mbps
3. Worker pool processes tasks concurrently using FFmpeg
4. Transcoded segments written to MinIO at `uploads/{videoId}/processed/{quality}/{chunkIndex}`
5. Video state transitions: `UPLOADED` → `PROCESSING` → `READY`
6. Status updates pushed to clients via WebSocket

### Streaming Flow
1. Client requests HLS manifest via `GET /stream/{videoId}/manifest`
2. Streaming service verifies video is in `READY` state (returns `409` if not)
3. Manifest returned with references to all quality profiles and segments
4. Client requests individual segments via `GET /stream/{videoId}/segment/{segmentId}`
5. Streaming service fetches from MinIO and serves to client
6. Client player switches between quality profiles based on bandwidth

## Key Design Principles

**Separation of Concerns**: Upload, processing, and streaming are independent services with clear responsibilities.

**Event-Driven Architecture**: Services communicate asynchronously via RabbitMQ, enabling loose coupling and independent scaling.

**Idempotency**: Processing operations check if output already exists before transcoding, making retries safe.

**Observable Behavior**: Every state transition is logged. WebSocket streams provide real-time visibility into video lifecycle.

**Fault Tolerance**: Node failures during processing don't corrupt data. Failed jobs can be retried. Partial work is tracked and resumable.

**Storage as Source of Truth**: MinIO holds all video data (chunks, transcoded segments). Services are stateless and can restart without data loss.

## Technologies

The project’s technology choices, tradeoffs, and evaluated alternatives are documented in `docs/technologies.md`.

## System Guarantees

✅ **Upload Durability**: Video ID is only returned after chunks are written to MinIO  
✅ **Processing Idempotency**: Re-processing the same video doesn't duplicate work  
✅ **Streaming Correctness**: Only `READY` videos can be streamed; others return `409 Conflict`  
✅ **State Visibility**: Clients can observe video lifecycle in real-time via WebSocket  
✅ **Concurrent Safety**: Multiple uploads and streams don't corrupt shared state  
✅ **Failure Recovery**: Services can restart and resume work without manual intervention  

## Non-Goals

This project intentionally does **not** focus on:
- CDN integration or edge caching
- Authentication or authorization
- Production-grade media encoding optimization
- Horizontal auto-scaling
- Building a distributed object store (MinIO handles this)
- Real-time live streaming (VOD only)

The goal is to build a system that **works correctly**, **fails gracefully**, and whose **behavior is always explainable**.
