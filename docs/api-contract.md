# API Contract Document

This document captures the external HTTP API contract for the system. It is intended to freeze client-visible behavior and decouple interface from implementation.
## Objective
Provide a clear, shared reference for client interactions. Once merged, any changes to endpoint signatures, request/response formats, or error behavior must be reviewed as breaking changes.

## Default Ports (Docker Swarm)
- Upload service: `8080`
- Status service: `8081` (WebSocket `/upload-status`)
- Processing service: `8082`
- Streaming service: `8083`

## 1. Upload Service Endpoints

#### `POST /upload`
- Request format: multipart form
	- Fields: `file` (video) and `name` (video title)
- Success response: `202 Accepted`
	```json
	{ "videoId": "<uuid>", "uploadStatusUrl": "ws(s)://<host>/upload-status?jobId=<uuid>" }
	```
- Notes:
	- `uploadStatusUrl` points to the status service WebSocket
	- the browser uses that socket for both source-chunk upload progress and transcoding progress
- Error responses:
	- `400 Bad Request` — no file or name provided
	- `500 Internal Server Error` — storage failure

#### `GET /upload-status?jobId={videoId}`
- Purpose: Real-time status updates via WebSocket, eliminating the need for polling
- Protocol: HTTP upgrade to WebSocket (endpoint corresponds to `uploadStatusUrl`)
- Message format: JSON status events pushed to the client as upload and transcoding progress
- Event types currently sent over the socket:
	- `task`
		- emitted when a source chunk is uploaded by `upload-service`
		- example:
			```json
			{
				"jobId": "<uuid>",
				"taskId": "<videoId>/chunks/output7.ts"
			}
			```
		- note:
			- these messages currently do not include a `type` field
			- clients should treat a message with `jobId` + `taskId` and no explicit `type` as a source chunk upload progress event
	- `meta`
		- emitted when the upload service knows the total number of source segments
		- example:
			```json
			{
				"jobId": "<uuid>",
				"totalSegments": 12,
				"type": "meta"
			}
			```
	- `transcode_progress`
		- emitted by `processing-service` as each `(segment, profile)` task is queued / completes / fails
		- example:
			```json
			{
				"jobId": "<uuid>",
				"profile": "high",
				"segmentNumber": 7,
				"state": "QUEUED | TRANSCODING | UPLOADING | DONE | FAILED",
				"doneSegments": 5,
				"totalSegments": 12,
				"type": "transcode_progress"
			}
			```
	- `failed`
		- emitted for terminal upload/processing failures
		- example:
			```json
			{
				"jobId": "<uuid>",
				"reason": "container_died",
				"machineId": "node-a",
				"containerId": "abc123",
				"type": "failed"
			}
			```
- Snapshot behavior on connect:
	- the status service first sends a DB-backed snapshot of completed source chunks
	- it then sends per-profile transcode `DONE` counts from Postgres
	- after that, the client receives live RabbitMQ-backed status events over the same socket
- Connection lifecycle:
	- Client initiates HTTP upgrade request to WebSocket
	- Server sends an immediate progress snapshot upon connection
	- Server pushes subsequent status updates as they occur
	- Client may close the connection at any time
- Error responses:
	- `400 Bad Request` — invalid WebSocket upgrade request

## 2. Streaming Service Endpoints

#### Service status
- Streaming endpoints serve clients requesting playback assets

#### `GET /stream/ready`
- Returns a list of videos that are ready for playback (ID + name)
- Only includes videos that are `COMPLETED` and have a manifest in object storage
- Success response: `200 OK`
	- Body: JSON array of objects:
		```json
		[
			{ "videoId": "<uuid-1>", "videoName": "Intro to Streaming" },
			{ "videoId": "<uuid-2>", "videoName": "Advanced Encoding" }
		]
		```

#### `GET /stream/{videoId}/manifest`
- Returns the master HLS manifest for a ready video
- Success response: `200 OK`
	- Body: HLS playlist (M3U8) manifest
	- `Content-Type: application/vnd.apple.mpegurl`
	- URIs are rewritten so variant playlists resolve to `/stream/{videoId}/variant/{profile}/playlist.m3u8`
- Error responses:
	- `404 Not Found` — unknown video ID
	- `409 Conflict` — video exists but is not yet ready for streaming

#### `GET /stream/{videoId}/variant/{profile}/playlist.m3u8`
- Returns a variant profile playlist for a ready video
- Success response: `200 OK`
	- Body: HLS playlist (M3U8) variant manifest
	- `Content-Type: application/vnd.apple.mpegurl`
	- Segment URIs are rewritten to `segment/{segmentId}` so they resolve under this endpoint
- Error responses:
	- `400 Bad Request` — invalid profile
	- `404 Not Found` — unknown video or missing variant manifest

#### `GET /stream/{videoId}/variant/{profile}/segment/{segmentId}`
- Returns a specific video segment
- Success response:
	- `200 OK` — full segment response
	- May return `206 Partial Content` when honoring `Range` requests
	- Content-Type: `video/MP2T` (represents a single `.ts` segment)
	- Supports HTTP byte-range requests via the `Range` header
	- Cacheable by CDNs/clients (specific `Cache-Control` directives TBD)
- Error responses:
	- `404 Not Found` — unknown video or segment

## 3. Processing Service Endpoints

#### `GET /health`
- Purpose: Liveness probe for Docker healthchecks and load balancers
- Success response: `200 OK`
	```json
	{ "status": "ok" }
	```

#### `GET /workers`
- Purpose: Operational snapshot of the transcoding worker pool
- Success response: `200 OK`
	```json
	{
		"queued": 12,
		"workers": [
			{ "id": "worker-0", "status": "BUSY" },
			{ "id": "worker-1", "status": "IDLE" }
		]
	}
	```
- `status` values: `IDLE` | `BUSY` | `OFFLINE`

---
### Lifecycle and Visibility Guarantees
> ### Will be expanded in a separate doc in week 4
- Streaming endpoints return `409` (not `404`) for videos that exist but are not ready
- Video IDs are stable and permanent once issued

### Contract Intentionally Hides
- Internal storage paths and bucket layout
- RabbitMQ status-queue vs task-queue routing
- Processing node coordination
- Segmentation and transcoding implementation details
- Inter-service communication
