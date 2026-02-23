# API Contract Document

This document captures the external HTTP API contract for the system. It is intended to freeze client-visible behavior and decouple interface from implementation.
## Objective
Provide a clear, shared reference for client interactions. Once merged, any changes to endpoint signatures, request/response formats, or error behavior must be reviewed as breaking changes.

## 1. Upload Service Endpoints

#### `POST /upload`
- Request format: multipart form, field name `file`
- Success response: `201 Created`
	```json
	{ "videoId": "<uuid>" }
	```
- Error responses:
	- `400 Bad Request` — no file provided
	- `500 Internal Server Error` — storage failure

#### `GET /video/{videoId}/status/stream`
- Purpose: Real-time status updates via WebSocket, eliminating the need for polling
- Protocol: HTTP upgrade to WebSocket
- Message format: JSON status messages pushed to client as the video progresses through lifecycle states
	```json
	{
		"videoId": "<uuid>",
		"state": "UPLOADED | PROCESSING | READY | FAILED",
		"timestamp": "<iso8601>"
	}
	```
- Connection lifecycle:
	- Client initiates HTTP upgrade request to WebSocket
	- Server sends immediate status update upon connection
	- Server pushes subsequent status updates as they occur
	- Client may close the connection at any time
	- Server closes the connection after sending a terminal state (`READY` or `FAILED`)
- Error responses:
	- `404 Not Found` — unknown video ID (before upgrade)
	- `400 Bad Request` — invalid WebSocket upgrade request

## 2. Streaming Service Endpoints

#### Service status
- Streaming endpoints serve clients requesting playback assets

#### `GET /stream/ready`
- Returns a list of video IDs that are ready for playback
- Only includes videos that are `COMPLETED` and have a manifest in object storage
- Success response: `200 OK`
	- Body: JSON array of video IDs

#### `GET /stream/{videoId}/manifest`
- Returns the HLS manifest for a ready video
- Success response: `200 OK`
	- Body: HLS playlist (M3U8) manifest
	- `Content-Type: application/vnd.apple.mpegurl`
	- Segment URIs are rewritten to `segment/{segmentId}` so they resolve under this endpoint
- Error responses:
	- `404 Not Found` — unknown video ID
	- `409 Conflict` — video exists but is not yet ready for streaming

#### `GET /stream/{videoId}/segment/{segmentId}`
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
- No external HTTP API by design at this stage
- If an API is added later, document it here

---
### Lifecycle and Visibility Guarantees
> ### Will be expanded in a separate doc in week 4
- Streaming endpoints return `409` (not `404`) for videos that exist but are not ready
- Video IDs are stable and permanent once issued

### Contract Intentionally Hides
- Internal storage paths and bucket layout
- Processing node coordination
- Segmentation and transcoding implementation details
- Inter-service communication
