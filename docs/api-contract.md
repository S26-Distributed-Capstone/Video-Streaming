# API Contract Document

This document captures the external HTTP API contract for the system. It is intended to freeze client-visible behavior and decouple interface from implementation. Endpoints that are not yet implemented remain marked as planned.

## Objective
Provide a clear, shared reference for client interactions. Once merged, any changes to endpoint signatures, request/response formats, or error behavior must be reviewed as breaking changes.

## 1. Upload Service Endpoints

#### `GET /health`
- Purpose: liveness/health check for upload-service
- Success response: `200 OK`
	```json
	{ "status": "UP", "service": "upload-service", "timestamp": "<iso8601>" }
	```
- Error responses: Probably not needed

#### `POST /upload` (planned)
- Request format: multipart form, field name `file`
- Success response: `201 Created`
	```json
	{ "videoId": "<uuid>" }
	```
- Error responses:
	- `400 Bad Request` — no file provided
	- `500 Internal Server Error` — storage failure

#### `GET /video/{videoId}/status` (planned)
- Success response: `200 OK`
	```json
	{
		"videoId": "<uuid>",
		"state": "UPLOADED | PROCESSING | READY | FAILED"
	}
	```
- Error responses:
	- `404 Not Found` — unknown video ID

## 2. Streaming Service Endpoints

#### Service status
- Streaming endpoints serve clients requesting playback assets

#### `GET /stream/{videoId}/manifest` (planned)
- Returns the HLS or DASH manifest for a ready video
- Error responses:
	- `404 Not Found` — unknown video ID
	- `409 Conflict` — video exists but is not yet ready for streaming

#### `GET /stream/{videoId}/segment/{segmentId}` (planned)
- Returns a specific video segment
- Error responses:
	- `404 Not Found` — unknown video or segment

#### `GET /health` (planned)
- Intended to match upload-service health response structure

## 3. Processing Service Endpoints
- No external HTTP API by design at this stage
- If an API is added later, document it here

---
### Lifecycle and Visibility Guarantees
>### Will be expanded in a separate doc in week 4
- Streaming endpoints return `409` (not `404`) for videos that exist but are not ready
- Video IDs are stable and permanent once issued

### Contract Intentionally Hides
- Internal storage paths and bucket layout
- Processing node coordination
- Segmentation and transcoding implementation details
- Inter-service communication