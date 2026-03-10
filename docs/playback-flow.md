
# Client → Streaming Service → MinIO: Playback Flow

---

## 1. User clicks "Play Selected"

```
app.js  →  startStreamingPlayback(videoId)
```

The frontend builds a manifest URL from the selected video ID:

```
http://localhost:8083/stream/{videoId}/manifest
```

HLS.js (or native Safari) is initialized with this URL.

---

## 2. HLS.js fetches the master manifest

```
Browser                          Streaming Service (8083)                   MinIO (9000)
  │                                     │                                       │
  │  GET /stream/{videoId}/manifest     │                                       │
  │ ──────────────────────────────────► │                                       │
  │                                     │  downloadFile("{videoId}/manifest/    │
  │                                     │    master.m3u8")                      │
  │                                     │ ────────────────────────────────────► │
  │                                     │ ◄──── raw master.m3u8 ────────────── │
  │                                     │                                       │
  │                                     │  rewriteMasterManifest():             │
  │                                     │    "low/playlist.m3u8"                │
  │                                     │      → "variant/low/playlist.m3u8"   │
  │                                     │                                       │
  │ ◄── rewritten master manifest ──── │                                       │
```

**What HLS.js receives:**
```
#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=800000
variant/low/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000
variant/medium/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000
variant/high/playlist.m3u8
```

HLS.js sees three quality levels and picks one based on current bandwidth (e.g., starts with `low`).

---

## 3. HLS.js fetches a variant playlist

The `variant/` prefix resolves relative to the manifest URL, so HLS.js requests:

```
GET /stream/{videoId}/variant/low/playlist.m3u8
```

```
Browser                          Streaming Service (8083)                   MinIO (9000)
  │                                     │                                       │
  │  GET /stream/{videoId}/variant/     │                                       │
  │    low/playlist.m3u8                │                                       │
  │ ──────────────────────────────────► │                                       │
  │                                     │  check playlist cache → miss          │
  │                                     │                                       │
  │                                     │  downloadFile("{videoId}/manifest/    │
  │                                     │    low.m3u8")                         │
  │                                     │ ────────────────────────────────────► │
  │                                     │ ◄──── raw low.m3u8 ──────────────── │
  │                                     │                                       │
  │                                     │  rewriteVariantManifestWithPresigned- │
  │                                     │  Urls():                              │
  │                                     │    for each segment filename:         │
  │                                     │      generatePresignedUrl(            │
  │                                     │        "{videoId}/processed/low/      │
  │                                     │          output0.ts", 3600s)          │
  │                                     │      → local HMAC-SHA256 signing      │
  │                                     │        (NO network call to MinIO)     │
  │                                     │                                       │
  │                                     │  cache rewritten playlist (30 min)    │
  │                                     │                                       │
  │ ◄── rewritten variant playlist ─── │                                       │
```

**What HLS.js receives:**
```
#EXTM3U
#EXT-X-VERSION:6
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:10
#EXTINF:10.000,
http://localhost:9000/uploads/{videoId}/processed/low/output0.ts?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Signature=abc123...
#EXTINF:10.000,
http://localhost:9000/uploads/{videoId}/processed/low/output1.ts?X-Amz-Algorithm=AWS4-HMAC-SHA256&...
#EXT-X-ENDLIST
```

The segment URIs are now **presigned S3 URLs** pointing directly at MinIO's public endpoint (`MINIO_PUBLIC_ENDPOINT`).

---

## 4. HLS.js fetches segments directly from MinIO

**This is the key change.** The streaming service is no longer involved.

```
Browser                                                                     MinIO (9000)
  │                                                                             │
  │  GET /uploads/{videoId}/processed/low/output0.ts?X-Amz-Signature=...       │
  │ ──────────────────────────────────────────────────────────────────────────► │
  │ ◄──── .ts segment bytes (video/MP2T) ──────────────────────────────────── │
  │                                                                             │
  │  GET /uploads/{videoId}/processed/low/output1.ts?X-Amz-Signature=...       │
  │ ──────────────────────────────────────────────────────────────────────────► │
  │ ◄──── .ts segment bytes ───────────────────────────────────────────────── │
  │                                                                             │
  │  (continues for each segment...)                                            │
```

---

## 5. Adaptive bitrate switching

If bandwidth improves, HLS.js requests a higher-quality variant playlist:

```
Browser  →  GET /stream/{videoId}/variant/high/playlist.m3u8  →  Streaming Service
         ◄── playlist with presigned URLs for high/ segments  ◄──
Browser  →  GET /uploads/{videoId}/processed/high/output0.ts?...  →  MinIO (direct)
```

---

## Summary: What talks to what

| Step | Browser → | Returns |
|------|-----------|---------|
| **Master manifest** | Streaming Service `:8083` | List of variant playlists |
| **Variant playlist** | Streaming Service `:8083` | Segment list with presigned S3 URLs |
| **Video segments** | **MinIO `:9000` (direct)** | Raw `.ts` bytes |

### Before (old proxy flow)
```
Browser → Streaming Service → MinIO → Streaming Service → Browser
          (downloaded bytes)           (proxied bytes)
```

### After (presigned URL flow)
```
Browser → Streaming Service    (manifests only — small text files)
Browser → MinIO                (segments — large binary data, direct)
```

The streaming service only handles **metadata** (manifests). All **heavy data** (video segments) flows directly from object storage to the browser.

---

## Relevant environment variables

| Variable | Used by | Purpose |
|----------|---------|---------|
| `MINIO_ENDPOINT` | All services internally | Docker-internal MinIO address (`http://minio:9000`) |
| `MINIO_PUBLIC_ENDPOINT` | Streaming service only | Browser-reachable MinIO address baked into presigned URLs (`http://localhost:9000` for dev) |

