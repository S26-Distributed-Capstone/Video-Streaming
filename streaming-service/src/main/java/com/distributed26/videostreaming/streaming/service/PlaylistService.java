package com.distributed26.videostreaming.streaming.service;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public final class PlaylistService {
    /**
     * TTL for the presigned redirect URL generated when a client fetches a
     * single segment.  Kept short (10 s) so that URLs cannot be meaningfully
     * shared or replayed after the segment has been delivered.
     */
    private static final long SEGMENT_PRESIGNED_URL_TTL_SECONDS = 10;

    private static final long PLAYLIST_CACHE_TTL_MILLIS = 30 * 60 * 1000L;

    private final ObjectStorageClient storageClient;
    private final Map<String, CachedPlaylist> playlistCache = new ConcurrentHashMap<>();

    public PlaylistService(ObjectStorageClient storageClient) {
        this.storageClient = storageClient;
    }

    public String loadMasterManifest(String videoId) throws IOException {
        String objectKey = videoId + "/manifest/master.m3u8";
        try (InputStream is = storageClient.downloadFile(objectKey)) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return rewriteMasterManifest(content);
        } catch (NoSuchKeyException e) {
            throw e;
        }
    }

    /**
     * Returns a variant playlist whose segment lines are proxy URLs that point
     * back to the streaming service ({@code /stream/{videoId}/segment/…}).
     * Because these URLs never expire, the cached manifest stays valid for its
     * full cache TTL regardless of video length.
     */
    public String loadVariantManifest(String videoId, String profile) throws IOException {
        String cacheKey = videoId + "/" + profile;
        CachedPlaylist cached = playlistCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return cached.content();
        }

        String objectKey = videoId + "/manifest/" + profile + ".m3u8";
        try (InputStream is = storageClient.downloadFile(objectKey)) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            String rewritten = rewriteVariantManifestWithProxyUrls(content, videoId, profile);
            playlistCache.put(cacheKey, new CachedPlaylist(rewritten, System.currentTimeMillis()));
            return rewritten;
        } catch (NoSuchKeyException e) {
            throw e;
        }
    }

    /**
     * Generates a short-lived presigned URL for a single segment.
     * Called on every segment request so the client always receives a fresh URL.
     */
    public String generateSegmentUrl(String videoId, String profile, String segment) {
        String objectKey = videoId + "/processed/" + profile + "/" + segment;
        return storageClient.generatePresignedUrl(objectKey, SEGMENT_PRESIGNED_URL_TTL_SECONDS);
    }

    public void invalidateVideo(String videoId) {
        String prefix = videoId + "/";
        playlistCache.keySet().removeIf(key -> key.startsWith(prefix));
    }

    static String rewriteMasterManifest(String content) {
        String[] lines = content.split("\\r?\\n");
        StringBuilder rewritten = new StringBuilder(content.length() + lines.length * 8);
        for (String line : lines) {
            if (!line.isEmpty()
                    && !line.startsWith("#")
                    && !line.startsWith("variant/")
                    && !line.startsWith("http://")
                    && !line.startsWith("https://")) {
                rewritten.append("variant/").append(line);
            } else {
                rewritten.append(line);
            }
            rewritten.append('\n');
        }
        return rewritten.toString();
    }

    /**
     * Rewrites segment references in a variant manifest to proxy URLs on the
     * streaming service.  The player will hit
     * {@code /stream/{videoId}/segment/{profile}/{file}} which generates a
     * fresh presigned redirect at request time.
     */
    static String rewriteVariantManifestWithProxyUrls(
            String content,
            String videoId,
            String profile
    ) {
        String[] lines = content.split("\\r?\\n");
        StringBuilder rewritten = new StringBuilder(content.length() * 2);
        for (String line : lines) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                String segmentFile = line.endsWith(".ts") ? line : line + ".ts";
                rewritten.append("/stream/").append(videoId)
                        .append("/segment/").append(profile)
                        .append("/").append(segmentFile);
            } else {
                rewritten.append(line);
            }
            rewritten.append('\n');
        }
        return rewritten.toString();
    }


    private record CachedPlaylist(String content, long createdAtMillis) {
        boolean isExpired() {
            return System.currentTimeMillis() - createdAtMillis > PLAYLIST_CACHE_TTL_MILLIS;
        }
    }
}
