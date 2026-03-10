package com.distributed26.videostreaming.streaming.service;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public final class PlaylistService {
    private static final long PRESIGNED_URL_DURATION_SECONDS = 3600;
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

    public String loadVariantManifest(String videoId, String profile) throws IOException {
        String cacheKey = videoId + "/" + profile;
        CachedPlaylist cached = playlistCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return cached.content();
        }

        String objectKey = videoId + "/manifest/" + profile + ".m3u8";
        try (InputStream is = storageClient.downloadFile(objectKey)) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            String rewritten = rewriteVariantManifestWithPresignedUrls(content, videoId, profile);
            playlistCache.put(cacheKey, new CachedPlaylist(rewritten, System.currentTimeMillis()));
            return rewritten;
        } catch (NoSuchKeyException e) {
            throw e;
        }
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

    static String rewriteVariantManifestWithPresignedUrls(
            String content,
            String videoId,
            String profile,
            ObjectStorageClient storageClient
    ) {
        String[] lines = content.split("\\r?\\n");
        StringBuilder rewritten = new StringBuilder(content.length() * 2);
        for (String line : lines) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                String segmentFile = line.endsWith(".ts") ? line : line + ".ts";
                String objectKey = videoId + "/processed/" + profile + "/" + segmentFile;
                String presignedUrl = storageClient.generatePresignedUrl(objectKey, PRESIGNED_URL_DURATION_SECONDS);
                rewritten.append(presignedUrl);
            } else {
                rewritten.append(line);
            }
            rewritten.append('\n');
        }
        return rewritten.toString();
    }

    private String rewriteVariantManifestWithPresignedUrls(String content, String videoId, String profile) {
        return rewriteVariantManifestWithPresignedUrls(content, videoId, profile, storageClient);
    }

    private record CachedPlaylist(String content, long createdAtMillis) {
        boolean isExpired() {
            return System.currentTimeMillis() - createdAtMillis > PLAYLIST_CACHE_TTL_MILLIS;
        }
    }
}
