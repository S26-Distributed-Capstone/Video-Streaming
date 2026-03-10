package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamingServiceApplication {
    private static final Logger logger = LogManager.getLogger(StreamingServiceApplication.class);
    private static final int DEFAULT_STREAMING_PORT = 8083;

    public static void main(String[] args) {
        int port = DEFAULT_STREAMING_PORT;
        String portEnv = System.getenv("STREAMING_PORT");
        if (portEnv != null && !portEnv.isBlank()) {
            try {
                port = Integer.parseInt(portEnv.trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid STREAMING_PORT '{}', using default {}", portEnv, port);
            }
        }
        Javalin app = createStreamingApp();
        logger.info("Starting streaming service on port {}", port);
        app.start(port);
    }

    static Javalin createStreamingApp() {
        StorageConfig storageConfig = loadStorageConfig();
        if (storageConfig.getPublicEndpointUrl().equals(storageConfig.getEndpointUrl())) {
            logger.warn("MINIO_PUBLIC_ENDPOINT is not set — presigned URLs will use internal endpoint '{}'. "
                    + "Set MINIO_PUBLIC_ENDPOINT to a browser-accessible URL.", storageConfig.getEndpointUrl());
        } else {
            logger.info("Presigned URLs will use public endpoint: {}", storageConfig.getPublicEndpointUrl());
        }
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        VideoStatusRepository videoStatusRepository = createVideoStatusRepository();
        return createStreamingApp(storageClient, videoStatusRepository);
    }

    static Javalin createStreamingApp(ObjectStorageClient storageClient, VideoStatusRepository videoStatusRepository) {
        Map<String, CachedPlaylist> playlistCache = new ConcurrentHashMap<>();

        Javalin app = Javalin.create(config -> {
            config.http.prefer405over404 = true;
        });

        app.events(event -> event.serverStopped(() -> {
            try {
                storageClient.close();
            } catch (Exception e) {
                logger.warn("Error closing storage client on shutdown", e);
            }
        }));

        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type,Range");
        });
        app.options("/*", ctx -> ctx.status(204));
        app.get("/stream/{videoId}/manifest", ctx -> {
            if (!validateVideoId(ctx)) {
                return;
            }
            if (!requireCompleted(ctx, videoStatusRepository)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String objectKey = videoId + "/manifest/master.m3u8";
            try (InputStream is = storageClient.downloadFile(objectKey)) {
                String content = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                String rewritten = rewriteMasterManifest(content);
                ctx.status(HttpStatus.OK)
                    .header("Cache-Control", "no-store, max-age=0")
                    .contentType("application/vnd.apple.mpegurl")
                    .result(rewritten);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Manifest not found");
            } catch (IOException e) {
                logger.error("Failed to read manifest {}", objectKey, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read manifest");
            }
        });
        app.get("/stream/{videoId}/variant/{profile}/playlist.m3u8", ctx -> {
            if (!validateVideoId(ctx)) {
                return;
            }
            String profile = ctx.pathParam("profile");
            if (!validateProfile(profile)) {
                ctx.status(HttpStatus.BAD_REQUEST).result("Invalid profile");
                return;
            }
            if (!requireCompleted(ctx, videoStatusRepository)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String cacheKey = videoId + "/" + profile;
            CachedPlaylist cached = playlistCache.get(cacheKey);
            if (cached != null && !cached.isExpired()) {
                logger.info("Serving cached variant playlist for videoId={} profile={}", videoId, profile);
                ctx.status(HttpStatus.OK)
                    .header("Cache-Control", "no-store, max-age=0")
                    .contentType("application/vnd.apple.mpegurl")
                    .result(cached.content());
                return;
            }
            String objectKey = videoId + "/manifest/" + profile + ".m3u8";
            try (InputStream is = storageClient.downloadFile(objectKey)) {
                String content = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                String rewritten = rewriteVariantManifestWithPresignedUrls(content, videoId, profile, storageClient);
                long segmentCount = rewritten.lines().filter(l -> l.startsWith("http")).count();
                logger.info("Generated variant playlist for videoId={} profile={} with {} presigned segment URLs",
                        videoId, profile, segmentCount);
                playlistCache.put(cacheKey, new CachedPlaylist(rewritten, System.currentTimeMillis()));
                ctx.status(HttpStatus.OK)
                    .header("Cache-Control", "no-store, max-age=0")
                    .contentType("application/vnd.apple.mpegurl")
                    .result(rewritten);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Variant manifest not found");
            } catch (IOException e) {
                logger.error("Failed to read variant manifest {}", objectKey, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read variant manifest");
            }
        });


        app.get("/stream/ready", ctx -> {
            if (videoStatusRepository == null) {
                ctx.status(500).result("Streaming status checks are not configured");
                return;
            }
            int limit = 50;
            String limitParam = ctx.queryParam("limit");
            if (limitParam != null && !limitParam.isBlank()) {
                try {
                    limit = Math.max(1, Integer.parseInt(limitParam.trim()));
                } catch (NumberFormatException ignored) {
                    // Keep default limit.
                }
            }
            var candidates = videoStatusRepository.findCompletedVideos(limit);
            var filtered = candidates.stream()
                .filter(video -> storageClient.fileExists(video.videoId() + "/manifest/master.m3u8"))
                .map(video -> new ReadyVideoResponse(
                    video.videoId(),
                    video.videoName() == null || video.videoName().isBlank() ? video.videoId() : video.videoName()
                ))
                .toList();
            ctx.status(HttpStatus.OK).json(filtered);
        });

        return app;
    }

    private record ReadyVideoResponse(String videoId, String videoName) {
    }

    private static StorageConfig loadStorageConfig() {
        Properties props = new Properties();
        try (InputStream input = StreamingServiceApplication.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            logger.warn("Could not load application.properties, using defaults", e);
        }

        return new StorageConfig(
            getEnvOrProp("MINIO_ENDPOINT", props, "minio.endpoint", "http://localhost:9000"),
            getEnvOrProp("MINIO_PUBLIC_ENDPOINT", props, "minio.public-endpoint", null),
            getEnvOrProp("MINIO_ACCESS_KEY", props, "minio.access-key", "minioadmin"),
            getEnvOrProp("MINIO_SECRET_KEY", props, "minio.secret-key", "minioadmin"),
            getEnvOrProp("MINIO_BUCKET_NAME", props, "minio.bucket-name", "uploads"),
            getEnvOrProp("MINIO_REGION", props, "minio.region", "us-east-1")
        );
    }

    private static String getEnvOrProp(String envKey, Properties props, String propKey, String defaultValue) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) {
            return envVal;
        }
        return props.getProperty(propKey, defaultValue);
    }

    private static VideoStatusRepository createVideoStatusRepository() {
        try {
            return VideoStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            logger.warn("Postgres not configured; streaming status checks disabled: {}", e.getMessage());
            return null;
        }
    }

    private static boolean requireCompleted(io.javalin.http.Context ctx, VideoStatusRepository repository) {
        if (repository == null) {
            ctx.status(500).result("Streaming status checks are not configured");
            return false;
        }
        String videoId = ctx.pathParam("videoId");
        Optional<String> status;
        try {
            status = repository.findStatusByVideoId(videoId);
        } catch (Exception e) {
            logger.error("Failed to load video status for videoId={}", videoId, e);
            ctx.status(500).result("Failed to load video status");
            return false;
        }
        if (status.isEmpty()) {
            ctx.status(404).result("Video not found");
            return false;
        }
        if (!"COMPLETED".equalsIgnoreCase(status.get())) {
            ctx.status(409).result("Video is not ready");
            return false;
        }
        return true;
    }

    private static boolean validateVideoId(io.javalin.http.Context ctx) {
        String videoId = ctx.pathParam("videoId");
        try {
            java.util.UUID.fromString(videoId);
        } catch (IllegalArgumentException e) {
            ctx.status(400).result("Invalid video ID");
            return false;
        }
        return true;
    }

    private static boolean validateProfile(String profile) {
        return profile != null && profile.matches("^[A-Za-z0-9_-]+$");
    }

    private static String rewriteMasterManifest(String content) {
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

    private static final long PRESIGNED_URL_DURATION_SECONDS = 3600; // 1 hour
    private static final long PLAYLIST_CACHE_TTL_MILLIS = 30 * 60 * 1000L; // 30 minutes

    private record CachedPlaylist(String content, long createdAtMillis) {
        boolean isExpired() {
            return System.currentTimeMillis() - createdAtMillis > PLAYLIST_CACHE_TTL_MILLIS;
        }
    }

    private static String rewriteVariantManifestWithPresignedUrls(
            String content, String videoId, String profile, ObjectStorageClient storageClient) {
        String[] lines = content.split("\\r?\\n");
        StringBuilder rewritten = new StringBuilder(content.length() * 2);
        for (String line : lines) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                // This is a segment filename — replace with a presigned S3 URL
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
}
