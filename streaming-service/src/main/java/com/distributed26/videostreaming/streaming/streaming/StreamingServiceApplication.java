package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Optional;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamingServiceApplication {
    private static final Logger logger = LogManager.getLogger(StreamingServiceApplication.class);
    private static final int DEFAULT_STREAMING_PORT = 8082;
    private static final String INSTANCE_ID = resolveInstanceId();

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
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        VideoStatusRepository videoStatusRepository = createVideoStatusRepository();
        return createStreamingApp(storageClient, videoStatusRepository);
    }

    static Javalin createStreamingApp(ObjectStorageClient storageClient, VideoStatusRepository videoStatusRepository) {
        Javalin app = Javalin.create(config -> {
            config.http.prefer405over404 = true;
        });
        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type,Range");
        });
        app.options("/*", ctx -> ctx.status(204));
        app.before(ctx -> {
            logger.info("streaming_request instance={} method={} path={} query={} remote={}",
                INSTANCE_ID,
                ctx.method(),
                ctx.path(),
                ctx.queryString(),
                ctx.ip()
            );
        });

        // Route stubs - logic will be implemented in next steps.
        app.get("/stream/{videoId}/manifest", ctx -> {
            if (!validateVideoId(ctx)) {
                return;
            }
            if (!requireCompleted(ctx, videoStatusRepository)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String objectKey = videoId + "/chunks/output.m3u8";
            try (InputStream is = storageClient.downloadFile(objectKey)) {
                String content = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                String rewritten = rewriteManifest(content);
                ctx.status(HttpStatus.OK)
                    .contentType("application/vnd.apple.mpegurl")
                    .result(rewritten);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Manifest not found");
            } catch (IOException e) {
                logger.error("Failed to read manifest {}", objectKey, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read manifest");
            }
        });
        app.get("/stream/{videoId}/segment/{segmentId}", ctx -> {
            if (!validateVideoId(ctx)) {
                return;
            }
            if (!requireCompleted(ctx, videoStatusRepository)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String segmentId = ctx.pathParam("segmentId");
            if (!isValidSegmentId(segmentId)) {
                ctx.status(HttpStatus.BAD_REQUEST).result("Invalid segment ID");
                return;
            }
            String fileName = segmentId.endsWith(".ts") ? segmentId : segmentId + ".ts";
            String objectKey = videoId + "/chunks/" + fileName;
            try (InputStream is = storageClient.downloadFile(objectKey)) {
                byte[] content = is.readAllBytes();
                ctx.status(HttpStatus.OK)
                    .contentType("video/MP2T")
                    .result(content);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Segment not found");
            } catch (IOException e) {
                logger.error("Failed to read segment {}", objectKey, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read segment");
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
            var candidates = videoStatusRepository.findCompletedVideoIds(limit);
            var filtered = candidates.stream()
                .filter(videoId -> storageClient.fileExists(videoId + "/chunks/output.m3u8"))
                .toList();
            ctx.status(HttpStatus.OK).json(filtered);
        });

        return app;
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

    private static boolean isValidSegmentId(String segmentId) {
        if (segmentId == null || segmentId.isBlank()) {
            return false;
        }
        return segmentId.matches("^[A-Za-z0-9_-]+(\\.ts)?$");
    }

    private static String resolveInstanceId() {
        String id = System.getenv("CONTAINER_ID");
        if (id == null || id.isBlank()) {
            id = System.getenv("HOSTNAME");
        }
        if (id == null || id.isBlank()) {
            id = "local";
        }
        return id;
    }

    private static String rewriteManifest(String content) {
        // Prefix segment lines so they resolve to /stream/{videoId}/segment/{segmentId}
        String[] lines = content.split("\\r?\\n");
        StringBuilder rewritten = new StringBuilder(content.length() + lines.length * 8);
        for (String line : lines) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                rewritten.append("segment/").append(line);
            } else {
                rewritten.append(line);
            }
            rewritten.append('\n');
        }
        return rewritten.toString();
    }
}
