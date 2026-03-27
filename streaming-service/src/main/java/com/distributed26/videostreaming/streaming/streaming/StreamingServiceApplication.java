package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import com.distributed26.videostreaming.streaming.service.PlaylistService;
import com.distributed26.videostreaming.streaming.service.StreamingReadinessService;
import com.distributed26.videostreaming.streaming.service.StreamingServiceConfig;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class StreamingServiceApplication {
    private static final Logger LOGGER = LogManager.getLogger(StreamingServiceApplication.class);

    public static void main(String[] args) {
        StreamingServiceConfig config = StreamingServiceConfig.fromEnv();
        Javalin app = createStreamingApp(config);
        LOGGER.info("Starting streaming service on port {}", config.port());
        app.start(config.port());
    }

    static Javalin createStreamingApp() {
        return createStreamingApp(StreamingServiceConfig.fromEnv());
    }

    static Javalin createStreamingApp(StreamingServiceConfig config) {
        StorageConfig storageConfig = config.storageConfig();
        if (storageConfig.getPublicEndpointUrl().equals(storageConfig.getEndpointUrl())) {
            LOGGER.warn("MINIO_PUBLIC_ENDPOINT is not set; presigned URLs will use internal endpoint '{}'.",
                    storageConfig.getEndpointUrl());
        } else {
            LOGGER.info("Presigned URLs will use public endpoint: {}", storageConfig.getPublicEndpointUrl());
        }
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        return createStreamingApp(storageClient, createVideoStatusRepository());
    }

    static Javalin createStreamingApp(ObjectStorageClient storageClient, VideoStatusRepository videoStatusRepository) {
        StreamingReadinessService readinessService = new StreamingReadinessService(videoStatusRepository, storageClient);
        PlaylistService playlistService = new PlaylistService(storageClient);

        Javalin app = Javalin.create(config -> config.http.prefer405over404 = true);
        app.events(event -> event.serverStopped(() -> closeStorageClient(storageClient)));

        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type,Range");
        });
        app.options("/*", ctx -> ctx.status(204));

        app.get("/stream/{videoId}/manifest", ctx -> {
            if (!readinessService.validateVideoId(ctx) || !readinessService.requireCompleted(ctx)) {
                return;
            }
            try {
                String manifest = playlistService.loadMasterManifest(ctx.pathParam("videoId"));
                ctx.status(HttpStatus.OK)
                        .header("Cache-Control", "no-store, max-age=0")
                        .contentType("application/vnd.apple.mpegurl")
                        .result(manifest);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Manifest not found");
            } catch (IOException e) {
                LOGGER.error("Failed to read master manifest for videoId={}", ctx.pathParam("videoId"), e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read manifest");
            }
        });

        app.get("/stream/{videoId}/variant/{profile}/playlist.m3u8", ctx -> {
            if (!readinessService.validateVideoId(ctx)
                    || !readinessService.validateProfile(ctx, ctx.pathParam("profile"))
                    || !readinessService.requireCompleted(ctx)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String profile = ctx.pathParam("profile");
            try {
                String playlist = playlistService.loadVariantManifest(videoId, profile);
                long segmentCount = playlist.lines().filter(line -> line.startsWith("/stream/")).count();
                LOGGER.info("Generated variant playlist for videoId={} profile={} with {} proxy segment URLs",
                        videoId, profile, segmentCount);
                ctx.status(HttpStatus.OK)
                        .header("Cache-Control", "no-store, max-age=0")
                        .contentType("application/vnd.apple.mpegurl")
                        .result(playlist);
            } catch (NoSuchKeyException e) {
                ctx.status(HttpStatus.NOT_FOUND).result("Variant manifest not found");
            } catch (IOException e) {
                LOGGER.error("Failed to read variant manifest for videoId={} profile={}", videoId, profile, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to read variant manifest");
            }
        });

        app.get("/stream/{videoId}/segment/{profile}/{segment}", ctx -> {
            if (!readinessService.validateVideoId(ctx)
                    || !readinessService.validateProfile(ctx, ctx.pathParam("profile"))) {
                return;
            }
            String segment = ctx.pathParam("segment");
            if (segment == null || !segment.matches("^[A-Za-z0-9_.-]+\\.ts$")) {
                ctx.status(HttpStatus.BAD_REQUEST).result("Invalid segment name");
                return;
            }
            String videoId = ctx.pathParam("videoId");
            String profile = ctx.pathParam("profile");
            try {
                String presignedUrl = playlistService.generateSegmentUrl(videoId, profile, segment);
                ctx.redirect(presignedUrl, HttpStatus.FOUND);
            } catch (Exception e) {
                LOGGER.error("Failed to generate segment URL for videoId={} profile={} segment={}",
                        videoId, profile, segment, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to generate segment URL");
            }
        });

        app.get("/stream/ready", ctx -> {
            if (videoStatusRepository == null) {
                ctx.status(500).result("Streaming status checks are not configured");
                return;
            }
            ctx.status(HttpStatus.OK).json(readinessService.readyVideos(parseReadyLimit(ctx.queryParam("limit"))));
        });

        return app;
    }

    private static void closeStorageClient(ObjectStorageClient storageClient) {
        try {
            storageClient.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing storage client on shutdown", e);
        }
    }

    private static int parseReadyLimit(String limitParam) {
        if (limitParam == null || limitParam.isBlank()) {
            return 50;
        }
        try {
            return Math.max(1, Integer.parseInt(limitParam.trim()));
        } catch (NumberFormatException e) {
            return 50;
        }
    }

    private static VideoStatusRepository createVideoStatusRepository() {
        try {
            return VideoStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; streaming status checks disabled: {}", e.getMessage());
            return null;
        }
    }
}
