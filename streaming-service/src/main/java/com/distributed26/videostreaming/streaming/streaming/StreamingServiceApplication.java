package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import com.distributed26.videostreaming.streaming.service.PlaylistService;
import com.distributed26.videostreaming.streaming.service.StreamingReadinessService;
import com.distributed26.videostreaming.streaming.service.StreamingServiceConfig;
import com.distributed26.videostreaming.streaming.service.VideoDeletionRetryWorker;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class StreamingServiceApplication {
    private static final Logger LOGGER = LogManager.getLogger(StreamingServiceApplication.class);
    private static final String DEV_LOG_SERVICE = "Streaming-service";
    private static final int DEFAULT_DELETE_RETRY_INTERVAL_SECONDS = 120;

    public static void main(String[] args) {
        StreamingServiceConfig config = StreamingServiceConfig.fromEnv();
        RabbitMQDevLogPublisher devLogPublisher = createDevLogPublisher();
        Javalin app = createStreamingApp(config, devLogPublisher);
        publishDevLogInfo(devLogPublisher, "Streaming service started");
        LOGGER.info("Starting streaming service on port {}", config.port());
        app.start(config.port());
    }

    static Javalin createStreamingApp() {
        return createStreamingApp(StreamingServiceConfig.fromEnv(), null);
    }

    static Javalin createStreamingApp(StreamingServiceConfig config) {
        return createStreamingApp(config, null);
    }

    static Javalin createStreamingApp(StreamingServiceConfig config, RabbitMQDevLogPublisher devLogPublisher) {
        StorageConfig storageConfig = config.storageConfig();
        if (storageConfig.getPublicEndpointUrl().equals(storageConfig.getEndpointUrl())) {
            LOGGER.warn("MINIO_PUBLIC_ENDPOINT is not set; presigned URLs will use internal endpoint '{}'.",
                    storageConfig.getEndpointUrl());
            publishDevLogWarn(devLogPublisher, "MINIO_PUBLIC_ENDPOINT is not set; streaming URLs are using the internal endpoint");
        } else {
            LOGGER.info("Presigned URLs will use public endpoint: {}", storageConfig.getPublicEndpointUrl());
        }
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        return createStreamingApp(
                storageClient,
                createVideoStatusRepository(devLogPublisher),
                devLogPublisher,
                config.deleteRetryIntervalSeconds()
        );
    }

    static Javalin createStreamingApp(ObjectStorageClient storageClient, VideoStatusRepository videoStatusRepository) {
        return createStreamingApp(storageClient, videoStatusRepository, null, DEFAULT_DELETE_RETRY_INTERVAL_SECONDS);
    }

    static Javalin createStreamingApp(
            ObjectStorageClient storageClient,
            VideoStatusRepository videoStatusRepository,
            RabbitMQDevLogPublisher devLogPublisher
    ) {
        return createStreamingApp(
                storageClient,
                videoStatusRepository,
                devLogPublisher,
                DEFAULT_DELETE_RETRY_INTERVAL_SECONDS
        );
    }

    static Javalin createStreamingApp(
            ObjectStorageClient storageClient,
            VideoStatusRepository videoStatusRepository,
            RabbitMQDevLogPublisher devLogPublisher,
            int deleteRetryIntervalSeconds
    ) {
        StreamingReadinessService readinessService = new StreamingReadinessService(videoStatusRepository, storageClient);
        PlaylistService playlistService = new PlaylistService(storageClient);
        ScheduledExecutorService deletionRetryExecutor = startDeletionRetryWorker(
                videoStatusRepository,
                readinessService,
                playlistService,
                deleteRetryIntervalSeconds
        );

        Javalin app = Javalin.create(config -> config.http.prefer405over404 = true);
        app.events(event -> event.serverStopped(() -> {
            deletionRetryExecutor.shutdownNow();
            closeStorageClient(storageClient);
            closeDevLogPublisher(devLogPublisher);
        }));

        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,DELETE,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type,Range");
        });
        app.options("/*", ctx -> ctx.status(204));

        app.get("/health", ctx -> ctx.json(java.util.Map.of("status", "ok")));

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

        app.delete("/stream/{videoId}", ctx -> {
            if (!readinessService.validateVideoId(ctx)) {
                return;
            }
            String videoId = ctx.pathParam("videoId");
            try {
                boolean deleted = readinessService.deleteVideo(videoId);
                if (!deleted) {
                    ctx.status(HttpStatus.NOT_FOUND).result("Video not found");
                    return;
                }
                playlistService.invalidateVideo(videoId);
                ctx.status(HttpStatus.OK).json(new DeleteVideosResponse(List.of(videoId)));
            } catch (Exception e) {
                LOGGER.error("Failed to delete videoId={}", videoId, e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to delete video");
            }
        });

        app.delete("/stream", ctx -> {
            DeleteVideosRequest request;
            try {
                request = ctx.bodyAsClass(DeleteVideosRequest.class);
            } catch (Exception e) {
                ctx.status(HttpStatus.BAD_REQUEST).result("Invalid delete request");
                return;
            }
            if (request == null || request.videoIds() == null || request.videoIds().isEmpty()) {
                ctx.status(HttpStatus.BAD_REQUEST).result("No video IDs provided");
                return;
            }

            for (String videoId : request.videoIds()) {
                try {
                    java.util.UUID.fromString(videoId);
                } catch (IllegalArgumentException e) {
                    ctx.status(HttpStatus.BAD_REQUEST).result("Invalid video ID");
                    return;
                }
            }

            for (String videoId : request.videoIds()) {
                if (videoStatusRepository.findStatusByVideoId(videoId).isEmpty()) {
                    ctx.status(HttpStatus.NOT_FOUND).result("Video not found");
                    return;
                }
            }

            try {
                for (String videoId : request.videoIds()) {
                    readinessService.deleteVideo(videoId);
                    playlistService.invalidateVideo(videoId);
                }
                ctx.status(HttpStatus.OK).json(new DeleteVideosResponse(request.videoIds()));
            } catch (Exception e) {
                LOGGER.error("Failed to delete videoIds={}", request.videoIds(), e);
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).result("Failed to delete videos");
            }
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

    private static VideoStatusRepository createVideoStatusRepository(RabbitMQDevLogPublisher devLogPublisher) {
        try {
            return VideoStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; streaming status checks disabled: {}", e.getMessage());
            publishDevLogWarn(devLogPublisher, "Streaming status checks are disabled because Postgres is not configured");
            return null;
        }
    }

    private static RabbitMQDevLogPublisher createDevLogPublisher() {
        try {
            return RabbitMQDevLogPublisher.fromEnv();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to initialize dev log publisher", e);
            return null;
        }
    }

    private static void publishDevLogInfo(RabbitMQDevLogPublisher publisher, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishInfo(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish streaming dev log info message={}", message, e);
        }
    }

    private static void publishDevLogWarn(RabbitMQDevLogPublisher publisher, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishWarn(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish streaming dev log warning message={}", message, e);
        }
    }

    private static void closeDevLogPublisher(RabbitMQDevLogPublisher publisher) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing dev log publisher", e);
        }
    }

    private static ScheduledExecutorService startDeletionRetryWorker(
            VideoStatusRepository videoStatusRepository,
            StreamingReadinessService readinessService,
            PlaylistService playlistService,
            int intervalSeconds
    ) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "stream-delete-retry");
            thread.setDaemon(true);
            return thread;
        });
        if (videoStatusRepository == null) {
            return executor;
        }
        VideoDeletionRetryWorker worker = new VideoDeletionRetryWorker(
                videoStatusRepository,
                readinessService,
                playlistService
        );
        executor.scheduleWithFixedDelay(worker, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        return executor;
    }

    private record DeleteVideosRequest(List<String> videoIds) {
    }

    private record DeleteVideosResponse(List<String> deletedVideoIds) {
    }
}
