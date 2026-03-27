package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher;
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
    private static final String DEV_LOG_SERVICE = "Streaming-service";

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
        return createStreamingApp(storageClient, createVideoStatusRepository(devLogPublisher), devLogPublisher);
    }

    static Javalin createStreamingApp(ObjectStorageClient storageClient, VideoStatusRepository videoStatusRepository) {
        return createStreamingApp(storageClient, videoStatusRepository, null);
    }

    static Javalin createStreamingApp(
            ObjectStorageClient storageClient,
            VideoStatusRepository videoStatusRepository,
            RabbitMQDevLogPublisher devLogPublisher
    ) {
        StreamingReadinessService readinessService = new StreamingReadinessService(videoStatusRepository, storageClient);
        PlaylistService playlistService = new PlaylistService(storageClient);

        Javalin app = Javalin.create(config -> config.http.prefer405over404 = true);
        app.events(event -> event.serverStopped(() -> {
            closeStorageClient(storageClient);
            closeDevLogPublisher(devLogPublisher);
        }));

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
                long segmentCount = playlist.lines().filter(line -> line.startsWith("http")).count();
                LOGGER.info("Generated variant playlist for videoId={} profile={} with {} presigned segment URLs",
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
}
