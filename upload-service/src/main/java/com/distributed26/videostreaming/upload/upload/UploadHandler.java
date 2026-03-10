package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.upload.processing.SegmentUploadCoordinator;
import com.distributed26.videostreaming.upload.processing.UploadInitializationService;
import com.distributed26.videostreaming.upload.processing.UploadProcessingConfig;
import com.distributed26.videostreaming.upload.processing.UploadRequest;
import com.distributed26.videostreaming.upload.processing.UploadRequestParser;
import com.distributed26.videostreaming.upload.processing.VideoSegmentationWorkflow;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadHandler implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(UploadHandler.class);

    private final UploadRequestParser requestParser;
    private final UploadInitializationService initializationService;
    private final VideoSegmentationWorkflow workflow;
    private final ExecutorService supervisionExecutor;
    private final ExecutorService ffmpegExecutor;
    private final ExecutorService segmentUploadExecutor;

    public UploadHandler(ObjectStorageClient storageClient, StatusEventBus statusEventBus, TranscodeTaskBus transcodeTaskBus) {
        this(storageClient, statusEventBus, transcodeTaskBus, null, null, null, null);
    }

    public UploadHandler(
            ObjectStorageClient storageClient,
            StatusEventBus statusEventBus,
            TranscodeTaskBus transcodeTaskBus,
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            String machineId,
            String containerId
    ) {
        this(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                videoUploadRepository,
                segmentUploadRepository,
                machineId,
                containerId,
                UploadProcessingConfig.fromDotenv(Dotenv.configure().directory("./").ignoreIfMissing().load())
        );
    }

    UploadHandler(
            ObjectStorageClient storageClient,
            StatusEventBus statusEventBus,
            TranscodeTaskBus transcodeTaskBus,
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            String machineId,
            String containerId,
            UploadProcessingConfig config
    ) {
        Objects.requireNonNull(storageClient, "storageClient is null");
        Objects.requireNonNull(statusEventBus, "statusEventBus is null");
        Objects.requireNonNull(transcodeTaskBus, "transcodeTaskBus is null");
        Objects.requireNonNull(config, "config is null");

        this.supervisionExecutor = Executors.newCachedThreadPool();
        this.ffmpegExecutor = Executors.newFixedThreadPool(config.ffmpegPoolSize());
        this.segmentUploadExecutor = Executors.newFixedThreadPool(config.uploadPoolSize());

        logger.info("CHUNK_DURATION_SECONDS resolved to {}", config.segmentDuration());
        logger.info("Initialized FFmpeg executor with pool size: {}", config.ffmpegPoolSize());
        logger.info("Upload-side segmentation mode: stream-copy only");
        logger.info("Initialized segment upload executor with pool size: {}", config.uploadPoolSize());
        logger.info("Initialized max in-flight segment uploads: {}", config.maxInFlightSegmentUploads());

        this.requestParser = new UploadRequestParser(config.maxVideoNameLength());
        this.initializationService = new UploadInitializationService(
                storageClient,
                videoUploadRepository,
                machineId,
                containerId,
                config.segmentDuration()
        );
        SegmentUploadCoordinator uploadCoordinator = new SegmentUploadCoordinator(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                segmentUploadRepository,
                this.segmentUploadExecutor,
                config.maxInFlightSegmentUploads(),
                config.segmentDuration()
        );
        this.workflow = new VideoSegmentationWorkflow(
                initializationService,
                uploadCoordinator,
                videoUploadRepository,
                statusEventBus,
                this.ffmpegExecutor,
                config.processingTimeoutMillis(),
                config.pollingIntervalMillis(),
                machineId,
                containerId
        );
    }

    public void upload(Context ctx) {
        long startTime = System.currentTimeMillis();
        UploadedFile uploadedFile = ctx.uploadedFile("file");
        if (uploadedFile == null) {
            ctx.status(400).result("No 'file' part found in request");
            return;
        }

        logger.info("Receiving upload for file: {}", uploadedFile.filename());

        UploadRequest request = requestParser.parse(ctx);
        if (request == null) {
            ctx.status(400).result("Missing or empty 'name' field");
            return;
        }

        Path inputPath;
        try {
            inputPath = saveUploadedFile(uploadedFile, request.videoId());
        } catch (IOException e) {
            logger.error("Failed to save uploaded file", e);
            ctx.status(500).result("Failed to save uploaded file");
            return;
        }

        try {
            initializationService.initialize(request, inputPath);
        } catch (RuntimeException e) {
            logger.error("Failed to initialize upload for videoId={}", request.videoId(), e);
            try {
                Files.deleteIfExists(inputPath);
            } catch (IOException ignored) {
            }
            ctx.status(500).result("Failed to store video metadata");
            return;
        }

        CompletableFuture.runAsync(
                () -> workflow.processVideo(request.videoId(), inputPath, startTime),
                supervisionExecutor
        );

        ctx.status(202).json(new UploadResponse(request.videoId(), request.uploadStatusUrl()));
    }

    private Path saveUploadedFile(UploadedFile uploadedFile, String videoId) throws IOException {
        Path inputPath = Files.createTempFile("upload-" + videoId, ".tmp");
        try (InputStream is = uploadedFile.content()) {
            Files.copy(is, inputPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return inputPath;
    }

    private record UploadResponse(String videoId, String uploadStatusUrl) {
    }

    @Override
    public void close() {
        shutdownExecutor("upload supervision", supervisionExecutor);
        shutdownExecutor("upload ffmpeg", ffmpegExecutor);
        shutdownExecutor("segment upload", segmentUploadExecutor);
    }

    private void shutdownExecutor(String name, ExecutorService executor) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("{} executor did not terminate cleanly within timeout", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while shutting down {} executor", name, e);
        }
    }
}
