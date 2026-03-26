package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.upload.processing.SegmentUploadCoordinator;
import com.distributed26.videostreaming.upload.processing.UploadInitializationService;
import com.distributed26.videostreaming.upload.processing.UploadProcessingConfig;
import com.distributed26.videostreaming.upload.processing.UploadRequest;
import com.distributed26.videostreaming.upload.processing.UploadRequestParser;
import com.distributed26.videostreaming.upload.processing.VideoSegmentationWorkflow;
import com.distributed26.videostreaming.upload.processing.StorageRetryExecutor;
import com.distributed26.videostreaming.upload.processing.StorageStateTracker;
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
    private final FailedVideoRegistry failedVideoRegistry;
    private final StorageStateTracker storageStateTracker;

    public UploadHandler(ObjectStorageClient storageClient, StatusEventBus statusEventBus, TranscodeTaskBus transcodeTaskBus) {
        this(storageClient, statusEventBus, transcodeTaskBus, null, null, null, null, new FailedVideoRegistry(), null);
    }

    public UploadHandler(
            ObjectStorageClient storageClient,
            StatusEventBus statusEventBus,
            TranscodeTaskBus transcodeTaskBus,
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            String machineId,
            String containerId,
            FailedVideoRegistry failedVideoRegistry,
            StorageStateTracker storageStateTracker
    ) {
        this(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                videoUploadRepository,
                segmentUploadRepository,
                machineId,
                containerId,
                failedVideoRegistry,
                storageStateTracker,
                UploadProcessingConfig.fromDotenv(Dotenv.configure().directory("./").ignoreIfMissing().load())
        );
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
                new FailedVideoRegistry(),
                null
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
            FailedVideoRegistry failedVideoRegistry,
            StorageStateTracker storageStateTracker,
            UploadProcessingConfig config
    ) {
        Objects.requireNonNull(storageClient, "storageClient is null");
        Objects.requireNonNull(statusEventBus, "statusEventBus is null");
        Objects.requireNonNull(transcodeTaskBus, "transcodeTaskBus is null");
        Objects.requireNonNull(config, "config is null");
        Objects.requireNonNull(failedVideoRegistry, "failedVideoRegistry is null");

        this.supervisionExecutor = Executors.newCachedThreadPool();
        this.ffmpegExecutor = Executors.newFixedThreadPool(config.ffmpegPoolSize());
        this.segmentUploadExecutor = Executors.newFixedThreadPool(config.uploadPoolSize());
        this.failedVideoRegistry = failedVideoRegistry;
        this.storageStateTracker = storageStateTracker != null
                ? storageStateTracker
                : new StorageStateTracker(videoUploadRepository, statusEventBus);

        logger.info("CHUNK_DURATION_SECONDS resolved to {}", config.segmentDuration());
        logger.info("Initialized FFmpeg executor with pool size: {}", config.ffmpegPoolSize());
        logger.info("Upload-side segmentation mode: stream-copy only");
        logger.info("Initialized segment upload executor with pool size: {}", config.uploadPoolSize());
        logger.info("Initialized max in-flight segment uploads: {}", config.maxInFlightSegmentUploads());

        this.requestParser = new UploadRequestParser(config.maxVideoNameLength());
        StorageRetryExecutor storageRetryExecutor = new StorageRetryExecutor(
                config.storageRetryInitialDelayMillis(),
                config.storageRetryMaxDelayMillis()
        );
        this.initializationService = new UploadInitializationService(
                storageClient,
                videoUploadRepository,
                storageRetryExecutor,
                this.storageStateTracker,
                machineId,
                containerId,
                config.segmentDuration()
        );
        SegmentUploadCoordinator uploadCoordinator = new SegmentUploadCoordinator(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                segmentUploadRepository,
                failedVideoRegistry,
                storageRetryExecutor,
                this.storageStateTracker,
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
                containerId,
                failedVideoRegistry,
                this.storageStateTracker
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
            initializationService.initializeUploadRecord(request, inputPath);
        } catch (RuntimeException e) {
            logger.error("Failed to initialize upload for videoId={}", request.videoId(), e);
            try {
                Files.deleteIfExists(inputPath);
            } catch (IOException ignored) {
            }
            ctx.status(500).result("Failed to initialize upload");
            return;
        }

        failedVideoRegistry.clear(request.videoId());
        String initialStatus = storageStateTracker.isServiceReady() ? "PROCESSING" : "WAITING_FOR_STORAGE";

        CompletableFuture.runAsync(
                () -> workflow.processVideo(request, inputPath, startTime),
                supervisionExecutor
        );

        ctx.status(202).json(new UploadResponse(
                request.videoId(),
                request.uploadStatusUrl(),
                initialStatus,
                UploadStatusPresenter.isRetryingMinioConnection(initialStatus),
                UploadStatusPresenter.statusMessage(initialStatus)
        ));
    }

    private Path saveUploadedFile(UploadedFile uploadedFile, String videoId) throws IOException {
        Path inputPath = Files.createTempFile("upload-" + videoId, ".tmp");
        try (InputStream is = uploadedFile.content()) {
            Files.copy(is, inputPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return inputPath;
    }

    private record UploadResponse(
            String videoId,
            String uploadStatusUrl,
            String status,
            boolean retryingMinioConnection,
            String statusMessage
    ) {
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
