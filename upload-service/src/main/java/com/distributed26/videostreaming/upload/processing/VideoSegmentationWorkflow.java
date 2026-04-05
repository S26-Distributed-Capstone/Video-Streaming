package com.distributed26.videostreaming.upload.processing;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class VideoSegmentationWorkflow {
    private static final Logger logger = LogManager.getLogger(VideoSegmentationWorkflow.class);

    private final UploadInitializationService initializationService;
    private final SegmentUploadCoordinator uploadCoordinator;
    private final VideoUploadRepository videoUploadRepository;
    private final StatusEventBus statusEventBus;
    private final ExecutorService ffmpegExecutor;
    private final long processingTimeoutMillis;
    private final long pollingIntervalMillis;
    private final String machineId;
    private final String containerId;
    private final FailedVideoRegistry failedVideoRegistry;
    private final StorageStateTracker storageStateTracker;

    public VideoSegmentationWorkflow(
            UploadInitializationService initializationService,
            SegmentUploadCoordinator uploadCoordinator,
            VideoUploadRepository videoUploadRepository,
            StatusEventBus statusEventBus,
            ExecutorService ffmpegExecutor,
            long processingTimeoutMillis,
            long pollingIntervalMillis,
            String machineId,
            String containerId,
            FailedVideoRegistry failedVideoRegistry,
            StorageStateTracker storageStateTracker
    ) {
        this.initializationService = initializationService;
        this.uploadCoordinator = uploadCoordinator;
        this.videoUploadRepository = videoUploadRepository;
        this.statusEventBus = statusEventBus;
        this.ffmpegExecutor = ffmpegExecutor;
        this.processingTimeoutMillis = processingTimeoutMillis;
        this.pollingIntervalMillis = pollingIntervalMillis;
        this.machineId = machineId;
        this.containerId = containerId;
        this.failedVideoRegistry = failedVideoRegistry;
        this.storageStateTracker = storageStateTracker;
    }

    public void processVideo(UploadRequest request, Path inputPath, long startTime) {
        String videoId = request.videoId();
        Path tempOutput = null;
        Map<Path, SegmentUploadCoordinator.PendingUpload> inFlightUploads = new ConcurrentHashMap<>();
        boolean cancelInFlightUploads = false;
        CompletableFuture<Void> ffmpegFuture = null;

        try {
            ensureVideoActive(videoId);
            initializationService.ensureVideoMetadataStored(request.videoId(), request.videoName());
            tempOutput = Files.createTempDirectory("hls-" + videoId);
            Path tempOutputFinal = tempOutput;

            FFmpeg ffmpeg = new FFmpeg("ffmpeg");
            FFprobe ffprobe = new FFprobe("ffprobe");

            logger.info("Starting FFmpeg segmentation for video: {}", videoId);

            ffmpegFuture = CompletableFuture.runAsync(() -> {
                FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);
                executor.createJob(buildSegmentationJob(inputPath, tempOutputFinal)).run();
            }, ffmpegExecutor);

            Set<Path> uploadedFiles = ConcurrentHashMap.newKeySet();
            Set<Integer> uploadedSegmentNumbers = ConcurrentHashMap.newKeySet();
            uploadCoordinator.preloadUploadedSegmentNumbers(videoId, uploadedSegmentNumbers);

            logger.info("Starting segment monitoring loop for video: {}", videoId);

            long loopStartTime = System.currentTimeMillis();
            long lastSegmentTime = System.currentTimeMillis();
            long currentPollingInterval = pollingIntervalMillis;
            long storageWaitAccumulatedMillis = 0L;
            long storageWaitStartedAt = -1L;

            while (!ffmpegFuture.isDone()) {
                ensureVideoActive(videoId);
                long now = System.currentTimeMillis();
                boolean waitingOnStorage = storageStateTracker.isVideoWaiting(videoId);
                if (waitingOnStorage) {
                    if (storageWaitStartedAt < 0L) {
                        storageWaitStartedAt = now;
                    }
                } else if (storageWaitStartedAt >= 0L) {
                    storageWaitAccumulatedMillis += now - storageWaitStartedAt;
                    storageWaitStartedAt = -1L;
                }

                if (now - loopStartTime - storageWaitAccumulatedMillis > processingTimeoutMillis) {
                    logger.error("Video processing timed out for video: {}", videoId);
                    ffmpegFuture.cancel(true);
                    throw new RuntimeException("Processing timed out after " + processingTimeoutMillis + "ms");
                }

                int uploadedCount = uploadCoordinator.uploadReadySegments(
                        tempOutput,
                        videoId,
                        uploadedFiles,
                        uploadedSegmentNumbers,
                        inFlightUploads,
                        false
                );

                if (uploadedCount > 0) {
                    lastSegmentTime = System.currentTimeMillis();
                    currentPollingInterval = pollingIntervalMillis;
                } else if (System.currentTimeMillis() - lastSegmentTime > 10000) {
                    currentPollingInterval = Math.min(5000, currentPollingInterval * 2);
                }

                try {
                    Thread.sleep(currentPollingInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ffmpegFuture.cancel(true);
                    throw new RuntimeException("Upload interrupted", e);
                }
            }

            try {
                ffmpegFuture.join();
            } catch (CompletionException e) {
                throw new RuntimeException("FFmpeg processing failed", e.getCause());
            }

            ensureVideoActive(videoId);
            long totalSegments = countSegments(tempOutput);
            if (videoUploadRepository != null && totalSegments > 0) {
                videoUploadRepository.updateTotalSegments(videoId, (int) totalSegments);
                statusEventBus.publish(new UploadMetaEvent(videoId, (int) totalSegments));
                logger.info("Successfully updated total segments to {} segments", totalSegments);
            }

            ensureVideoActive(videoId);
            uploadCoordinator.uploadReadySegments(
                    tempOutput,
                    videoId,
                    uploadedFiles,
                    uploadedSegmentNumbers,
                    inFlightUploads,
                    true
            );

            long duration = System.currentTimeMillis() - startTime;
            logger.info(
                    "Successfully segmented and uploaded video: {}. Uploaded {} chunks (including playlist). Total time: {} ms",
                    videoId,
                    uploadedFiles.size(),
                    duration
            );
            if (videoUploadRepository != null) {
                videoUploadRepository.updateStatus(videoId, "UPLOADED");
            }
        } catch (java.util.concurrent.CancellationException e) {
            cancelInFlightUploads = true;
            if (ffmpegFuture != null) {
                ffmpegFuture.cancel(true);
            }
            if (failedVideoRegistry != null && failedVideoRegistry.isFailed(videoId)) {
                logger.info("Upload processing cancelled after terminal failure videoId={}", videoId);
                initializationService.deleteVideoMetadata(videoId);
            } else {
                logger.info("Upload processing cancelled videoId={}", videoId, e);
            }
        } catch (Exception e) {
            cancelInFlightUploads = true;
            if (ffmpegFuture != null) {
                ffmpegFuture.cancel(true);
            }
            logger.error("Upload/Processing failed for video: {}", videoId, e);
            logger.warn("Recording failure for videoId={} machineId={} containerId={} reason={}",
                    videoId, machineId, containerId, e.getMessage());
            if (videoUploadRepository != null) {
                videoUploadRepository.updateStatus(videoId, "FAILED");
            }
            initializationService.deleteVideoMetadata(videoId);
        } finally {
            uploadCoordinator.waitForOrCancelInFlightUploads(inFlightUploads, cancelInFlightUploads);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            cleanup(inputPath, tempOutput);
        }
    }

    private void ensureVideoActive(String videoId) {
        if (failedVideoRegistry != null && failedVideoRegistry.isFailed(videoId)) {
            throw new java.util.concurrent.CancellationException("Video already marked FAILED for videoId=" + videoId);
        }
    }

    private long countSegments(Path tempOutput) throws IOException {
        try (Stream<Path> stream = Files.list(tempOutput)) {
            return stream.filter(path -> path.getFileName().toString().endsWith(".ts")).count();
        }
    }

    private FFmpegBuilder buildSegmentationJob(Path inputPath, Path tempOutput) {
        return new FFmpegBuilder()
                .setInput(inputPath.toString())
                .addOutput(tempOutput.resolve("output.m3u8").toString())
                .setFormat("hls")
                .addExtraArgs("-start_number", "0")
                .addExtraArgs("-hls_time", String.valueOf(initializationService.getSegmentDuration()))
                .addExtraArgs("-hls_list_size", "0")
                .addExtraArgs("-c:v", "copy")
                .addExtraArgs("-c:a", "copy")
                .done();
    }

    private void cleanup(Path inputPath, Path tempOutput) {
        if (inputPath != null) {
            try {
                Files.deleteIfExists(inputPath);
            } catch (IOException ignored) {
            }
        }
        if (tempOutput != null) {
            try (Stream<Path> walk = Files.walk(tempOutput)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException ignored) {
            }
        }
    }
}
