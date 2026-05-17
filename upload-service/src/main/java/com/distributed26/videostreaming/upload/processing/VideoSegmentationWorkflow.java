package com.distributed26.videostreaming.upload.processing;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
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
import net.bramp.ffmpeg.builder.FFmpegOutputBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class VideoSegmentationWorkflow {
    private static final Logger logger = LogManager.getLogger(VideoSegmentationWorkflow.class);
    // Default to the fast/risky mode (stream copy) for better upload latency.
    // Override with UPLOAD_SEGMENTATION_MODE=auto or reencode when safety is required.
    private static final String SEGMENTATION_MODE = env("UPLOAD_SEGMENTATION_MODE", "copy").toLowerCase(Locale.ROOT);
    // When re-encoding is needed, prefer an ultra-fast preset by default to reduce CPU/time.
    private static final String SEGMENTATION_PRESET = env("UPLOAD_SEGMENTATION_PRESET", "ultrafast");
    private static final String SEGMENTATION_CRF = env("UPLOAD_SEGMENTATION_CRF", "28");
    private static final double MAX_KEYFRAME_GAP_FACTOR = envDouble("UPLOAD_SEGMENTATION_MAX_KEYFRAME_GAP_FACTOR", 1.25d);

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
                executor.createJob(buildSegmentationJob(request, inputPath, tempOutputFinal)).run();
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
                    inputPath,
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
                    inputPath,
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

    private FFmpegBuilder buildSegmentationJob(UploadRequest request, Path inputPath, Path tempOutput) {
        // Re-encode segments to ensure each segment contains SPS/PPS and starts with a keyframe.
        // This prevents segments that start mid-GOP (missing parameter-sets) from being undecodable by FFmpeg.
        int segDuration = initializationService.getSegmentDuration();
        int estimatedFps = 30;
        int keyint = Math.max(1, segDuration * estimatedFps);

        SegmentationStrategy strategy = resolveSegmentationStrategy(request, inputPath, segDuration);

        FFmpegOutputBuilder builder = new FFmpegBuilder()
                .setInput(inputPath.toString())
                .addOutput(tempOutput.resolve("output.m3u8").toString())
                .setFormat("hls")
                .addExtraArgs("-start_number", "0")
                .addExtraArgs("-hls_time", String.valueOf(segDuration))
                .addExtraArgs("-hls_list_size", "0")
                .addExtraArgs("-hls_flags", "independent_segments");

        if (strategy == SegmentationStrategy.COPY) {
            logger.info("Segmenting with stream copy for input={} (mode={}, safe keyframe cadence detected)",
                    inputPath, SEGMENTATION_MODE);
            builder
                    .addExtraArgs("-c:v", "copy")
                    .addExtraArgs("-c:a", "copy");
        } else {
            logger.info("Segmenting with re-encode for input={} (mode={}, preset={}, crf={})",
                    inputPath, SEGMENTATION_MODE, SEGMENTATION_PRESET, SEGMENTATION_CRF);
            builder
                    // Re-encode video so every segment is self-contained
                    .addExtraArgs("-c:v", "libx264")
                    .addExtraArgs("-preset", SEGMENTATION_PRESET)
                    .addExtraArgs("-crf", SEGMENTATION_CRF)
                    .addExtraArgs("-x264-params", "keyint=" + keyint + ":scenecut=0")
                    .addExtraArgs("-force_key_frames", "expr:gte(t,n_forced*" + segDuration + ")")
                    // Re-encode audio to aac for compatibility
                    .addExtraArgs("-c:a", "aac");
        }

        return builder.done();
    }

    private SegmentationStrategy resolveSegmentationStrategy(UploadRequest request, Path inputPath, int segDuration) {
        // Check per-request override first (e.g., segmentationMode=reencode)
        if (request != null && request.segmentationMode() != null) {
            String override = request.segmentationMode().trim().toLowerCase(Locale.ROOT);
            if ("reencode".equals(override) || "re-encode".equals(override) || "safe".equals(override)) {
                logger.info("Segmentation override requested: re-encode for videoId={}", request.videoId());
                return SegmentationStrategy.REENCODE;
            }
            if ("copy".equals(override) || "fast".equals(override) || "risky".equals(override)) {
                logger.info("Segmentation override requested: copy for videoId={}", request.videoId());
                return SegmentationStrategy.COPY;
            }
        }

        if ("copy".equals(SEGMENTATION_MODE)) {
            return SegmentationStrategy.COPY;
        }
        if ("reencode".equals(SEGMENTATION_MODE) || "re-encode".equals(SEGMENTATION_MODE)) {
            return SegmentationStrategy.REENCODE;
        }

        // Auto mode: only copy when video is H.264 and keyframe cadence is frequent enough.
        try {
            String codec = probePrimaryVideoCodec(inputPath);
            if (!"h264".equalsIgnoreCase(codec)) {
                logger.info("Auto segmentation selected re-encode: unsupported copy codec='{}' input={}", codec, inputPath);
                return SegmentationStrategy.REENCODE;
            }

            List<Double> keyframeTimes = probeKeyframeTimesSeconds(inputPath);
            if (keyframeTimes.size() < 2) {
                logger.info("Auto segmentation selected re-encode: insufficient keyframe data ({}) input={}",
                        keyframeTimes.size(), inputPath);
                return SegmentationStrategy.REENCODE;
            }

            double maxAllowedGap = segDuration * Math.max(1.0d, MAX_KEYFRAME_GAP_FACTOR);
            double maxObservedGap = 0d;
            for (int i = 1; i < keyframeTimes.size(); i++) {
                maxObservedGap = Math.max(maxObservedGap, keyframeTimes.get(i) - keyframeTimes.get(i - 1));
            }

            if (maxObservedGap > maxAllowedGap) {
                logger.info("Auto segmentation selected re-encode: keyframe gap too large maxGap={}s allowed={}s input={}",
                        String.format(Locale.US, "%.3f", maxObservedGap),
                        String.format(Locale.US, "%.3f", maxAllowedGap),
                        inputPath);
                return SegmentationStrategy.REENCODE;
            }

            return SegmentationStrategy.COPY;
        } catch (Exception e) {
            logger.warn("Auto segmentation probe failed for input={}, falling back to re-encode", inputPath, e);
            return SegmentationStrategy.REENCODE;
        }
    }

    private String probePrimaryVideoCodec(Path inputPath) throws IOException, InterruptedException {
        List<String> lines = runCommand(
                List.of(
                        "ffprobe",
                        "-v", "error",
                        "-select_streams", "v:0",
                        "-show_entries", "stream=codec_name",
                        "-of", "default=noprint_wrappers=1:nokey=1",
                        inputPath.toString()
                )
        );
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                return trimmed;
            }
        }
        throw new IOException("ffprobe returned no video codec for " + inputPath);
    }

    private List<Double> probeKeyframeTimesSeconds(Path inputPath) throws IOException, InterruptedException {
        List<String> lines = runCommand(
                List.of(
                        "ffprobe",
                        "-v", "error",
                        "-skip_frame", "nokey",
                        "-select_streams", "v:0",
                        "-show_entries", "frame=best_effort_timestamp_time",
                        "-of", "csv=p=0",
                        inputPath.toString()
                )
        );
        List<Double> timestamps = new ArrayList<>();
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                timestamps.add(Double.parseDouble(trimmed));
            } catch (NumberFormatException ignored) {
            }
        }
        return timestamps;
    }

    private List<String> runCommand(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).start();
        List<String> stdout = new ArrayList<>();
        List<String> stderr = new ArrayList<>();
        try (BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
             BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = out.readLine()) != null) {
                stdout.add(line);
            }
            while ((line = err.readLine()) != null) {
                stderr.add(line);
            }
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Command failed exit=" + exitCode + " cmd=" + String.join(" ", command)
                    + " stderr=" + String.join(" | ", stderr));
        }
        return stdout;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static double envDouble(String key, double defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private enum SegmentationStrategy {
        COPY,
        REENCODE
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
