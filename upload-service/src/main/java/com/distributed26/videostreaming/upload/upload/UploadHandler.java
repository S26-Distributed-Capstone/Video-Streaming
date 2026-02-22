package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.github.cdimascio.dotenv.Dotenv;

public class UploadHandler {
    private static final Logger logger = LogManager.getLogger(UploadHandler.class);
    private final ObjectStorageClient storageClient;
    private final JobTaskBus jobTaskBus;
    private final VideoUploadRepository videoUploadRepository;
    private final SegmentUploadRepository segmentUploadRepository;
    private final String machineId;
    private final String containerId;
    private final int segmentDuration;
    private final ExecutorService ffmpegExecutor;
    private final ExecutorService supervisionExecutor;
    private final long processingTimeoutMillis;
    private final long pollingIntervalMillis;

    public UploadHandler(ObjectStorageClient storageClient, JobTaskBus jobTaskBus) {
        this(storageClient, jobTaskBus, null, null, null, null);
    }

    public UploadHandler(
            ObjectStorageClient storageClient,
            JobTaskBus jobTaskBus,
            VideoUploadRepository videoUploadRepository,
            String machineId
    ) {
        this(storageClient, jobTaskBus, videoUploadRepository, null, machineId, null);
    }

    public UploadHandler(
            ObjectStorageClient storageClient,
            JobTaskBus jobTaskBus,
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            String machineId
    ) {
        this(storageClient, jobTaskBus, videoUploadRepository, segmentUploadRepository, machineId, null);
    }

    public UploadHandler(
            ObjectStorageClient storageClient,
            JobTaskBus jobTaskBus,
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            String machineId,
            String containerId
    ) {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        this.storageClient = storageClient;
        this.jobTaskBus = Objects.requireNonNull(jobTaskBus, "jobTaskBus is null");
        this.videoUploadRepository = videoUploadRepository;
        this.segmentUploadRepository = segmentUploadRepository;
        this.machineId = machineId;
        this.containerId = containerId;

        this.segmentDuration = dotenv.get("CHUNK_DURATION_SECONDS") == null ? 10 : Integer.parseInt(dotenv.get("CHUNK_DURATION_SECONDS"));
        logger.info("CHUNK_DURATION_SECONDS resolved to {}", this.segmentDuration);

        String timeoutEnv = dotenv.get("PROCESSING_TIMEOUT_SECONDS");
        this.processingTimeoutMillis = timeoutEnv != null && !timeoutEnv.isEmpty()
            ? Long.parseLong(timeoutEnv) * 1000
            : 3600 * 1000; // Default 1 hour

        String pollingEnv = dotenv.get("POLLING_INTERVAL_MILLIS");
        this.pollingIntervalMillis = pollingEnv != null && !pollingEnv.isEmpty()
            ? Long.parseLong(pollingEnv)
            : 1000; // Default 1 second

        // Executor for the lightweight supervision tasks (monitoring loop)
        // CachedThreadPool is suitable here as these tasks spend most time sleeping
        this.supervisionExecutor = Executors.newCachedThreadPool();

        // Use a bounded thread pool to control concurrent FFmpeg processes
        int poolSize = dotenv.get("FFMPEG_THREAD_POOL_SIZE") != null && !dotenv.get("FFMPEG_THREAD_POOL_SIZE").isEmpty()
                ? Integer.parseInt(dotenv.get("FFMPEG_THREAD_POOL_SIZE"))
                : Runtime.getRuntime().availableProcessors();

        this.ffmpegExecutor = Executors.newFixedThreadPool(poolSize);
        logger.info("Initialized FFmpeg executor with pool size: {}", poolSize);
    }

    public void upload(Context ctx) {
        long startTime = System.currentTimeMillis();
        UploadedFile uploadedFile = ctx.uploadedFile("file");
        if (uploadedFile == null) {
            ctx.status(400).result("No 'file' part found in request");
            return;
        }

        String filename = uploadedFile.filename();
        logger.info("Receiving upload for file: {}", filename);

        final String videoId = resolveVideoId(ctx);
        if (videoUploadRepository != null) {
            int totalSegments = 0;
            try {
                totalSegments = videoUploadRepository.findByVideoId(videoId)
                    .map(r -> r.getTotalSegments())
                    .orElse(0);
            } catch (Exception e) {
                logger.warn("Failed to load existing upload record for videoId={}", videoId, e);
            }
            videoUploadRepository.create(videoId, totalSegments, "PROCESSING", machineId, containerId);
        }

        // We need to copy the uploaded file to a safe location because Javalin cleans up
        // the uploaded file once the request handler returns.
        Path inputPath;
        long saveStart = System.currentTimeMillis();
        try {
            inputPath = Files.createTempFile("upload-" + videoId, ".tmp");
            try (InputStream is = uploadedFile.content()) {
                Files.copy(is, inputPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            logger.error("Failed to save uploaded file", e);
            ctx.status(500).result("Failed to save uploaded file");
            return;
        }
        logger.info("Saved upload videoId={} bytes={} in {} ms",
            videoId, inputPath.toFile().length(), System.currentTimeMillis() - saveStart);

        if (videoUploadRepository != null) {
            try {
                long estimateStart = System.currentTimeMillis();
                int estimatedSegments = estimateTotalSegments(inputPath);
                if (estimatedSegments > 0) {
                    videoUploadRepository.updateTotalSegments(videoId, estimatedSegments);
                }
                logger.info("Estimated total segments videoId={} totalSegments={} in {} ms",
                    videoId, estimatedSegments, System.currentTimeMillis() - estimateStart);
            } catch (Exception e) {
                logger.warn("Failed to estimate total segments for video: {}", videoId, e);
            }
        }

        // Run the supervision logic on the supervision executor
        CompletableFuture.runAsync(() -> {
            processVideo(videoId, inputPath, startTime);
        }, supervisionExecutor);

        String uploadStatusUrl = buildUploadStatusUrl(ctx, videoId);
        ctx.status(202).json(new UploadResponse(videoId, uploadStatusUrl));
    }

    private String buildUploadStatusUrl(Context ctx, String videoId) {
        String scheme = ctx.scheme();
        String wsScheme = "https".equalsIgnoreCase(scheme) ? "wss" : "ws";
        String statusHost = resolveStatusHost(ctx);
        return wsScheme + "://" + statusHost + "/upload-status?jobId=" + videoId;
    }

    private String resolveStatusHost(Context ctx) {
        String statusHost = System.getenv("STATUS_HOST");
        if (statusHost != null && !statusHost.isBlank()) {
            return statusHost.trim();
        }
        String statusPort = System.getenv("STATUS_PORT");
        if (statusPort != null && !statusPort.isBlank()) {
            return ctx.host().replaceAll(":\\d+$", ":" + statusPort.trim());
        }
        return ctx.host();
    }

    private record UploadResponse(String videoId, String uploadStatusUrl) {
    }

    private String resolveVideoId(Context ctx) {
        // Reuse caller-provided videoId when retrying, otherwise generate a new one
        String requestedVideoId = ctx.formParam("videoId");
        if (requestedVideoId == null || requestedVideoId.isBlank()) {
            requestedVideoId = ctx.queryParam("videoId");
        }
        if (requestedVideoId != null && !requestedVideoId.isBlank()) {
            try {
                String videoId = UUID.fromString(requestedVideoId.trim()).toString();
                logger.info("Using provided video ID: {}", videoId);
                return videoId;
            } catch (IllegalArgumentException e) {
                String videoId = UUID.randomUUID().toString();
                logger.warn("Invalid provided videoId '{}'; generated new video ID: {}", requestedVideoId, videoId);
                return videoId;
            }
        }
        String videoId = UUID.randomUUID().toString();
        logger.info("Assigned video ID: {}", videoId);
        return videoId;
    }

    private void processVideo(String videoId, Path inputPath, long startTime) {
        Path tempOutput = null;
        long processStart = System.currentTimeMillis();

        try {
            // 2. Create temp directory for segments
            tempOutput = Files.createTempDirectory("hls-" + videoId);

            // 3. Segment the video using FFmpeg
            // Assuming ffmpeg/ffprobe are in PATH. In a real app, these paths should be configurable.
            FFmpeg ffmpeg = new FFmpeg("ffmpeg");
            FFprobe ffprobe = new FFprobe("ffprobe");

            FFmpegBuilder builder = new FFmpegBuilder()
                    .setInput(inputPath.toString())
                    .addOutput(tempOutput.resolve("output.m3u8").toString())
                    .setFormat("hls")
                    .addExtraArgs("-start_number", "0")
                    .addExtraArgs("-hls_time", String.valueOf(segmentDuration))
                    .addExtraArgs("-hls_list_size", "0")
                    // Force keyframes at segment boundaries for closer-to-target durations.
                    .addExtraArgs("-force_key_frames", "expr:gte(t,n_forced*" + segmentDuration + ")")
                    .addExtraArgs("-sc_threshold", "0")
                    .done();

            logger.info("Starting FFmpeg segmentation for video: {}", videoId);
            long ffmpegStart = System.currentTimeMillis();

            // Start FFmpeg process
            FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);
            net.bramp.ffmpeg.job.FFmpegJob job = executor.createJob(builder);

            CompletableFuture<Void> ffmpegFuture = CompletableFuture.runAsync(() -> {
                job.run();
            }, ffmpegExecutor);

            // 4. Upload generated files to S3 as they become available
            // Using thread-safe Set in case future changes introduce concurrent access
            Set<Path> uploadedFiles = ConcurrentHashMap.newKeySet();
            Set<Integer> uploadedSegmentNumbers = ConcurrentHashMap.newKeySet();
            if (segmentUploadRepository != null) {
                try {
                    uploadedSegmentNumbers.addAll(segmentUploadRepository.findSegmentNumbers(videoId));
                    logger.info("Loaded {} existing uploaded segments for videoId={}", uploadedSegmentNumbers.size(), videoId);
                } catch (Exception e) {
                    logger.warn("Failed to load existing segment uploads for videoId={}", videoId, e);
                }
            }

            logger.info("Starting segment monitoring loop for video: {}", videoId);

            long loopStartTime = System.currentTimeMillis();
            long lastSegmentTime = System.currentTimeMillis();
            long currentPollingInterval = pollingIntervalMillis;

            while (!ffmpegFuture.isDone()) {
                if (System.currentTimeMillis() - loopStartTime > processingTimeoutMillis) {
                    logger.error("Video processing timed out for video: {}", videoId);

                    // Stop the FFmpeg job explicitly if possible
                    // FFmpegJob does not have a stop() method, but cancelling the future should interrupt the thread
                    // and hopefully the library handles interruption by killing the process.
                    ffmpegFuture.cancel(true);
                    throw new RuntimeException("Processing timed out after " + processingTimeoutMillis + "ms");
                }

                long batchStart = System.currentTimeMillis();
                int uploadedCount = uploadReadySegments(tempOutput, videoId, uploadedFiles, uploadedSegmentNumbers, false);
                if (uploadedCount > 0) {
                    logger.info("Uploaded {} segments in {} ms (poll batch) videoId={}",
                        uploadedCount, System.currentTimeMillis() - batchStart, videoId);
                }

                // Adaptive polling:
                // If we found segments, we might find more soon (fast processing), so keep interval short/default.
                // If we didn't find segments for a while, back off slightly to save resources, but cap at 5 seconds.
                if (uploadedCount > 0) {
                    lastSegmentTime = System.currentTimeMillis();
                    currentPollingInterval = pollingIntervalMillis;
                } else if (System.currentTimeMillis() - lastSegmentTime > 10000) {
                   // If no segments for 10 seconds, slow down polling
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

            // Verify FFmpeg completed successfully
            try {
                ffmpegFuture.join();
            } catch (CompletionException e) {
                 // Ensure job is stopped if an exception occurred during execution
                throw new RuntimeException("FFmpeg processing failed", e.getCause());
            }
            logger.info("FFmpeg segmentation completed videoId={} in {} ms",
                videoId, System.currentTimeMillis() - ffmpegStart);

            long totalSegments = 0;
            try (Stream<Path> stream = Files.list(tempOutput)) {
                totalSegments = stream
                    .filter(path -> path.getFileName().toString().endsWith(".ts"))
                    .count();
            }

            if (videoUploadRepository != null) {
                if (totalSegments > 0) {
                    videoUploadRepository.updateTotalSegments(videoId, (int) totalSegments);
                    jobTaskBus.publish(new UploadMetaEvent(videoId, (int) totalSegments));
                    logger.info("Successfully updated total segments to {} segments", totalSegments);
                }
            }

            // 5. Final sweep - upload remaining files (last segment + playlist)
            long finalSweepStart = System.currentTimeMillis();
            uploadReadySegments(tempOutput, videoId, uploadedFiles, uploadedSegmentNumbers, true);
            logger.info("Final sweep completed videoId={} in {} ms",
                videoId, System.currentTimeMillis() - finalSweepStart);
            int totalChunks = uploadedFiles.size();
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Successfully segmented and uploaded video: {}. Uploaded {} chunks (including playlist). Total time: {} ms", videoId, totalChunks, duration);
            if (videoUploadRepository != null) {
                videoUploadRepository.updateStatus(videoId, "COMPLETED");
            }

            // Here you would typically update a database status to "COMPLETED"

        } catch (Exception e) {
            logger.error("Upload/Processing failed for video: " + videoId, e);
            logger.warn("Recording failure for videoId={} machineId={} containerId={} reason={}",
                videoId, machineId, containerId, e.getMessage());
            if (videoUploadRepository != null) {
                videoUploadRepository.updateStatus(videoId, "FAILED");
            }
            // Here you would typically update a database status to "FAILED"
        } finally {
            // Give the OS a moment to release file locks if the process was just killed
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}

            // 6. Cleanup
            if (inputPath != null) {
                try { Files.deleteIfExists(inputPath); } catch (IOException ignored) {}
            }
            if (tempOutput != null) {
                try (Stream<Path> walk = Files.walk(tempOutput)) {
                    walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                } catch (IOException ignored) {}
            }
            logger.info("Upload pipeline finished videoId={} in {} ms",
                videoId, System.currentTimeMillis() - processStart);
        }
    }

    private int estimateTotalSegments(Path inputPath) throws IOException {
        FFprobe ffprobe = new FFprobe("ffprobe");
        double durationSeconds = ffprobe.probe(inputPath.toString()).getFormat().duration;
        if (durationSeconds <= 0) {
            return 0;
        }
        return (int) Math.ceil(durationSeconds / segmentDuration);
    }

    /**
     * @return number of files uploaded in this pass
     */
    private int uploadReadySegments(
            Path tempOutput,
            String videoId,
            Set<Path> uploadedFiles,
            Set<Integer> uploadedSegmentNumbers,
            boolean isFinalSweep
    ) {
        List<Path> files;
        try (Stream<Path> stream = Files.list(tempOutput)) {
             files = stream.toList();
        } catch (IOException e) {
            logger.error("Failed to list segments in directory: " + tempOutput, e);
            return 0;
        }

        int uploadedCount = 0;

        // 1. Handle .ts segments
        // Sort by Last Modified to ensure we process in order
        List<Path> tsFiles = files.stream()
                .filter(p -> p.getFileName().toString().endsWith(".ts"))
                .sorted(Comparator.comparingLong(p -> p.toFile().lastModified()))
                .toList();

        // If not final sweep, exclude the most recent file as it might be currently written by ffmpeg
        int tsLimit = isFinalSweep ? tsFiles.size() : Math.max(0, tsFiles.size() - 1);

        for (int i = 0; i < tsLimit; i++) {
            Path path = tsFiles.get(i);
            if (!uploadedFiles.contains(path)) {
                try {
                    OptionalInt segmentNumber = extractSegmentNumber(path.getFileName().toString());
                    if (segmentNumber.isPresent() && uploadedSegmentNumbers.contains(segmentNumber.getAsInt())) {
                        logger.info("Skipping already uploaded segment {} for videoId={}", segmentNumber.getAsInt(), videoId);
                        uploadedFiles.add(path);
                        continue;
                    }
                    uploadFile(path, videoId);
                    uploadedFiles.add(path);
                    if (segmentNumber.isPresent()) {
                        uploadedSegmentNumbers.add(segmentNumber.getAsInt());
                    }
                    uploadedCount++;
                } catch (Exception e) {
                    logger.error("Failed to upload segment: {}", path, e);
                    // Rethrow to stop processing if any segment upload fails.
                    throw e;
                }
            }
        }

        // 2. Handle .m3u8 playlist (only upload at the end to ensure it lists all segments)
        if (isFinalSweep) {
            files.stream()
                .filter(p -> p.getFileName().toString().endsWith(".m3u8"))
                .forEach(path -> {
                    if (!uploadedFiles.contains(path)) {
                        try {
                            uploadFile(path, videoId);
                            uploadedFiles.add(path);
                        } catch (Exception e) {
                            logger.error("Failed to upload playlist: {}", path, e);
                            throw e;
                        }
                    }
                });
            // We don't count playlist upload as a "segment" for backoff purposes essentially, but
            // since it is final sweep, the loop terminates anyway.
        }

        return uploadedCount;
    }

    private void uploadFile(Path path, String videoId) {
        try {
            String fileName = path.getFileName().toString();
            String objectKey = videoId + "/chunks/" + fileName;
            long size = Files.size(path);
            logger.info("Uploading segment: {} ({} bytes)", objectKey, size);

            try (InputStream is = new FileInputStream(path.toFile())) {
                storageClient.uploadFile(objectKey, is, size);
                if (fileName.endsWith(".ts")) {
                    if (segmentUploadRepository != null) {
                        OptionalInt segmentNumber = extractSegmentNumber(fileName);
                        if (segmentNumber.isPresent()) {
                            segmentUploadRepository.insert(videoId, segmentNumber.getAsInt());
                            logger.info("Recorded segment_upload videoId={} segmentNumber={}", videoId, segmentNumber.getAsInt());
                        } else {
                            logger.warn("Could not parse segment number from {}", fileName);
                        }
                    } else {
                        logger.warn("SegmentUploadRepository is null; skipping segment_upload insert");
                    }
                    jobTaskBus.publish(new JobTaskEvent(videoId, objectKey));
                }
                logger.info("Finished uploading segment: {}", objectKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload segment: " + path, e);
        }
    }

    private OptionalInt extractSegmentNumber(String fileName) {
        Matcher matcher = Pattern.compile("(\\d+)").matcher(fileName);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last >= 0 ? OptionalInt.of(last) : OptionalInt.empty();
    }

}
