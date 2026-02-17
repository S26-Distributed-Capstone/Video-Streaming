package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final int segmentDuration;
    private final ExecutorService ffmpegExecutor;
    private final ExecutorService supervisionExecutor;
    private final long processingTimeoutMillis;
    private final long pollingIntervalMillis;

    public UploadHandler(ObjectStorageClient storageClient) {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        this.storageClient = storageClient;
        String chunkSeconds = dotenv.get("CHUNK_DURATION_SECONDS");

        this.segmentDuration = dotenv.get("CHUNK_DURATION_SECONDS").isEmpty() ? 10 : Integer.parseInt(dotenv.get("CHUNK_DURATION_SECONDS"));

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

        // Create UUID for the video
        String videoId = UUID.randomUUID().toString();
        logger.info("Assigned video ID: {}", videoId);

        // We need to copy the uploaded file to a safe location because Javalin cleans up
        // the uploaded file once the request handler returns.
        Path inputPath;
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

        // Run the supervision logic on the supervision executor
        CompletableFuture.runAsync(() -> {
            processVideo(videoId, inputPath, startTime);
        }, supervisionExecutor);

        ctx.status(202).json(videoId);
    }

    private void processVideo(String videoId, Path inputPath, long startTime) {
        Path tempOutput = null;

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
                    .done();

            logger.info("Starting FFmpeg segmentation for video: {}", videoId);

            // Start FFmpeg process
            FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);
            net.bramp.ffmpeg.job.FFmpegJob job = executor.createJob(builder);

            CompletableFuture<Void> ffmpegFuture = CompletableFuture.runAsync(() -> {
                job.run();
            }, ffmpegExecutor);

            // 4. Upload generated files to S3 as they become available
            // Using thread-safe Set in case future changes introduce concurrent access
            Set<Path> uploadedFiles = ConcurrentHashMap.newKeySet();

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

                int uploadedCount = uploadReadySegments(tempOutput, videoId, uploadedFiles, false);

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

            // 5. Final sweep - upload remaining files (last segment + playlist)
            uploadReadySegments(tempOutput, videoId, uploadedFiles, true);

            int totalChunks = uploadedFiles.size();
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Successfully segmented and uploaded video: {}. Uploaded {} chunks (including playlist). Total time: {} ms", videoId, totalChunks, duration);

            // Here you would typically update a database status to "COMPLETED"

        } catch (Exception e) {
            logger.error("Upload/Processing failed for video: " + videoId, e);
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
        }
    }

    /**
     * @return number of files uploaded in this pass
     */
    private int uploadReadySegments(Path tempOutput, String videoId, Set<Path> uploadedFiles, boolean isFinalSweep) {
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
                    uploadFile(path, videoId);
                    uploadedFiles.add(path);
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
                logger.info("Finished uploading segment: {}", objectKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload segment: " + path, e);
        }
    }
}
