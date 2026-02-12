package com.distributed26.videostreaming.upload;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    public UploadHandler(ObjectStorageClient storageClient) {
        Dotenv dotenv = Dotenv.load();
        this.storageClient = storageClient;
        this.segmentDuration =dotenv.get("CHUNK_DURATION_SECONDS").isEmpty() ? 10 : Integer.parseInt(dotenv.get("CHUNK_DURATION_SECONDS"));
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

        Path tempInput = null;
        Path tempOutput = null;

        try {
            // 1. Save uploaded file to temp file
            tempInput = Files.createTempFile("upload-" + videoId, ".tmp");
            try (InputStream is = uploadedFile.content()) {
                Files.copy(is, tempInput, StandardCopyOption.REPLACE_EXISTING);
            }

            // 2. Create temp directory for segments
            tempOutput = Files.createTempDirectory("hls-" + videoId);

            // 3. Segment the video using FFmpeg asynchronously
            // Assuming ffmpeg/ffprobe are in PATH. In a real app, these paths should be configurable.
            FFmpeg ffmpeg = new FFmpeg("ffmpeg");
            FFprobe ffprobe = new FFprobe("ffprobe");

            FFmpegBuilder builder = new FFmpegBuilder()
                    .setInput(tempInput.toString())
                    .addOutput(tempOutput.resolve("output.m3u8").toString())
                    .setFormat("hls")
                    .addExtraArgs("-start_number", "0")
                    .addExtraArgs("-hls_time", String.valueOf(segmentDuration))
                    .addExtraArgs("-hls_list_size", "0")
                    .done();

            // Run FFmpeg in a separate thread
            logger.info("Starting background FFmpeg segmentation task for video: {}", videoId);
            CompletableFuture<Void> ffmpegFuture = CompletableFuture.runAsync(() -> {
                long ffmpegStart = System.currentTimeMillis();
                new FFmpegExecutor(ffmpeg, ffprobe).createJob(builder).run();
                logger.info("Background FFmpeg segmentation completed for video: {} in {} ms", videoId, System.currentTimeMillis() - ffmpegStart);
            });

            // 4. Upload generated files to S3 as they become available
            Set<Path> uploadedFiles = new HashSet<>();

            logger.info("Starting segment monitoring loop for video: {}", videoId);

            while (!ffmpegFuture.isDone()) {
                uploadReadySegments(tempOutput, videoId, uploadedFiles, false);
                try {
                    Thread.sleep(1000); // Poll every second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Upload interrupted", e);
                }
            }

            // Verify FFmpeg completed successfully
            try {
                ffmpegFuture.join();
            } catch (CompletionException e) {
                throw new RuntimeException("FFmpeg processing failed", e.getCause());
            }

            // 5. Final sweep - upload remaining files (last segment + playlist)
            uploadReadySegments(tempOutput, videoId, uploadedFiles, true);

            int totalChunks = uploadedFiles.size();
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Successfully segmented and uploaded video: {}. Uploaded {} chunks (including playlist). Total time: {} ms", videoId, totalChunks, duration);
            ctx.status(201).json(videoId);

        } catch (Exception e) {
            logger.error("Upload/Processing failed", e);
            ctx.status(500).result("Upload/Processing failed: " + e.getMessage());
        } finally {
            // 6. Cleanup
            if (tempInput != null) {
                try { Files.deleteIfExists(tempInput); } catch (IOException ignored) {}
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

    private void uploadReadySegments(Path tempOutput, String videoId, Set<Path> uploadedFiles, boolean isFinalSweep) {
        try (Stream<Path> stream = Files.list(tempOutput)) {
            List<Path> files = stream.toList();

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
                    uploadFile(path, videoId);
                    uploadedFiles.add(path);
                }
            }

            // 2. Handle .m3u8 playlist (only upload at the end to ensure it lists all segments)
            if (isFinalSweep) {
                files.stream()
                    .filter(p -> p.getFileName().toString().endsWith(".m3u8"))
                    .forEach(path -> {
                        if (!uploadedFiles.contains(path)) {
                            uploadFile(path, videoId);
                            uploadedFiles.add(path);
                        }
                    });
            }

        } catch (IOException e) {
            logger.error("Failed to list segments in directory: " + tempOutput, e);
        }
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
