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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
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

            // 3. Segment the video using FFmpeg
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

            new FFmpegExecutor(ffmpeg, ffprobe).createJob(builder).run();

            // 4. Upload all generated files to S3
            int totalChunks;
            int uploadedChunks = 0;

            try (Stream<Path> stream = Files.list(tempOutput)) {
                List<Path> files = stream.collect(Collectors.toList());
                totalChunks = files.size();

                for (Path path : files) {
                    try {
                        String fileName = path.getFileName().toString();
                        String objectKey = videoId + "/" + fileName;
                        long size = Files.size(path);
                        logger.info("Uploading segment: {} ({} bytes)", objectKey, size);

                        try (InputStream is = new FileInputStream(path.toFile())) {
                            storageClient.uploadFile(objectKey, is, size);
                            uploadedChunks++;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to upload segment: " + path, e);
                    }
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Successfully segmented and uploaded video: {}. Uploaded {}/{} chunks. Total time: {} ms", videoId, uploadedChunks, totalChunks, duration);
            ctx.status(201).json(videoId);

        } catch (Exception e) {
            logger.error("Upload/Processing failed", e);
            ctx.status(500).result("Upload/Processing failed: " + e.getMessage());
        } finally {
            // 5. Cleanup
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
}
