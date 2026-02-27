package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.shared.jobs.Task;
import com.distributed26.videostreaming.shared.jobs.TaskType;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Transcodes one source chunk to a single {@link TranscodingProfile} using FFmpeg.
 *
 * Source: {@code {videoId}/chunks/{fileName}}
 * Output: {@code {videoId}/processed/{profile}/{fileName}}
 */
public class TranscodingTask extends Task {
    private static final Logger LOGGER = LogManager.getLogger(TranscodingTask.class);

    /**
     * Number of threads each FFmpeg process may use.
     * Defaults to 2 so that multiple concurrent workers don't fight over all CPU cores.
     * Override with the THREADS_PER_WORKER env var.
     * Rule of thumb: WORKER_POOL_SIZE = floor(CPU_cores / THREADS_PER_WORKER).
     */
    static final int FFMPEG_THREADS;
    static {
        String val = System.getenv("THREADS_PER_WORKER");
        int parsed = 2;
        if (val != null && !val.isBlank()) {
            try { parsed = Integer.parseInt(val.trim()); } catch (NumberFormatException ignored) {}
        }
        FFMPEG_THREADS = parsed;
    }

    private final TranscodingProfile profile;
    private final String chunkKey;
    private final String outputKey;

    public TranscodingTask(String id, String jobId, String chunkKey, TranscodingProfile profile) {
        super(id, jobId, TaskType.TRANSCODE, chunkKey, 0, 3);
        this.chunkKey = chunkKey;
        this.profile = profile;
        this.outputKey = deriveOutputKey(chunkKey, profile);
    }

    public TranscodingProfile getProfile() { return profile; }
    public String getChunkKey() { return chunkKey; }
    public String getOutputKey() { return outputKey; }

    /**
     * FFmpeg / FFprobe are initialized once (lazily on first real execute) via the
     * initialization-on-demand holder pattern.  This avoids running `ffmpeg -version`
     * on every task and keeps the unit tests fast (they mock fileExists → true so the
     * holder is never triggered in test runs).
     */
    private static final class FfmpegHolder {
        static final FFmpeg  FFMPEG;
        static final FFprobe FFPROBE;
        static {
            try {
                FFMPEG  = new FFmpeg("ffmpeg");
                FFPROBE = new FFprobe("ffprobe");
            } catch (IOException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    /**
     * Download source, transcode, upload result. Idempotent — skips if outputKey already exists.
     */
    public void execute(ObjectStorageClient storageClient) throws IOException {
        if (storageClient.fileExists(outputKey)) {
            LOGGER.info("Output already exists, skipping: {}", outputKey);
            return;
        }

        Path inputTemp = Files.createTempFile("transcode-in-", ".ts");
        Path outputTemp = Files.createTempFile("transcode-out-", ".ts");

        try {
            LOGGER.info("Downloading source chunk: {}", chunkKey);
            try (InputStream is = storageClient.downloadFile(chunkKey)) {
                Files.copy(is, inputTemp, StandardCopyOption.REPLACE_EXISTING);
            }

            LOGGER.info("Transcoding chunk={} profile={}", chunkKey, profile.getName());
            // Use CRF for quality-based encoding with a hard maxrate ceiling.
            // -b:v alone is only an average target and libx264 can freely exceed it,
            // causing the HIGH profile to inflate the file size above the source.
            // -maxrate/-bufsize enforce the VBV ceiling; -crf avoids wasting bits on
            // simple scenes that would otherwise be padded up to the ABR target.
            String maxrate = profile.getBitrate() + ""; // bps string for ffmpeg
            String bufsize = (profile.getBitrate() * 2) + "";
            FFmpegBuilder builder = new FFmpegBuilder()
                    .setInput(inputTemp.toString())
                    .addOutput(outputTemp.toString())
                        .addExtraArgs("-vf", "scale=-2:" + profile.getVerticalResolution())
                        .addExtraArgs("-c:v", "libx264")
                        .addExtraArgs("-crf", "23")
                        .addExtraArgs("-maxrate", maxrate)
                        .addExtraArgs("-bufsize", bufsize)
                        .addExtraArgs("-threads", String.valueOf(FFMPEG_THREADS))
                        .addExtraArgs("-an")
                        .done();

            new FFmpegExecutor(FfmpegHolder.FFMPEG, FfmpegHolder.FFPROBE).createJob(builder).run();

            long size = Files.size(outputTemp);
            LOGGER.info("Uploading transcoded output: {} ({} bytes)", outputKey, size);
            try (InputStream is = new FileInputStream(outputTemp.toFile())) {
                storageClient.uploadFile(outputKey, is, size);
            }

            LOGGER.info("Done: chunk={} profile={} output={}", chunkKey, profile.getName(), outputKey);
        } finally {
            Files.deleteIfExists(inputTemp);
            Files.deleteIfExists(outputTemp);
        }
    }

    /** e.g. "{videoId}/chunks/output0.ts" + "low"  →  "{videoId}/processed/low/output0.ts" */
    private static String deriveOutputKey(String chunkKey, TranscodingProfile profile) {
        String[] parts = chunkKey.split("/");
        String videoId = parts[0];
        String fileName = parts[parts.length - 1];
        return videoId + "/processed/" + profile.getName() + "/" + fileName;
    }
}
