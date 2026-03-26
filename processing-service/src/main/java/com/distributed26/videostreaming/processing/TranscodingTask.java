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
    private static final int DOWNLOAD_MAX_ATTEMPTS;
    private static final long DOWNLOAD_RETRY_INITIAL_DELAY_MILLIS;
    private static final long DOWNLOAD_RETRY_MAX_DELAY_MILLIS;

    /**
     * Number of threads each FFmpeg process may use.
     * Defaults to 2 so that multiple concurrent workers don't fight over all CPU cores.
     * Override with the THREADS_PER_WORKER env var.
     * The default worker pool size is 3/4 of available CPUs (see ProcessingServiceApplication).
     */
    static final int FFMPEG_THREADS;
    static final String FFMPEG_PRESET;
    static final int CHUNK_DURATION_SECONDS;
    static {
        String val = System.getenv("THREADS_PER_WORKER");
        int parsed = 2;
        if (val != null && !val.isBlank()) {
            try { parsed = Integer.parseInt(val.trim()); } catch (NumberFormatException ignored) {}
        }
        FFMPEG_THREADS = parsed;

        String preset = System.getenv("FFMPEG_PRESET");
        if (preset == null || preset.isBlank()) {
            FFMPEG_PRESET = "veryfast";
        } else {
            FFMPEG_PRESET = preset.trim();
        }

        CHUNK_DURATION_SECONDS = parseIntEnv("CHUNK_DURATION_SECONDS", 10);
    }

    static {
        DOWNLOAD_MAX_ATTEMPTS = parseIntEnv("DOWNLOAD_MAX_ATTEMPTS", 5);
        DOWNLOAD_RETRY_INITIAL_DELAY_MILLIS = parseLongEnv("DOWNLOAD_RETRY_INITIAL_DELAY_MILLIS", 250L);
        DOWNLOAD_RETRY_MAX_DELAY_MILLIS = parseLongEnv("DOWNLOAD_RETRY_MAX_DELAY_MILLIS", 4000L);
    }

    private final TranscodingProfile profile;
    private final String chunkKey;
    private final String outputKey;
    private final double outputTsOffsetSeconds;

    public TranscodingTask(String id, String jobId, String chunkKey, TranscodingProfile profile) {
        this(id, jobId, chunkKey, profile, -1d);
    }

    public TranscodingTask(String id, String jobId, String chunkKey, TranscodingProfile profile, double outputTsOffsetSeconds) {
        super(id, jobId, TaskType.TRANSCODE, chunkKey, 0, 3);
        this.chunkKey = chunkKey;
        this.profile = profile;
        this.outputKey = deriveOutputKey(chunkKey, profile);
        this.outputTsOffsetSeconds = outputTsOffsetSeconds;
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
        execute(storageClient, null);
    }

    /**
     * Download source, transcode, and upload output. Optional callback runs
     * after transcoding completes and right before upload begins.
     */
    public void execute(ObjectStorageClient storageClient, Runnable beforeUpload) throws IOException {
        if (safeFileExists(storageClient, outputKey)) {
            LOGGER.info("Output already exists, skipping: {}", outputKey);
            return;
        }

        Path inputTemp = Files.createTempFile("transcode-in-", ".ts");
        Path outputTemp = Files.createTempFile("transcode-out-", ".ts");

        try {
            CompletedTranscode completed = transcodeInternal(storageClient, inputTemp, outputTemp);
            long size = completed.sizeBytes();
            if (beforeUpload != null) {
                beforeUpload.run();
            }
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

    public CompletedTranscode transcodeToSpool(ObjectStorageClient storageClient, Path spoolRoot) throws IOException {
        if (safeFileExists(storageClient, outputKey)) {
            LOGGER.info("Output already exists in object storage, skipping local spool: {}", outputKey);
            return null;
        }

        Path inputTemp = Files.createTempFile("transcode-in-", ".ts");
        Path outputTemp = Files.createTempFile("transcode-out-", ".ts");

        try {
            CompletedTranscode completed = transcodeInternal(storageClient, inputTemp, outputTemp);
            Path finalPath = spoolPath(spoolRoot);
            Files.createDirectories(finalPath.getParent());
            Path partialPath = finalPath.resolveSibling(finalPath.getFileName() + ".part");
            Files.copy(outputTemp, partialPath, StandardCopyOption.REPLACE_EXISTING);
            Files.move(partialPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            long size = Files.size(finalPath);
            return new CompletedTranscode(finalPath, outputKey, size, completed.outputTsOffsetSeconds());
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Path finalPath = spoolPath(spoolRoot);
            Files.createDirectories(finalPath.getParent());
            Files.copy(outputTemp, finalPath, StandardCopyOption.REPLACE_EXISTING);
            long size = Files.size(finalPath);
            return new CompletedTranscode(finalPath, outputKey, size, extractEffectiveOffsetSeconds());
        } finally {
            Files.deleteIfExists(inputTemp);
            Files.deleteIfExists(outputTemp);
        }
    }

    public Path spoolPath(Path spoolRoot) {
        String fileName = outputKey.substring(outputKey.lastIndexOf('/') + 1);
        return spoolRoot
                .resolve(getJobId())
                .resolve(profile.getName())
                .resolve(fileName);
    }

    private CompletedTranscode transcodeInternal(
            ObjectStorageClient storageClient,
            Path inputTemp,
            Path outputTemp
    ) throws IOException {
        LOGGER.info("Downloading source chunk: {}", chunkKey);
        downloadChunkWithRetry(storageClient, inputTemp);

        LOGGER.info("Transcoding chunk={} profile={}", chunkKey, profile.getName());
        String maxrate = profile.getBitrate() + "";
        String bufsize = (profile.getBitrate() * 2) + "";
        double effectiveOutputTsOffsetSeconds = extractEffectiveOffsetSeconds();
        FFmpegBuilder builder = new FFmpegBuilder()
                .setInput(inputTemp.toString())
                .addOutput(outputTemp.toString())
                .setFormat("mpegts")
                    .addExtraArgs("-output_ts_offset", String.format(java.util.Locale.US, "%.3f", effectiveOutputTsOffsetSeconds))
                    .addExtraArgs("-vf", "scale=-2:" + profile.getVerticalResolution())
                    .addExtraArgs("-c:v", "libx264")
                    .addExtraArgs("-preset", FFMPEG_PRESET)
                    .addExtraArgs("-crf", "23")
                    .addExtraArgs("-pix_fmt", "yuv420p")
                    .addExtraArgs("-maxrate", maxrate)
                    .addExtraArgs("-bufsize", bufsize)
                    .addExtraArgs("-x264-params", "scenecut=0:open_gop=0")
                    .addExtraArgs("-threads", String.valueOf(FFMPEG_THREADS))
                    .addExtraArgs("-c:a", "aac")
                    .addExtraArgs("-b:a", "128k")
                    .addExtraArgs("-ac", "2")
                    .addExtraArgs("-ar", "48000")
                    .addExtraArgs("-af", "aresample=async=1:first_pts=0")
                    .addExtraArgs("-muxpreload", "0")
                    .addExtraArgs("-muxdelay", "0")
                    .done();

        new FFmpegExecutor(FfmpegHolder.FFMPEG, FfmpegHolder.FFPROBE).createJob(builder).run();
        return new CompletedTranscode(outputTemp, outputKey, Files.size(outputTemp), effectiveOutputTsOffsetSeconds);
    }

    private double extractEffectiveOffsetSeconds() {
        int segmentNumber = extractSegmentNumber(chunkKey);
        return outputTsOffsetSeconds >= 0d
                ? outputTsOffsetSeconds
                : segmentNumber >= 0
                ? (double) segmentNumber * Math.max(1, CHUNK_DURATION_SECONDS)
                : 0d;
    }

    public record CompletedTranscode(Path localPath, String outputKey, long sizeBytes, double outputTsOffsetSeconds) {
    }

    public static int chunkDurationSeconds() {
        return CHUNK_DURATION_SECONDS;
    }

    /**
     * Checks whether a file exists in object storage, returning {@code false}
     * (rather than crashing) when MinIO is unreachable. Because all MinIO writes
     * are idempotent, the safe default is to proceed with transcoding when the
     * existence check cannot be completed.
     */
    private static boolean safeFileExists(ObjectStorageClient storageClient, String key) {
        try {
            return storageClient.fileExists(key);
        } catch (Exception e) {
            LOGGER.warn("Unable to check if output already exists in object storage (key={}). " +
                    "Proceeding with transcoding since writes are idempotent: {}", key, e.toString());
            return false;
        }
    }

    private void downloadChunkWithRetry(ObjectStorageClient storageClient, Path inputTemp) throws IOException {
        IOException lastIo = null;
        RuntimeException lastRuntime = null;
        long delayMillis = Math.max(1L, DOWNLOAD_RETRY_INITIAL_DELAY_MILLIS);
        int attempts = Math.max(1, DOWNLOAD_MAX_ATTEMPTS);

        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (InputStream is = storageClient.downloadFile(chunkKey)) {
                Files.copy(is, inputTemp, StandardCopyOption.REPLACE_EXISTING);
                if (attempt > 1) {
                    LOGGER.info("Download recovered for chunk={} on attempt {}/{}", chunkKey, attempt, attempts);
                }
                return;
            } catch (IOException e) {
                lastIo = e;
                if (attempt == attempts) {
                    break;
                }
                LOGGER.warn("Source chunk download failed (attempt {}/{}) chunk={}: {}",
                        attempt, attempts, chunkKey, describeDownloadError(e));
                sleepBeforeRetry(delayMillis);
                delayMillis = Math.min(Math.max(1L, DOWNLOAD_RETRY_MAX_DELAY_MILLIS), delayMillis * 2);
            } catch (RuntimeException e) {
                lastRuntime = e;
                if (attempt == attempts) {
                    break;
                }
                LOGGER.warn("Source chunk download failed (attempt {}/{}) chunk={}: {}",
                        attempt, attempts, chunkKey, describeDownloadError(e));
                sleepBeforeRetry(delayMillis);
                delayMillis = Math.min(Math.max(1L, DOWNLOAD_RETRY_MAX_DELAY_MILLIS), delayMillis * 2);
            }
        }

        String detail = lastIo != null ? describeDownloadError(lastIo)
                : lastRuntime != null ? describeDownloadError(lastRuntime)
                : "unknown error";
        throw new IOException(
                "Failed to download source chunk from MinIO after " + attempts + " attempt(s): chunk=" + chunkKey
                        + " — " + detail,
                lastIo != null ? lastIo : lastRuntime);
    }

    /**
     * Produces a concise, human-readable description of a download failure cause.
     * Unwraps common wrapper exceptions so the log message shows the root issue
     * (e.g. "Connection refused" rather than a stack of IllegalStateExceptions).
     */
    private static String describeDownloadError(Exception e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String rootMsg = root.getMessage();
        if (rootMsg == null || rootMsg.isBlank()) {
            rootMsg = root.getClass().getSimpleName();
        }
        // If the root is different from the original, include both for context
        if (root != e) {
            return e.getClass().getSimpleName() + ": " + rootMsg
                    + " (caused by " + root.getClass().getSimpleName() + ")";
        }
        return e.getClass().getSimpleName() + ": " + rootMsg;
    }

    private static void sleepBeforeRetry(long delayMillis) throws IOException {
        try {
            Thread.sleep(Math.max(1L, delayMillis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry download", e);
        }
    }

    private static int parseIntEnv(String key, int defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long parseLongEnv(String key, long defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    /** e.g. "{videoId}/chunks/output0.ts" + "low"  →  "{videoId}/processed/low/output0.ts" */
    private static String deriveOutputKey(String chunkKey, TranscodingProfile profile) {
        String[] parts = chunkKey.split("/");
        String videoId = parts[0];
        String fileName = parts[parts.length - 1];
        return videoId + "/processed/" + profile.getName() + "/" + fileName;
    }

    private static int extractSegmentNumber(String chunkKey) {
        if (chunkKey == null) {
            return -1;
        }
        int slash = chunkKey.lastIndexOf('/');
        String fileName = slash >= 0 ? chunkKey.substring(slash + 1) : chunkKey;
        int dot = fileName.lastIndexOf('.');
        String base = dot > 0 ? fileName.substring(0, dot) : fileName;
        int end = base.length();
        while (end > 0 && Character.isDigit(base.charAt(end - 1))) {
            end -= 1;
        }
        if (end == base.length()) {
            return -1;
        }
        try {
            return Integer.parseInt(base.substring(end));
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}
