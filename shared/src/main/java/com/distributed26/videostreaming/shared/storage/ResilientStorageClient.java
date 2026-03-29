package com.distributed26.videostreaming.shared.storage;

import java.io.InputStream;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Decorator around {@link ObjectStorageClient} that adds retry with exponential
 * backoff to write/list/admin operations. This makes the processing pipeline
 * resilient to transient MinIO / S3 outages — callers block (with bounded
 * back-off sleeps) instead of failing immediately.
 *
 * <p><strong>Pass-through (no retry):</strong> {@code fileExists},
 * {@code downloadFile}, and {@code uploadFile} are delegated directly.
 * {@code fileExists} is always an optimistic pre-check handled by
 * {@code safeFileExists}. {@code downloadFile} is called by
 * {@code TranscodingTask.downloadChunkWithRetry()}, which already has its own
 * bounded retry with exponential backoff. {@code uploadFile} cannot be safely
 * retried at this layer because the {@code InputStream} argument is consumed on
 * the first attempt — if that attempt partially reads the stream before failing,
 * subsequent retries would upload corrupt/truncated data. Every caller already
 * has its own retry mechanism that reopens the stream:
 * <ul>
 *   <li>{@code LocalSpoolUploadWorkerPool} — catches failure, resets task to
 *       PENDING, next poll cycle opens a fresh {@code FileInputStream}</li>
 *   <li>{@code TranscodingTask} — failure re-queues the task via RabbitMQ</li>
 *   <li>{@code AbrManifestService} — failure propagates, manifest is regenerated</li>
 * </ul>
 *
 * <p><strong>Non-transient errors are never retried.</strong> If the cause
 * chain contains an {@code S3Exception} with a 4xx status code (other than
 * 408 Request Timeout or 429 Too Many Requests), the error is thrown
 * immediately — retrying will not fix a missing key, missing bucket, or
 * access-denied error.
 *
 * <p>Back-off parameters are configurable via {@code .env} / environment variables
 * (resolved by the caller and passed to the constructor):
 * <ul>
 *   <li>{@code STORAGE_RETRY_INITIAL_DELAY_MS} — first delay (default 500 ms)</li>
 *   <li>{@code STORAGE_RETRY_MAX_DELAY_MS} — ceiling delay (default 30 000 ms)</li>
 *   <li>{@code STORAGE_RETRY_MAX_ATTEMPTS} — max attempts, 0 = unlimited (default 0)</li>
 * </ul>
 */
public class ResilientStorageClient implements ObjectStorageClient {
    private static final Logger LOGGER = LogManager.getLogger(ResilientStorageClient.class);

    private final ObjectStorageClient delegate;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final int maxAttempts; // 0 = unlimited

    /** Default retry parameters — caller should use the 4-arg constructor for custom config. */
    public static final long DEFAULT_INITIAL_DELAY_MS = 500L;
    public static final long DEFAULT_MAX_DELAY_MS = 30_000L;
    public static final int DEFAULT_MAX_ATTEMPTS = 0;

    public ResilientStorageClient(ObjectStorageClient delegate) {
        this(delegate, DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_DELAY_MS, DEFAULT_MAX_ATTEMPTS);
    }

    public ResilientStorageClient(ObjectStorageClient delegate, long initialDelayMs, long maxDelayMs, int maxAttempts) {
        this.delegate = delegate;
        this.initialDelayMs = Math.max(1L, initialDelayMs);
        this.maxDelayMs = Math.max(this.initialDelayMs, maxDelayMs);
        this.maxAttempts = Math.max(0, maxAttempts);
    }

    // --- Delegated operations with retry ---

    /**
     * No retry — the {@code InputStream} is consumed on the first attempt. If
     * that attempt partially reads the stream before failing, subsequent retries
     * would upload corrupt or truncated data (most InputStreams are not
     * rewindable). Every caller already handles failure with its own mechanism
     * that recreates the stream from its source (file on disk, byte array, etc.).
     */
    @Override
    public void uploadFile(String key, InputStream data, long size) {
        delegate.uploadFile(key, data, size);
    }

    /**
     * No retry — {@code downloadFile} is called by
     * {@code TranscodingTask.downloadChunkWithRetry()}, which already has its
     * own bounded retry with exponential backoff and descriptive error messages.
     * Wrapping it in another (potentially unlimited) retry loop would make that
     * bounded logic dead code and could tie up worker threads indefinitely on
     * missing keys or prolonged outages.
     */
    @Override
    public InputStream downloadFile(String key) {
        return delegate.downloadFile(key);
    }

    @Override
    public void deleteFile(String key) {
        retryVoid("deleteFile(" + key + ")", () -> delegate.deleteFile(key));
    }

    /**
     * No retry — {@code fileExists} is only ever used as an optimistic
     * idempotency check ("does the output already exist?"). Every caller
     * already handles failure gracefully via {@code safeFileExists} (returns
     * {@code false} on error so transcoding proceeds). Retrying here would
     * just block the worker thread for no reason when MinIO is down.
     */
    @Override
    public boolean fileExists(String key) {
        return delegate.fileExists(key);
    }

    @Override
    public List<String> listFiles(String prefix) {
        return retry("listFiles(" + prefix + ")", () -> delegate.listFiles(prefix));
    }

    @Override
    public void ensureBucketExists() {
        retryVoid("ensureBucketExists", delegate::ensureBucketExists);
    }

    @Override
    public String generatePresignedUrl(String key, long durationSeconds) {
        return retry("generatePresignedUrl(" + key + ")",
                () -> delegate.generatePresignedUrl(key, durationSeconds));
    }

    @Override
    public void close() {
        delegate.close();
    }

    // --- Retry engine ---

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface RunnableWithException {
        void run() throws Exception;
    }

    private <T> T retry(String operationName, SupplierWithException<T> operation) {
        long delay = initialDelayMs;
        int attempt = 0;
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted before " + operationName + " attempt " + (attempt + 1));
            }
            attempt++;
            try {
                return operation.get();
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    throw toRuntime("Interrupted during " + operationName, e);
                }
                if (isNonTransient(e)) {
                    throw toRuntime(operationName + " failed with non-transient error", e);
                }
                if (maxAttempts > 0 && attempt >= maxAttempts) {
                    throw toRuntime(operationName + " failed after " + attempt + " attempt(s)", e);
                }
                LOGGER.warn("Storage operation {} failed (attempt {}{}) — retrying in {} ms: {}",
                        operationName,
                        attempt,
                        maxAttempts > 0 ? "/" + maxAttempts : "",
                        delay,
                        e.toString());
                sleep(delay);
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted after sleep during " + operationName + " (attempt " + attempt + ")");
                }
                delay = Math.min(maxDelayMs, delay * 2);
            }
        }
    }

    private void retryVoid(String operationName, RunnableWithException operation) {
        retry(operationName, () -> { operation.run(); return null; });
    }

    /**
     * Walks the exception cause chain looking for an {@link S3Exception} with a
     * 4xx status code. Client errors (missing key, missing bucket, access
     * denied, invalid request) will never succeed on retry — retrying wastes
     * worker time and hides the real error.
     *
     * <p>408 (Request Timeout) and 429 (Too Many Requests) are excluded because
     * they are transient and <em>should</em> be retried.
     */
    static boolean isNonTransient(Exception e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof S3Exception s3ex) {
                int status = s3ex.statusCode();
                if (status >= 400 && status < 500 && status != 408 && status != 429) {
                    return true;
                }
            }
            if (current.getCause() == current) break;
            current = current.getCause();
        }
        return false;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static RuntimeException toRuntime(String message, Exception cause) {
        if (cause instanceof RuntimeException re) {
            return new RuntimeException(message, re);
        }
        return new RuntimeException(message, cause);
    }
}
