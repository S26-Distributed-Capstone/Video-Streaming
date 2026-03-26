package com.distributed26.videostreaming.shared.storage;

import java.io.InputStream;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Decorator around {@link ObjectStorageClient} that adds retry with exponential
 * backoff to every operation. This makes the processing pipeline resilient to
 * transient MinIO / S3 outages — callers block (with bounded back-off sleeps)
 * instead of failing immediately.
 *
 * <p>The retry loop runs until the operation succeeds or the calling thread is
 * interrupted (e.g. container shutdown). There is no hard attempt cap because
 * MinIO may be down for an extended period and the service should resume
 * automatically once connectivity is restored.
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

    @Override
    public void uploadFile(String key, InputStream data, long size) {
        retryVoid("uploadFile(" + key + ")", () -> delegate.uploadFile(key, data, size));
    }

    @Override
    public InputStream downloadFile(String key) {
        return retry("downloadFile(" + key + ")", () -> delegate.downloadFile(key));
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
