package com.distributed26.videostreaming.upload.processing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StorageRetryExecutor {
    private static final Logger logger = LogManager.getLogger(StorageRetryExecutor.class);

    private final long initialDelayMillis;
    private final long maxDelayMillis;

    public StorageRetryExecutor(long initialDelayMillis, long maxDelayMillis) {
        this.initialDelayMillis = Math.max(1L, initialDelayMillis);
        this.maxDelayMillis = Math.max(this.initialDelayMillis, maxDelayMillis);
    }

    public void run(String operationName, ThrowingRunnable operation) {
        run(operationName, RetryObserver.NOOP, operation);
    }

    public void run(String operationName, RetryObserver observer, ThrowingRunnable operation) {
        long delayMillis = initialDelayMillis;
        int attempt = 1;

        while (true) {
            try {
                operation.run();
                observer.onSucceeded(attempt);
                if (attempt > 1) {
                    logger.info("Storage operation recovered after retry op={} attempts={}", operationName, attempt);
                }
                return;
            } catch (java.util.concurrent.CancellationException e) {
                observer.onCancelled(e);
                throw e;
            } catch (NonRetriableException e) {
                observer.onCancelled(e);
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                observer.onCancelled(e);
                throw new java.util.concurrent.CancellationException(
                        "Interrupted while waiting to retry storage operation: " + operationName
                );
            } catch (RuntimeException e) {
                observer.onRetrying(attempt, e, delayMillis);
                logger.warn(
                        "Storage operation failed op={} attempt={} retryDelayMillis={}",
                        operationName,
                        attempt,
                        delayMillis,
                        e
                );
                sleep(delayMillis, operationName);
                delayMillis = nextDelay(delayMillis);
                attempt++;
            } catch (Exception e) {
                observer.onCancelled(e);
                throw new RuntimeException("Unexpected checked exception in storage operation: " + operationName, e);
            }
        }
    }

    private void sleep(long delayMillis, String operationName) {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while retrying storage operation: " + operationName, e);
        }
    }

    private long nextDelay(long currentDelayMillis) {
        long doubled = currentDelayMillis >= Long.MAX_VALUE / 2 ? Long.MAX_VALUE : currentDelayMillis * 2;
        return Math.min(maxDelayMillis, doubled);
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public interface RetryObserver {
        RetryObserver NOOP = new RetryObserver() {
        };

        default void onRetrying(int attempt, RuntimeException failure, long nextDelayMillis) {
        }

        default void onSucceeded(int attempts) {
        }

        default void onCancelled(Exception failure) {
        }
    }

    public static final class NonRetriableException extends RuntimeException {
        public NonRetriableException(String message) {
            super(message);
        }

        public NonRetriableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
