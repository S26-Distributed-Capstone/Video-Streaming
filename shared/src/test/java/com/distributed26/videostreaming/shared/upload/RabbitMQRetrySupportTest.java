package com.distributed26.videostreaming.shared.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RabbitMQRetrySupportTest {

    @Test
    void retriesIoFailureUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();

        String result = withRetryProperties(0, () -> RabbitMQRetrySupport.retry("test operation", () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IOException("not ready");
            }
            return "ok";
        }));

        assertEquals("ok", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void wrapsTimeoutFailureWhenAttemptsExhausted() {
        AtomicInteger attempts = new AtomicInteger();

        RuntimeException error = assertThrows(RuntimeException.class, () ->
                withRetryProperties(2, () -> RabbitMQRetrySupport.retry("test operation", () -> {
                    attempts.incrementAndGet();
                    throw new TimeoutException("timed out");
                }))
        );

        assertEquals(2, attempts.get());
        assertEquals("Failed to test operation after 2 attempt(s)", error.getMessage());
    }

    private static <T> T withRetryProperties(int maxAttempts, ThrowingSupplier<T> action) {
        String previousAttempts = System.getProperty("RABBITMQ_RETRY_MAX_ATTEMPTS");
        String previousInitialDelay = System.getProperty("RABBITMQ_RETRY_INITIAL_DELAY_MS");
        String previousMaxDelay = System.getProperty("RABBITMQ_RETRY_MAX_DELAY_MS");
        try {
            System.setProperty("RABBITMQ_RETRY_MAX_ATTEMPTS", String.valueOf(maxAttempts));
            System.setProperty("RABBITMQ_RETRY_INITIAL_DELAY_MS", "1");
            System.setProperty("RABBITMQ_RETRY_MAX_DELAY_MS", "1");
            return action.run();
        } finally {
            if (previousAttempts == null) {
                System.clearProperty("RABBITMQ_RETRY_MAX_ATTEMPTS");
            } else {
                System.setProperty("RABBITMQ_RETRY_MAX_ATTEMPTS", previousAttempts);
            }
            if (previousInitialDelay == null) {
                System.clearProperty("RABBITMQ_RETRY_INITIAL_DELAY_MS");
            } else {
                System.setProperty("RABBITMQ_RETRY_INITIAL_DELAY_MS", previousInitialDelay);
            }
            if (previousMaxDelay == null) {
                System.clearProperty("RABBITMQ_RETRY_MAX_DELAY_MS");
            } else {
                System.setProperty("RABBITMQ_RETRY_MAX_DELAY_MS", previousMaxDelay);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T run();
    }
}
