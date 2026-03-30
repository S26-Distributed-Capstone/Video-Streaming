package com.distributed26.videostreaming.shared.upload;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class RabbitMQRetrySupport {
    private static final Logger LOGGER = LogManager.getLogger(RabbitMQRetrySupport.class);
    private static final Dotenv DOTENV = Dotenv.configure().directory("./").ignoreIfMissing().load();
    private static final long DEFAULT_INITIAL_DELAY_MILLIS = 1_000L;
    private static final long DEFAULT_MAX_DELAY_MILLIS = 30_000L;
    private static final long DEFAULT_MAX_ATTEMPTS = 0L;

    private RabbitMQRetrySupport() {
    }

    static <T> T retry(String operationName, RetryableAction<T> action) {
        Objects.requireNonNull(operationName, "operationName is null");
        Objects.requireNonNull(action, "action is null");

        long initialDelayMillis = readLongSetting("RABBITMQ_RETRY_INITIAL_DELAY_MS", DEFAULT_INITIAL_DELAY_MILLIS, 1L);
        long maxDelayMillis = readLongSetting("RABBITMQ_RETRY_MAX_DELAY_MS", DEFAULT_MAX_DELAY_MILLIS, initialDelayMillis);
        long maxAttempts = readLongSetting("RABBITMQ_RETRY_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS, 0L);
        long delayMillis = initialDelayMillis;
        long attempt = 0L;

        while (true) {
            attempt++;
            try {
                return action.run();
            } catch (IOException | TimeoutException e) {
                if (maxAttempts > 0 && attempt >= maxAttempts) {
                    throw new RuntimeException("Failed to " + operationName + " after " + attempt + " attempt(s)", e);
                }
                LOGGER.warn(
                        "Failed to {} on attempt {}. Retrying in {} ms",
                        operationName,
                        attempt,
                        delayMillis,
                        e
                );
                sleep(delayMillis, operationName);
                delayMillis = Math.min(maxDelayMillis, delayMillis * 2);
            }
        }
    }

    private static void sleep(long delayMillis, String operationName) {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while retrying " + operationName, interrupted);
        }
    }

    private static long readLongSetting(String key, long defaultValue, long minimumValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getenv(key);
        }
        if (value == null || value.isBlank()) {
            value = DOTENV.get(key);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Math.max(minimumValue, Long.parseLong(value.trim()));
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid {} value '{}', using default {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    @FunctionalInterface
    interface RetryableAction<T> {
        T run() throws IOException, TimeoutException;
    }
}
