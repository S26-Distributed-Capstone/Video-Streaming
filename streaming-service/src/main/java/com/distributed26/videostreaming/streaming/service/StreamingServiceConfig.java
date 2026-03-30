package com.distributed26.videostreaming.streaming.service;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import io.github.cdimascio.dotenv.Dotenv;

public final class StreamingServiceConfig {
    private static final int DEFAULT_STREAMING_PORT = 8083;
    private static final int DEFAULT_DELETE_RETRY_INTERVAL_SECONDS = 120;

    private final int port;
    private final int deleteRetryIntervalSeconds;
    private final StorageConfig storageConfig;

    private StreamingServiceConfig(int port, int deleteRetryIntervalSeconds, StorageConfig storageConfig) {
        this.port = port;
        this.deleteRetryIntervalSeconds = deleteRetryIntervalSeconds;
        this.storageConfig = storageConfig;
    }

    public static StreamingServiceConfig fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        return new StreamingServiceConfig(
                parsePort(dotenv.get("STREAMING_PORT")),
                parsePositiveInt(dotenv.get("STREAMING_DELETE_RETRY_INTERVAL_SECONDS"),
                        DEFAULT_DELETE_RETRY_INTERVAL_SECONDS),
                new StorageConfig(
                        getEnvOrDotenv(dotenv, "MINIO_ENDPOINT", "http://localhost:9000"),
                        getEnvOrDotenv(dotenv, "MINIO_PUBLIC_ENDPOINT", null),
                        getEnvOrDotenv(dotenv, "MINIO_ACCESS_KEY", "minioadmin"),
                        getEnvOrDotenv(dotenv, "MINIO_SECRET_KEY", "minioadmin"),
                        getEnvOrDotenv(dotenv, "MINIO_BUCKET_NAME", "uploads"),
                        getEnvOrDotenv(dotenv, "MINIO_REGION", "us-east-1")
                )
        );
    }

    public int port() {
        return port;
    }

    public int deleteRetryIntervalSeconds() {
        return deleteRetryIntervalSeconds;
    }

    public StorageConfig storageConfig() {
        return storageConfig;
    }

    private static int parsePort(String rawPort) {
        if (rawPort == null || rawPort.isBlank()) {
            return DEFAULT_STREAMING_PORT;
        }
        try {
            return Integer.parseInt(rawPort.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_STREAMING_PORT;
        }
    }

    private static int parsePositiveInt(String rawValue, int defaultValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        try {
            return Math.max(1, Integer.parseInt(rawValue.trim()));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        String dotenvVal = dotenv.get(key);
        return (dotenvVal == null || dotenvVal.isBlank()) ? defaultValue : dotenvVal;
    }
}
