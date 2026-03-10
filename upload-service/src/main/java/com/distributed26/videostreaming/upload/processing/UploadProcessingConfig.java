package com.distributed26.videostreaming.upload.processing;

import io.github.cdimascio.dotenv.Dotenv;

public record UploadProcessingConfig(
        int maxVideoNameLength,
        int segmentDuration,
        long processingTimeoutMillis,
        long pollingIntervalMillis,
        int ffmpegPoolSize,
        int uploadPoolSize,
        int maxInFlightSegmentUploads
) {
    public static UploadProcessingConfig fromDotenv(Dotenv dotenv) {
        int uploadPoolSize = envInt(
                dotenv,
                "UPLOAD_THREAD_POOL_SIZE",
                Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 2))
        );
        return new UploadProcessingConfig(
                200,
                envInt(dotenv, "CHUNK_DURATION_SECONDS", 10),
                envLong(dotenv, "PROCESSING_TIMEOUT_SECONDS", 3600L) * 1000L,
                envLong(dotenv, "POLLING_INTERVAL_MILLIS", 1000L),
                envInt(dotenv, "FFMPEG_THREAD_POOL_SIZE", Runtime.getRuntime().availableProcessors()),
                uploadPoolSize,
                envInt(dotenv, "MAX_IN_FLIGHT_SEGMENT_UPLOADS", uploadPoolSize * 2)
        );
    }

    private static int envInt(Dotenv dotenv, String key, int defaultValue) {
        String value = dotenv.get(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long envLong(Dotenv dotenv, String key, long defaultValue) {
        String value = dotenv.get(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}
