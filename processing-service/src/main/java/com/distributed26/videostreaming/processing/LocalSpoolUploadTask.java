package com.distributed26.videostreaming.processing;

public record LocalSpoolUploadTask(
        long id,
        String videoId,
        String spoolOwner,
        String profile,
        int segmentNumber,
        String chunkKey,
        String outputKey,
        String spoolPath,
        long sizeBytes,
        double outputTsOffsetSeconds,
        int attemptCount
) {
}
