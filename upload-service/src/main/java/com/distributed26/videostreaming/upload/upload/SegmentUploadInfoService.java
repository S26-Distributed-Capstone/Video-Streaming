package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import java.util.Objects;

/**
 * Reads how many segments are already recorded for a given videoId.
 * Intended to be used by a future queue consumer.
 */
public class SegmentUploadInfoService {
    private final SegmentUploadRepository segmentUploadRepository;

    public SegmentUploadInfoService(SegmentUploadRepository segmentUploadRepository) {
        this.segmentUploadRepository = Objects.requireNonNull(segmentUploadRepository, "segmentUploadRepository is null");
    }

    public static SegmentUploadInfoService fromEnv() {
        return new SegmentUploadInfoService(SegmentUploadRepository.fromEnv());
    }

    public int getUploadedSegmentCount(String videoId) {
        if (videoId == null || videoId.isBlank()) {
            throw new IllegalArgumentException("videoId is required");
        }
        return segmentUploadRepository.countByVideoId(videoId);
    }
}
