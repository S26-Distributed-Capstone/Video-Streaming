package com.distributed26.videostreaming.upload.processing;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import net.bramp.ffmpeg.FFprobe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class UploadInitializationService {
    private static final Logger logger = LogManager.getLogger(UploadInitializationService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ObjectStorageClient storageClient;
    private final VideoUploadRepository videoUploadRepository;
    private final StorageRetryExecutor storageRetryExecutor;
    private final StorageStateTracker storageStateTracker;
    private final String machineId;
    private final String containerId;
    private final int segmentDuration;

    public UploadInitializationService(
            ObjectStorageClient storageClient,
            VideoUploadRepository videoUploadRepository,
            StorageRetryExecutor storageRetryExecutor,
            StorageStateTracker storageStateTracker,
            String machineId,
            String containerId,
            int segmentDuration
    ) {
        this.storageClient = storageClient;
        this.videoUploadRepository = videoUploadRepository;
        this.storageRetryExecutor = storageRetryExecutor;
        this.storageStateTracker = storageStateTracker;
        this.machineId = machineId;
        this.containerId = containerId;
        this.segmentDuration = segmentDuration;
    }

    public void initializeUploadRecord(UploadRequest request, Path inputPath) {
        if (videoUploadRepository != null) {
            int totalSegments = 0;
            try {
                totalSegments = videoUploadRepository.findByVideoId(request.videoId())
                        .map(r -> r.getTotalSegments())
                        .orElse(0);
            } catch (Exception e) {
                logger.warn("Failed to load existing upload record for videoId={}", request.videoId(), e);
            }
            videoUploadRepository.create(
                    request.videoId(),
                    request.videoName(),
                    totalSegments,
                    "PROCESSING",
                    machineId,
                    containerId
            );
            try {
                int estimatedSegments = estimateTotalSegments(inputPath);
                if (estimatedSegments > 0) {
                    videoUploadRepository.updateTotalSegments(request.videoId(), estimatedSegments);
                }
            } catch (Exception e) {
                logger.warn("Failed to estimate total segments for video: {}", request.videoId(), e);
            }
        }
    }

    public void ensureVideoMetadataStored(String videoId, String videoName) {
        storageRetryExecutor.run(
                "store metadata for videoId=" + videoId,
                new StorageRetryExecutor.RetryObserver() {
                    private boolean waiting;

                    @Override
                    public void onRetrying(int attempt, RuntimeException failure, long nextDelayMillis) {
                        if (!waiting) {
                            waiting = true;
                            storageStateTracker.beginStorageWait(videoId, failure.getMessage());
                        }
                    }

                    @Override
                    public void onSucceeded(int attempts) {
                        if (waiting) {
                            waiting = false;
                            storageStateTracker.endStorageWait(videoId);
                        }
                    }

                    @Override
                    public void onCancelled(Exception failure) {
                        if (waiting) {
                            waiting = false;
                            storageStateTracker.endStorageWait(videoId);
                        }
                    }
                },
                () -> storeVideoMetadata(videoId, videoName)
        );
    }

    public void deleteVideoMetadata(String videoId) {
        try {
            storageClient.deleteFile(videoId + "/metadata.json");
            logger.info("Deleted video metadata for videoId={}", videoId);
        } catch (Exception e) {
            logger.warn("Failed to delete video metadata for videoId={}", videoId, e);
        }
    }

    public int getSegmentDuration() {
        return segmentDuration;
    }

    private void storeVideoMetadata(String videoId, String videoName) {
        try {
            String key = videoId + "/metadata.json";
            byte[] payload = OBJECT_MAPPER.writeValueAsBytes(
                    java.util.Map.of("videoId", videoId, "videoName", videoName)
            );
            storageClient.uploadFile(key, new ByteArrayInputStream(payload), payload.length);
            logger.info("Stored video metadata at {}", key);
        } catch (Exception e) {
            throw new RuntimeException("Failed to store video metadata", e);
        }
    }

    private int estimateTotalSegments(Path inputPath) throws IOException {
        FFprobe ffprobe = new FFprobe("ffprobe");
        double durationSeconds = ffprobe.probe(inputPath.toString()).getFormat().duration;
        if (durationSeconds <= 0) {
            return 0;
        }
        return (int) Math.ceil(durationSeconds / segmentDuration);
    }
}
