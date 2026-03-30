package com.distributed26.videostreaming.processing.runtime;

import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class ProcessingStorageStateTracker {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingStorageStateTracker.class);
    private static final String DEV_LOG_SERVICE = "Processing-service";

    private final VideoProcessingRepository videoProcessingRepository;
    private final StatusEventBus statusEventBus;
    private final RabbitMQDevLogPublisher devLogPublisher;
    private final AtomicBoolean serviceReady = new AtomicBoolean(true);
    private final AtomicInteger waitingOperations = new AtomicInteger(0);
    private final Map<String, AtomicInteger> waitingOperationsByVideo = new ConcurrentHashMap<>();

    ProcessingStorageStateTracker(
            VideoProcessingRepository videoProcessingRepository,
            StatusEventBus statusEventBus,
            RabbitMQDevLogPublisher devLogPublisher
    ) {
        this.videoProcessingRepository = videoProcessingRepository;
        this.statusEventBus = statusEventBus;
        this.devLogPublisher = devLogPublisher;
    }

    boolean isServiceReady() {
        return serviceReady.get();
    }

    boolean isVideoWaiting(String videoId) {
        AtomicInteger count = waitingOperationsByVideo.get(videoId);
        return count != null && count.get() > 0;
    }

    void beginStorageWait(String videoId, String reason) {
        int global = waitingOperations.incrementAndGet();
        if (global == 1) {
            serviceReady.set(false);
            LOGGER.warn("Processing storage entered unavailable state reason={}", reason);
            publishDevLogWarn("MinIO is down, waiting for it to come back before processing uploads resume");
        }

        if (videoId == null || videoId.isBlank()) {
            return;
        }

        AtomicInteger perVideo = waitingOperationsByVideo.computeIfAbsent(videoId, ignored -> new AtomicInteger(0));
        if (perVideo.incrementAndGet() == 1) {
            updateVideoStatus(videoId, "WAITING_FOR_STORAGE");
            publishVideoStorageEvent(videoId, "WAITING", reason);
        }
    }

    void endStorageWait(String videoId) {
        if (videoId != null && !videoId.isBlank()) {
            AtomicInteger perVideo = waitingOperationsByVideo.get(videoId);
            if (perVideo != null) {
                int remaining = perVideo.decrementAndGet();
                if (remaining <= 0) {
                    waitingOperationsByVideo.remove(videoId, perVideo);
                    updateVideoStatus(videoId, "PROCESSING");
                    publishVideoStorageEvent(videoId, "AVAILABLE", null);
                }
            }
        }

        int global = waitingOperations.updateAndGet(current -> Math.max(0, current - 1));
        if (global == 0) {
            serviceReady.set(true);
            LOGGER.info("Processing storage recovered and is available again");
            publishDevLogInfo("MinIO recovered; processing uploads resumed");
        }
    }

    private void updateVideoStatus(String videoId, String status) {
        if (videoProcessingRepository == null) {
            return;
        }
        try {
            videoProcessingRepository.updateStatus(videoId, status);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to update processing status videoId={} status={}", videoId, status, e);
        }
    }

    private void publishVideoStorageEvent(String videoId, String state, String reason) {
        if (statusEventBus == null) {
            return;
        }
        try {
            statusEventBus.publish(new UploadStorageStatusEvent(videoId, state, reason));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing storage status videoId={} state={}", videoId, state, e);
        }
    }

    private void publishDevLogWarn(String message) {
        if (devLogPublisher == null) {
            return;
        }
        try {
            devLogPublisher.publishWarn(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing dev log warning message={}", message, e);
        }
    }

    private void publishDevLogInfo(String message) {
        if (devLogPublisher == null) {
            return;
        }
        try {
            devLogPublisher.publishInfo(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing dev log info message={}", message, e);
        }
    }
}
