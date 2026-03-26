package com.distributed26.videostreaming.upload.processing;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StorageStateTracker {
    private static final Logger logger = LogManager.getLogger(StorageStateTracker.class);

    private final VideoUploadRepository videoUploadRepository;
    private final StatusEventBus statusEventBus;
    private final AtomicBoolean serviceReady = new AtomicBoolean(true);
    private final AtomicInteger waitingOperations = new AtomicInteger(0);
    private final Map<String, AtomicInteger> waitingOperationsByVideo = new ConcurrentHashMap<>();

    public StorageStateTracker(VideoUploadRepository videoUploadRepository, StatusEventBus statusEventBus) {
        this.videoUploadRepository = videoUploadRepository;
        this.statusEventBus = statusEventBus;
    }

    public boolean isServiceReady() {
        return serviceReady.get();
    }

    public boolean isVideoWaiting(String videoId) {
        AtomicInteger count = waitingOperationsByVideo.get(videoId);
        return count != null && count.get() > 0;
    }

    public void beginStorageWait(String videoId, String reason) {
        int global = waitingOperations.incrementAndGet();
        if (global == 1) {
            serviceReady.set(false);
            logger.warn("Storage entered unavailable state reason={}", reason);
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

    public void endStorageWait(String videoId) {
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
            logger.info("Storage recovered and is available again");
        }
    }

    private void updateVideoStatus(String videoId, String status) {
        if (videoUploadRepository == null) {
            return;
        }
        try {
            videoUploadRepository.updateStatus(videoId, status);
        } catch (RuntimeException e) {
            logger.warn("Failed to update upload status videoId={} status={}", videoId, status, e);
        }
    }

    private void publishVideoStorageEvent(String videoId, String state, String reason) {
        if (statusEventBus == null) {
            return;
        }
        try {
            statusEventBus.publish(new UploadStorageStatusEvent(videoId, state, reason));
        } catch (RuntimeException e) {
            logger.warn("Failed to publish storage status event videoId={} state={}", videoId, state, e);
        }
    }
}
