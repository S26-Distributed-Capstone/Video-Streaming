package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class TerminalFailureStorageCleanup implements JobEventListener, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(TerminalFailureStorageCleanup.class);
    private static final String USER_CANCELLED_REASON = "user_cancelled";

    private final ObjectStorageClient storageClient;
    private final VideoUploadRepository videoUploadRepository;
    private final ExecutorService cleanupExecutor;
    private final Set<String> queuedVideoIds = ConcurrentHashMap.newKeySet();

    public TerminalFailureStorageCleanup(ObjectStorageClient storageClient, VideoUploadRepository videoUploadRepository) {
        this.storageClient = storageClient;
        this.videoUploadRepository = videoUploadRepository;
        this.cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "terminal-failure-storage-cleanup");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onEvent(JobEvent event) {
        if (!(event instanceof UploadFailedEvent failed)) {
            return;
        }
        String videoId = failed.getJobId();
        if (!shouldDeleteStorage(videoId, failed.getReason())) {
            LOGGER.info("Skipping object storage cleanup for videoId={} because failure is not terminal", videoId);
            return;
        }
        if (!queuedVideoIds.add(videoId)) {
            LOGGER.debug("Cleanup already queued for failed videoId={}", videoId);
            return;
        }
        cleanupExecutor.submit(() -> {
            try {
                deleteVideoObjects(videoId, failed.getReason());
            } finally {
                queuedVideoIds.remove(videoId);
            }
        });
    }

    private boolean shouldDeleteStorage(String videoId, String reason) {
        if (USER_CANCELLED_REASON.equalsIgnoreCase(reason)) {
            return true;
        }
        if (videoUploadRepository == null) {
            return false;
        }
        try {
            return videoUploadRepository.isFailed(videoId);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to verify terminal DB state for videoId={}", videoId, e);
            return false;
        }
    }

    private void deleteVideoObjects(String videoId, String reason) {
        String prefix = videoId + "/";
        try {
            List<String> objectKeys = new ArrayList<>(storageClient.listFiles(prefix));
            if (objectKeys.isEmpty()) {
                LOGGER.info("No object storage cleanup needed for failed videoId={} reason={}", videoId, reason);
                return;
            }
            for (String objectKey : objectKeys) {
                storageClient.deleteFile(objectKey);
            }
            LOGGER.info("Deleted {} object(s) from storage for failed videoId={} reason={}",
                    objectKeys.size(), videoId, reason);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to delete object storage for failed videoId={} reason={}", videoId, reason, e);
        }
    }

    @Override
    public void close() {
        cleanupExecutor.shutdownNow();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Terminal failure storage cleanup executor did not terminate cleanly");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while shutting down terminal failure storage cleanup", e);
        }
    }
}
