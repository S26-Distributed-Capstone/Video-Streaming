package com.distributed26.videostreaming.streaming.service;

import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class VideoDeletionRetryWorker implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(VideoDeletionRetryWorker.class);
    private static final List<String> RETRYABLE_STATUSES = List.of("DELETING", "DELETE_FAILED");
    private static final int BATCH_SIZE = 25;

    private final VideoStatusRepository videoStatusRepository;
    private final StreamingReadinessService readinessService;
    private final PlaylistService playlistService;

    public VideoDeletionRetryWorker(
            VideoStatusRepository videoStatusRepository,
            StreamingReadinessService readinessService,
            PlaylistService playlistService
    ) {
        this.videoStatusRepository = videoStatusRepository;
        this.readinessService = readinessService;
        this.playlistService = playlistService;
    }

    @Override
    public void run() {
        if (videoStatusRepository == null) {
            return;
        }
        List<String> videoIds = videoStatusRepository.findVideoIdsByStatuses(RETRYABLE_STATUSES, BATCH_SIZE);
        for (String videoId : videoIds) {
            try {
                boolean deleted = readinessService.deleteVideo(videoId);
                if (deleted) {
                    playlistService.invalidateVideo(videoId);
                    LOGGER.info("Retried video deletion successfully for videoId={}", videoId);
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Video deletion retry failed for videoId={}", videoId, e);
            }
        }
    }
}
