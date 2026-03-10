package com.distributed26.videostreaming.streaming.service;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.javalin.http.Context;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StreamingReadinessService {
    private static final Logger LOGGER = LogManager.getLogger(StreamingReadinessService.class);

    private final VideoStatusRepository videoStatusRepository;
    private final ObjectStorageClient storageClient;

    public StreamingReadinessService(VideoStatusRepository videoStatusRepository, ObjectStorageClient storageClient) {
        this.videoStatusRepository = videoStatusRepository;
        this.storageClient = storageClient;
    }

    public boolean validateVideoId(Context ctx) {
        String videoId = ctx.pathParam("videoId");
        try {
            UUID.fromString(videoId);
            return true;
        } catch (IllegalArgumentException e) {
            ctx.status(400).result("Invalid video ID");
            return false;
        }
    }

    public boolean validateProfile(Context ctx, String profile) {
        if (profile != null && profile.matches("^[A-Za-z0-9_-]+$")) {
            return true;
        }
        ctx.status(400).result("Invalid profile");
        return false;
    }

    public boolean requireCompleted(Context ctx) {
        if (videoStatusRepository == null) {
            ctx.status(500).result("Streaming status checks are not configured");
            return false;
        }
        String videoId = ctx.pathParam("videoId");
        Optional<String> status;
        try {
            status = videoStatusRepository.findStatusByVideoId(videoId);
        } catch (Exception e) {
            LOGGER.error("Failed to load video status for videoId={}", videoId, e);
            ctx.status(500).result("Failed to load video status");
            return false;
        }
        if (status.isEmpty()) {
            ctx.status(404).result("Video not found");
            return false;
        }
        if (!"COMPLETED".equalsIgnoreCase(status.get())) {
            ctx.status(409).result("Video is not ready");
            return false;
        }
        return true;
    }

    public List<ReadyVideoResponse> readyVideos(int limit) {
        if (videoStatusRepository == null) {
            throw new IllegalStateException("Streaming status checks are not configured");
        }
        return videoStatusRepository.findCompletedVideos(limit).stream()
                .filter(video -> storageClient.fileExists(video.videoId() + "/manifest/master.m3u8"))
                .map(video -> new ReadyVideoResponse(
                        video.videoId(),
                        video.videoName() == null || video.videoName().isBlank() ? video.videoId() : video.videoName()
                ))
                .toList();
    }

    public record ReadyVideoResponse(String videoId, String videoName) {
    }
}
