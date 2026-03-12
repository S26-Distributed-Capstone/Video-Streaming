package com.distributed26.videostreaming.shared.upload;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class FailedVideoRegistry {
    private final Set<String> failedVideoIds = ConcurrentHashMap.newKeySet();

    public void markFailed(String videoId) {
        if (videoId == null || videoId.isBlank()) {
            return;
        }
        failedVideoIds.add(videoId);
    }

    public boolean isFailed(String videoId) {
        return videoId != null && failedVideoIds.contains(videoId);
    }

    public void clear(String videoId) {
        if (videoId == null || videoId.isBlank()) {
            return;
        }
        failedVideoIds.remove(videoId);
    }
}
