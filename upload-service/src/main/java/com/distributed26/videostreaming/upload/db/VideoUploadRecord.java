package com.distributed26.videostreaming.upload.db;

import java.util.Objects;

public class VideoUploadRecord {
    private final String videoId;
    private final int totalSegments;
    private final String status;
    private final String machineId;

    public VideoUploadRecord(String videoId, int totalSegments, String status, String machineId) {
        this.videoId = Objects.requireNonNull(videoId, "videoId is null");
        this.totalSegments = totalSegments;
        this.status = Objects.requireNonNull(status, "status is null");
        this.machineId = machineId;
    }

    public String getVideoId() {
        return videoId;
    }

    public int getTotalSegments() {
        return totalSegments;
    }

    public String getStatus() {
        return status;
    }

    public String getMachineId() {
        return machineId;
    }
}
