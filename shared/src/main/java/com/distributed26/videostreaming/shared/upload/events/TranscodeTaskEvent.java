package com.distributed26.videostreaming.shared.upload.events;

import java.util.Objects;

public class TranscodeTaskEvent extends JobEvent {
    private final String type = "transcode_task";
    private final String chunkKey;
    private final String profile;
    private final int segmentNumber;

    public TranscodeTaskEvent(String jobId, String chunkKey, String profile, int segmentNumber) {
        super(jobId, buildTaskId(profile, segmentNumber));
        this.chunkKey = Objects.requireNonNull(chunkKey, "chunkKey is null");
        this.profile = Objects.requireNonNull(profile, "profile is null");
        this.segmentNumber = segmentNumber;
    }

    public String getType() {
        return type;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getProfile() {
        return profile;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    private static String buildTaskId(String profile, int segmentNumber) {
        return "transcode:" + Objects.requireNonNull(profile, "profile is null") + ":" + segmentNumber;
    }
}
