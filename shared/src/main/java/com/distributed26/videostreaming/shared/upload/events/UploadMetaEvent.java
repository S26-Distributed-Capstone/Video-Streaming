package com.distributed26.videostreaming.shared.upload.events;

public class UploadMetaEvent extends JobTaskEvent {
    private final int totalSegments;
    private final String type = "meta";

    public UploadMetaEvent(String jobId, int totalSegments) {
        super(jobId, "meta");
        this.totalSegments = totalSegments;
    }

    public int getTotalSegments() {
        return totalSegments;
    }

    public String getType() {
        return type;
    }
}
