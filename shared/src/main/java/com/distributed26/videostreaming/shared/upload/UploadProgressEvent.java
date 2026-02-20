package com.distributed26.videostreaming.shared.upload;

public class UploadProgressEvent extends JobTaskEvent {
    private final int completedSegments;
    private final String type = "progress";

    public UploadProgressEvent(String jobId, int completedSegments) {
        super(jobId, "progress");
        this.completedSegments = completedSegments;
    }

    public int getCompletedSegments() {
        return completedSegments;
    }

    public String getType() {
        return type;
    }
}
