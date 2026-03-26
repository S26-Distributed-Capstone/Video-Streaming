package com.distributed26.videostreaming.shared.upload.events;

public class UploadStorageStatusEvent extends JobEvent {
    private final String state;
    private final String reason;
    private final String type = "storage_status";

    public UploadStorageStatusEvent(String jobId, String state, String reason) {
        super(jobId, "storage_status");
        this.state = state;
        this.reason = reason;
    }

    public String getState() {
        return state;
    }

    public String getReason() {
        return reason;
    }

    public String getType() {
        return type;
    }
}
