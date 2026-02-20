package com.distributed26.videostreaming.shared.upload.events;

public class UploadFailedEvent extends JobTaskEvent {
    private final String reason;
    private final String machineId;
    private final String containerId;
    private final String type = "failed";

    public UploadFailedEvent(String jobId, String reason, String machineId, String containerId) {
        super(jobId, "failed");
        this.reason = reason;
        this.machineId = machineId;
        this.containerId = containerId;
    }

    public String getReason() {
        return reason;
    }

    public String getMachineId() {
        return machineId;
    }

    public String getContainerId() {
        return containerId;
    }

    public String getType() {
        return type;
    }
}
