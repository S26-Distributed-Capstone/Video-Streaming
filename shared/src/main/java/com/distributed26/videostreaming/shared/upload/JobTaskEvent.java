package com.distributed26.videostreaming.shared.upload;

import java.util.Objects;

public class JobTaskEvent {
    private final String jobId;
    private final String taskId;

    public JobTaskEvent(String jobId, String taskId) {
        this.jobId = Objects.requireNonNull(jobId, "jobId is null");
        this.taskId = Objects.requireNonNull(taskId, "taskId is null");
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }
}
