package com.distributed26.videostreaming.upload.db;

import java.util.Objects;

public class JobTaskRecord {
    private final String jobId;
    private final int numTasks;

    public JobTaskRecord(String jobId, int numTasks) {
        this.jobId = Objects.requireNonNull(jobId, "jobId is null");
        this.numTasks = numTasks;
    }

    public String getJobId() {
        return jobId;
    }

    public int getNumTasks() {
        return numTasks;
    }
}
