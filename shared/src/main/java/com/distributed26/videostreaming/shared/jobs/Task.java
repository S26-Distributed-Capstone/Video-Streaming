package com.distributed26.videostreaming.shared.jobs;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Task {
    private final String id;
    private final String jobId;
    private final TaskType type;
    private final String payload;
    private final Map<String, String> metadata;
    private Status status;
    private int attempt;
    private int maxAttempts;
    private String assignedWorkerId;
    private final Instant createdAt;

    public Task(
            String id,
            String jobId,
            TaskType type,
            String payload,
            int attempt,
            int maxAttempts
    ) {
        this(id, jobId, type, payload, null, attempt, maxAttempts);
    }

    public Task(
            String id,
            String jobId,
            TaskType type,
            String payload,
            Map<String, String> metadata,
            int attempt,
            int maxAttempts
    ) {
        this.id = Objects.requireNonNull(id, "id is null");
        this.jobId = Objects.requireNonNull(jobId, "jobId is null");
        this.type = Objects.requireNonNull(type, "type is null");

        this.status = Status.CREATED;
        this.payload = payload;
        this.metadata = (metadata == null) ? new HashMap<>() : new HashMap<>(metadata);
        this.attempt = attempt;
        this.maxAttempts = maxAttempts;
        this.assignedWorkerId = null;
        this.createdAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    public TaskType getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = Objects.requireNonNull(status, "status is null");
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public void setAssignedWorkerId(String assignedWorkerId) {
        this.assignedWorkerId = assignedWorkerId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
