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
    private final int attempt;
    private final int maxAttempts;
    private final String assignedWorkerId;
    private final Instant createdAt;

    public Task(
            String id,
            String jobId,
            TaskType type,
            String payload,
            int attempt,
            int maxAttempts
    ) {
        this(id, jobId, type, payload, null, Status.CREATED, attempt, maxAttempts, null, Instant.now());
    }

    public Task(
            String id,
            String jobId,
            TaskType type,
            String payload,
            Map<String, String> metadata,
            Status status,
            int attempt,
            int maxAttempts,
            String assignedWorkerId,
            Instant createdAt
    ) {
        this.id = Objects.requireNonNull(id, "id is null");
        this.jobId = Objects.requireNonNull(jobId, "jobId is null");
        this.type = Objects.requireNonNull(type, "type is null");

        this.status = Objects.requireNonNull(status, "status is null");
        this.payload = payload;
        this.metadata = (metadata == null) ? new HashMap<>() : Util.validateAndCopy(metadata);
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be >= 0");
        }
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must be >= 0");
        }
        if (attempt > maxAttempts) {
            throw new IllegalArgumentException("attempt cannot exceed maxAttempts");
        }
        this.attempt = attempt;
        this.maxAttempts = maxAttempts;
        this.assignedWorkerId = assignedWorkerId;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt is null");
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

    public synchronized Status getStatus() {
        return status;
    }

    public synchronized void setStatus(Status status){
        this.status = status;
    }

    public synchronized int getAttempt() {
        return attempt;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public synchronized String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

}
