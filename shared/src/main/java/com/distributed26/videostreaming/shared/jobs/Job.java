package com.distributed26.videostreaming.shared.jobs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Job {
    private final String id;
    private Status status;
    private final Instant createdAt;
    private final List<Task> tasks;
    private final String payload;
    private final Map<String, String> metadata;

    public Job(String id, Instant createdAt, List<Task> tasks, String payload) {
        this(id, createdAt, tasks, payload, null);
    }

    public Job(String id, Instant createdAt, List<Task> tasks, String payload, Map<String, String> metadata) {
        this.id = Objects.requireNonNull(id, "id is null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt is null");
        this.tasks = new ArrayList<>(Objects.requireNonNull(tasks, "tasks is null"));
        this.payload = payload;
        this.metadata = (metadata == null) ? new HashMap<>() : validateMetadata(metadata);

        this.status = Status.CREATED;
    }

    public String getId() {
        return id;
    }

    public Status getStatus() {
        return status;
    }

    public synchronized void setStatus(Status status) {
        this.status = Objects.requireNonNull(status, "status is null");
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public List<Task> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    private static Map<String, String> validateMetadata(Map<String, String> metadata) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("metadata key is null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("metadata value is null");
            }
        }
        return new HashMap<>(metadata);
    }
}
