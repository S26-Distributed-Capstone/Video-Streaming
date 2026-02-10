package com.distributed26.videostreaming.shared.jobs;

import java.time.Instant;
import java.util.Objects;

public class Worker {
    private final String id;
    private WorkerStatus status;
    private final Instant registeredAt;
    private Instant lastHeartbeatAt;

    public Worker(String id, Instant registeredAt) {
        this.id = Objects.requireNonNull(id, "id is null");
        this.registeredAt = Objects.requireNonNull(registeredAt, "registeredAt");

        this.status = WorkerStatus.IDLE;
        this.lastHeartbeatAt = registeredAt;
    }

    public String getId() {
        return id;
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public void setStatus(WorkerStatus status) {
        this.status = Objects.requireNonNull(status, "status is null");
    }

    public Instant getRegisteredAt() {
        return registeredAt;
    }

    public Instant getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    public void heartbeat() {
        this.lastHeartbeatAt = Instant.now();
    }
}
