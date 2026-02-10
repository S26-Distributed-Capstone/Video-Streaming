package com.distributed26.videostreaming.shared.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;

public class WorkerTest {
    @Test
    public void constructorSetsDefaults() {
        Instant registeredAt = Instant.parse("2024-01-01T00:00:00Z");
        Worker worker = new Worker("w1", registeredAt);

        assertEquals("w1", worker.getId());
        assertEquals(registeredAt, worker.getRegisteredAt());
        assertEquals(WorkerStatus.IDLE, worker.getStatus());
        assertEquals(registeredAt, worker.getLastHeartbeatAt());
    }

    @Test
    public void constructorRejectsNulls() {
        Instant registeredAt = Instant.parse("2024-01-01T00:00:00Z");
        assertThrows(NullPointerException.class, () -> new Worker(null, registeredAt));
        assertThrows(NullPointerException.class, () -> new Worker("w1", null));
    }

    @Test
    public void setStatusRejectsNull() {
        Worker worker = new Worker("w1", Instant.parse("2024-01-01T00:00:00Z"));
        assertThrows(NullPointerException.class, () -> worker.setStatus(null));
    }

    @Test
    public void setStatusUpdatesValue() {
        Worker worker = new Worker("w1", Instant.parse("2024-01-01T00:00:00Z"));
        worker.setStatus(WorkerStatus.BUSY);
        assertEquals(WorkerStatus.BUSY, worker.getStatus());
    }

    @Test
    public void heartbeatUpdatesLastHeartbeatAt() throws InterruptedException {
        Worker worker = new Worker("w1", Instant.parse("2024-01-01T00:00:00Z"));
        Instant before = worker.getLastHeartbeatAt();
        Thread.sleep(2);
        worker.heartbeat();
        Instant after = worker.getLastHeartbeatAt();

        assertTrue(after.isAfter(before));
    }
}
