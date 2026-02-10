package com.distributed26.videostreaming.shared.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JobTest {
    @Test
    public void constructorSetsDefaultsAndCopiesCollections() {
        Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
        List<Task> tasks = new ArrayList<>();
        tasks.add(new Task("t1", "j1", TaskType.TRANSCODE, "payload", 0, 3));
        Map<String, String> metadata = new HashMap<>();
        metadata.put("k1", "v1");

        Job job = new Job("j1", createdAt, tasks, "job-payload", metadata);

        assertEquals("j1", job.getId());
        assertEquals(createdAt, job.getCreatedAt());
        assertEquals("job-payload", job.getPayload());
        assertEquals(Status.CREATED, job.getStatus());
        assertEquals(1, job.getTasks().size());
        assertEquals(1, job.getMetadata().size());
        assertEquals("v1", job.getMetadata().get("k1"));

        tasks.clear();
        metadata.clear();
        assertEquals(1, job.getTasks().size());
        assertEquals(1, job.getMetadata().size());

        assertThrows(UnsupportedOperationException.class, () ->
                job.getTasks().add(new Task("t2", "j1", TaskType.SEGMENT, "payload", 0, 3)));
        assertThrows(UnsupportedOperationException.class, () -> job.getMetadata().put("k2", "v2"));
    }

    @Test
    public void constructorRejectsNulls() {
        Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
        List<Task> tasks = List.of(new Task("t1", "j1", TaskType.TRANSCODE, "payload", 0, 3));

        assertThrows(NullPointerException.class, () -> new Job(null, createdAt, tasks, "payload"));
        assertThrows(NullPointerException.class, () -> new Job("j1", null, tasks, "payload"));
        assertThrows(NullPointerException.class, () -> new Job("j1", createdAt, null, "payload"));
    }

    @Test
    public void constructorRejectsInvalidMetadata() {
        Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
        List<Task> tasks = List.of(new Task("t1", "j1", TaskType.TRANSCODE, "payload", 0, 3));

        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "v1");
        assertThrows(IllegalArgumentException.class, () -> new Job("j1", createdAt, tasks, "payload", nullKey));

        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("k1", null);
        assertThrows(IllegalArgumentException.class, () -> new Job("j1", createdAt, tasks, "payload", nullValue));
    }

    @Test
    public void setStatusRejectsNull() {
        Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
        List<Task> tasks = List.of(new Task("t1", "j1", TaskType.TRANSCODE, "payload", 0, 3));
        Job job = new Job("j1", createdAt, tasks, "payload");

        assertThrows(NullPointerException.class, () -> job.setStatus(null));
    }

    @Test
    public void setStatusUpdatesValue() {
        Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
        List<Task> tasks = List.of(new Task("t1", "j1", TaskType.TRANSCODE, "payload", 0, 3));
        Job job = new Job("j1", createdAt, tasks, "payload");

        job.setStatus(Status.RUNNING);
        assertEquals(Status.RUNNING, job.getStatus());
    }
}
