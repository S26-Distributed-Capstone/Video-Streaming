package com.distributed26.videostreaming.shared.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

public class StatusTest {
    @Test
    public void valuesContainsAllStatuses() {
        Status[] values = Status.values();
        assertEquals(6, values.length);
        assertEquals(EnumSet.of(
                Status.CREATED,
                Status.QUEUED,
                Status.RUNNING,
                Status.SUCCEEDED,
                Status.FAILED,
                Status.CANCELED
        ), EnumSet.allOf(Status.class));
    }

    @Test
    public void valueOfParsesNames() {
        assertEquals(Status.CREATED, Status.valueOf("CREATED"));
        assertEquals(Status.QUEUED, Status.valueOf("QUEUED"));
        assertNotNull(Status.valueOf("RUNNING"));
    }
}
