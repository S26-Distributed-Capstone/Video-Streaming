package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
import com.distributed26.videostreaming.upload.db.JobTaskRepository;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JobTaskTrackerTest {
    /*
     * This test verifies the JobTaskTracker flow:
     * - Given a jobId, load numTasks from Postgres via JobTaskRepository.
     * - Subscribe to the JobTaskBus for that jobId.
     * - Publish task completion events for that jobId.
     * - Assert the tracker counts tasks and reports completion.
    */
    @Disabled
    @Test
    void tracksCompletionCounts() {
        String jobId = "job-123";

        JobTaskRepository repo = JobTaskRepository.fromEnv();

        JobTaskBus bus = new TestJobTaskBus();
        JobTaskTracker tracker = new JobTaskTracker(repo, bus);

        tracker.start(jobId);

        int numTasks = tracker.getNumTasks();
        for (int i = 1; i <= numTasks; i++) {
            bus.publish(new JobTaskEvent(jobId, "task-" + i));
        }

        assertEquals(numTasks, tracker.getCompletedCount());
        assertTrue(tracker.isComplete());
        assertTrue(tracker.getNumTasks() > 0);

        tracker.stop();
    }
}
