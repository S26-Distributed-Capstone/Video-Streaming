package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.upload.RabbitMQJobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingServiceApplicationTest {

    @Mock
    private RabbitMQJobTaskBus bus;

    private BlockingQueue<TranscodingTask> taskQueue;

    @BeforeEach
    void setUp() {
        taskQueue = new LinkedBlockingQueue<>();
        ProcessingServiceApplication.resetState();
    }

    // ── Task count ─────────────────────────────────────────────────────────────

    @Test
    void oneChunk_threeProfies_queues3Tasks() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);

        assertEquals(3, taskQueue.size());
    }

    @Test
    void twoChunks_threeProfiles_queues6Tasks() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");
        sendMeta("vid1", 2);

        assertEquals(6, taskQueue.size());
    }

    @Test
    void taskCount_isChunksTimesProfiles() {
        int chunkCount = 5;
        String[] keys = new String[chunkCount];
        for (int i = 0; i < chunkCount; i++) {
            keys[i] = "vid1/chunks/seg" + i + ".ts";
        }
        sendChunks("vid1", keys);
        sendMeta("vid1", chunkCount);

        assertEquals(chunkCount * ProcessingServiceApplication.PROFILES.length, taskQueue.size());
    }

    // ── Ordering ───────────────────────────────────────────────────────────────

    @Test
    void metaBeforeChunks_tasksQueuedWhenLastChunkArrives() {
        sendMeta("vid1", 2);
        assertEquals(0, taskQueue.size(), "Should not queue before chunks arrive");

        sendChunks("vid1", "vid1/chunks/seg0.ts");
        assertEquals(0, taskQueue.size(), "Should not queue until all chunks present");

        sendChunks("vid1", "vid1/chunks/seg1.ts");
        assertEquals(6, taskQueue.size());
    }

    @Test
    void chunksBeforeMeta_tasksQueuedOnMetaEvent() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");
        assertEquals(0, taskQueue.size(), "Should not queue before meta arrives");

        sendMeta("vid1", 2);
        assertEquals(6, taskQueue.size());
    }

    // ── Task properties ────────────────────────────────────────────────────────

    @Test
    void queuedTasks_haveCreatedStatus() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);

        for (TranscodingTask t : taskQueue) {
            assertEquals(Status.CREATED, t.getStatus());
        }
    }

    @Test
    void queuedTasks_allThreeProfilesCovered() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);

        boolean hasLow    = taskQueue.stream().anyMatch(t -> t.getProfile() == TranscodingProfile.LOW);
        boolean hasMedium = taskQueue.stream().anyMatch(t -> t.getProfile() == TranscodingProfile.MEDIUM);
        boolean hasHigh   = taskQueue.stream().anyMatch(t -> t.getProfile() == TranscodingProfile.HIGH);

        assertTrue(hasLow,    "Expected a LOW profile task");
        assertTrue(hasMedium, "Expected a MEDIUM profile task");
        assertTrue(hasHigh,   "Expected a HIGH profile task");
    }

    @Test
    void queuedTasks_haveCorrectJobId() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);

        for (TranscodingTask t : taskQueue) {
            assertEquals("vid1", t.getJobId());
        }
    }

    @Test
    void queuedTasks_outputKeysAreUnderProcessedPath() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);

        for (TranscodingTask t : taskQueue) {
            assertTrue(t.getOutputKey().startsWith("vid1/processed/"),
                    "Expected output under vid1/processed/ but was: " + t.getOutputKey());
        }
    }

    @Test
    void queuedTasks_haveUniqueIds() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");
        sendMeta("vid1", 2);

        long distinctIds = taskQueue.stream().map(TranscodingTask::getId).distinct().count();
        assertEquals(taskQueue.size(), distinctIds);
    }

    // ── Multiple videos ────────────────────────────────────────────────────────

    @Test
    void twoVideos_processedIndependently() {
        sendChunks("vidA", "vidA/chunks/seg0.ts");
        sendChunks("vidB", "vidB/chunks/seg0.ts", "vidB/chunks/seg1.ts");

        sendMeta("vidA", 1); // vidA complete: 1×3 = 3 tasks
        assertEquals(3, taskQueue.size());

        sendMeta("vidB", 2); // vidB complete: 2×3 = 6 more tasks
        assertEquals(9, taskQueue.size());
    }

    @Test
    void twoVideos_tasksHaveCorrectJobIds() {
        sendChunks("vidA", "vidA/chunks/seg0.ts");
        sendMeta("vidA", 1);
        sendChunks("vidB", "vidB/chunks/seg0.ts");
        sendMeta("vidB", 1);

        long forA = taskQueue.stream().filter(t -> "vidA".equals(t.getJobId())).count();
        long forB = taskQueue.stream().filter(t -> "vidB".equals(t.getJobId())).count();
        assertEquals(3, forA);
        assertEquals(3, forB);
    }

    // ── No premature queuing ───────────────────────────────────────────────────

    @Test
    void partialChunks_noTasksQueued() {
        sendMeta("vid1", 3);
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");

        assertEquals(0, taskQueue.size(), "Should not queue until all 3 chunks present");
    }

    @Test
    void metaOnly_noTasksQueued() {
        sendMeta("vid1", 2);
        assertEquals(0, taskQueue.size());
    }

    @Test
    void chunksOnly_noTasksQueued() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");
        assertEquals(0, taskQueue.size());
    }

    // ── Idempotency / duplicates ───────────────────────────────────────────────

    @Test
    void duplicateChunkKey_countedOnce() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendChunks("vid1", "vid1/chunks/seg0.ts"); // duplicate
        sendMeta("vid1", 1);

        // Only 1 unique chunk → 3 tasks, not 6
        assertEquals(3, taskQueue.size());
    }

    @Test
    void lateMetaAfterAlreadyQueued_doesNotRequeue() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendMeta("vid1", 1);
        assertEquals(3, taskQueue.size());

        // A spurious second meta event should not requeue
        sendMeta("vid1", 1);
        assertEquals(3, taskQueue.size());
    }

    // ── Bus subscription ───────────────────────────────────────────────────────

    @Test
    void firstChunkEvent_subscribesListenerForVideoId() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");

        verify(bus, times(1)).subscribe(eq("vid1"), any());
    }

    @Test
    void secondChunkEvent_doesNotResubscribe() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");

        // subscribe called exactly once for the videoId
        verify(bus, times(1)).subscribe(eq("vid1"), any());
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void sendChunks(String videoId, String... chunkKeys) {
        for (String key : chunkKeys) {
            ProcessingServiceApplication.onEvent(new JobTaskEvent(videoId, key), taskQueue, bus);
        }
    }

    private void sendMeta(String videoId, int totalSegments) {
        ProcessingServiceApplication.onEvent(new UploadMetaEvent(videoId, totalSegments), taskQueue, bus);
    }
}
