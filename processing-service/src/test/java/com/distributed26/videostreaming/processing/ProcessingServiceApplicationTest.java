package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
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
    private StatusEventBus bus;
    @Mock
    private TranscodedSegmentStatusRepository transcodeStatusRepository;
    @Mock
    private ProcessingUploadTaskRepository processingUploadTaskRepository;
    @Mock
    private ProcessingTaskClaimRepository processingTaskClaimRepository;

    private BlockingQueue<TranscodingTask> taskQueue;

    @BeforeEach
    void setUp() {
        taskQueue = new LinkedBlockingQueue<>();
        ProcessingServiceApplication.resetState();
    }

    // ── Task count ─────────────────────────────────────────────────────────────

    @Test
    void oneSourceChunk_enqueuesThreeProfileTasks() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");

        assertEquals(3, taskQueue.size());
    }

    @Test
    void twoSourceChunks_enqueueSixProfileTasks() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");

        assertEquals(6, taskQueue.size());
    }

    @Test
    void taskCount_isSourceChunksTimesProfiles() {
        int chunkCount = 5;
        String[] keys = new String[chunkCount];
        for (int i = 0; i < chunkCount; i++) {
            keys[i] = "vid1/chunks/seg" + i + ".ts";
        }
        sendChunks("vid1", keys);

        assertEquals(chunkCount * ProcessingServiceApplication.PROFILES.length, taskQueue.size());
    }

    // ── Ordering: tasks are queued immediately, not on meta ───────────────────

    @Test
    void transcodeTaskMessages_immediatelyQueueThreeProfileTasks() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        assertEquals(3, taskQueue.size(), "Profile tasks should be queued as soon as task messages arrive");

        sendChunks("vid1", "vid1/chunks/seg1.ts");
        assertEquals(6, taskQueue.size(), "Each source chunk independently adds three profile tasks");
    }

    @Test
    void metaEvent_doesNotQueueTranscodeTasks() {
        sendMeta("vid1", 2);
        assertEquals(0, taskQueue.size(), "UploadMetaEvent alone queues nothing");

        sendMeta("vid1", 2); // second meta also ignored
        assertEquals(0, taskQueue.size());
    }

    @Test
    void queuedTasksRemainUnchangedWhenMetaArrives() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");
        assertEquals(6, taskQueue.size());

        sendMeta("vid1", 2); // meta arrives after chunks — should change nothing
        assertEquals(6, taskQueue.size());
    }

    // ── Task properties ────────────────────────────────────────────────────────

    @Test
    void queuedTasks_haveCreatedStatus() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");

        for (TranscodingTask t : taskQueue) {
            assertEquals(Status.CREATED, t.getStatus());
        }
    }

    @Test
    void queuedTasks_allThreeProfilesCovered() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");

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

        for (TranscodingTask t : taskQueue) {
            assertEquals("vid1", t.getJobId());
        }
    }

    @Test
    void queuedTasks_outputKeysAreUnderProcessedPath() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");

        for (TranscodingTask t : taskQueue) {
            assertTrue(t.getOutputKey().startsWith("vid1/processed/"),
                    "Expected output under vid1/processed/ but was: " + t.getOutputKey());
        }
    }

    @Test
    void queuedTasks_haveUniqueIds() {
        sendChunks("vid1", "vid1/chunks/seg0.ts", "vid1/chunks/seg1.ts");

        long distinctIds = taskQueue.stream().map(TranscodingTask::getId).distinct().count();
        assertEquals(taskQueue.size(), distinctIds);
    }

    // ── Multiple videos ────────────────────────────────────────────────────────

    @Test
    void twoVideos_enqueueTasksIndependently() {
        sendChunks("vidA", "vidA/chunks/seg0.ts");
        assertEquals(3, taskQueue.size());

        sendChunks("vidB", "vidB/chunks/seg0.ts", "vidB/chunks/seg1.ts");
        assertEquals(9, taskQueue.size());
    }

    @Test
    void twoVideos_tasksHaveCorrectJobIds() {
        sendChunks("vidA", "vidA/chunks/seg0.ts");
        sendChunks("vidB", "vidB/chunks/seg0.ts");

        long forA = taskQueue.stream().filter(t -> "vidA".equals(t.getJobId())).count();
        long forB = taskQueue.stream().filter(t -> "vidB".equals(t.getJobId())).count();
        assertEquals(3, forA);
        assertEquals(3, forB);
    }

    // ── Task message semantics ────────────────────────────────────────────────

    @Test
    void duplicateTranscodeTask_isQueuedAgain() {
        sendChunks("vid1", "vid1/chunks/seg0.ts");
        sendChunks("vid1", "vid1/chunks/seg0.ts"); // duplicate publish / retry

        // Processing now consumes one task message at a time and does not dedupe retries.
        assertEquals(6, taskQueue.size());
    }

    @Test
    void alreadyDoneSegments_areSkipped() {
        ProcessingServiceApplication.runtime().setTranscodeStatusRepository(transcodeStatusRepository);
        ProcessingServiceApplication.runtime().setStatusBus(bus);
        when(transcodeStatusRepository.hasState("vid1", "low", 0, TranscodeSegmentState.DONE)).thenReturn(true);
        when(transcodeStatusRepository.hasState("vid1", "medium", 0, TranscodeSegmentState.DONE)).thenReturn(false);
        when(transcodeStatusRepository.hasState("vid1", "high", 0, TranscodeSegmentState.DONE)).thenReturn(false);
        when(transcodeStatusRepository.countByState(any(), any(), any())).thenReturn(0);

        sendChunks("vid1", "vid1/chunks/seg0.ts");

        assertEquals(2, taskQueue.size(), "One profile should be skipped because it's already DONE");
        verify(transcodeStatusRepository).hasState("vid1", "low", 0, TranscodeSegmentState.DONE);
        verify(transcodeStatusRepository).hasState("vid1", "medium", 0, TranscodeSegmentState.DONE);
        verify(transcodeStatusRepository).hasState("vid1", "high", 0, TranscodeSegmentState.DONE);
        verify(bus).publish(argThat(ev ->
                ev instanceof com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent t
                        && "vid1".equals(t.getJobId())
                        && "low".equals(t.getProfile())
                        && t.getSegmentNumber() == 0
                        && t.getState() == TranscodeSegmentState.DONE
        ));
    }

    @Test
    void openLocalUploadTask_isSkipped() {
        ProcessingServiceApplication.runtime().setProcessingUploadTaskRepository(processingUploadTaskRepository);
        when(processingUploadTaskRepository.hasOpenTask("vid1", "low", 0)).thenReturn(true);
        when(processingUploadTaskRepository.hasOpenTask("vid1", "medium", 0)).thenReturn(false);
        when(processingUploadTaskRepository.hasOpenTask("vid1", "high", 0)).thenReturn(false);

        sendChunks("vid1", "vid1/chunks/seg0.ts");

        assertEquals(2, taskQueue.size(), "One profile should be skipped because a local upload task already exists");
        verify(processingUploadTaskRepository).hasOpenTask("vid1", "low", 0);
        verify(processingUploadTaskRepository).hasOpenTask("vid1", "medium", 0);
        verify(processingUploadTaskRepository).hasOpenTask("vid1", "high", 0);
    }

    @Test
    void failedVideo_doesNotQueueMoreTranscodeTasks() {
        ProcessingServiceApplication.runtime().markVideoFailed("vid1");

        sendChunks("vid1", "vid1/chunks/seg0.ts");

        assertEquals(0, taskQueue.size(), "Failed videos should not enqueue more transcode work");
    }

    @Test
    void activeClaim_isSkipped() {
        ProcessingServiceApplication.runtime().setProcessingTaskClaimRepository(processingTaskClaimRepository);
        when(processingTaskClaimRepository.hasActiveClaim("vid1", "low", 0, 60000L)).thenReturn(true);
        when(processingTaskClaimRepository.hasActiveClaim("vid1", "medium", 0, 60000L)).thenReturn(false);
        when(processingTaskClaimRepository.hasActiveClaim("vid1", "high", 0, 60000L)).thenReturn(false);

        sendChunks("vid1", "vid1/chunks/seg0.ts");

        assertEquals(2, taskQueue.size(), "One profile should be skipped because another instance already claimed it");
        verify(processingTaskClaimRepository).hasActiveClaim("vid1", "low", 0, 60000L);
        verify(processingTaskClaimRepository).hasActiveClaim("vid1", "medium", 0, 60000L);
        verify(processingTaskClaimRepository).hasActiveClaim("vid1", "high", 0, 60000L);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void sendChunks(String videoId, String... chunkKeys) {
        for (String key : chunkKeys) {
            int segmentNumber = extractSegmentNumber(key);
            enqueueIfPresent(new TranscodeTaskEvent(videoId, key, "low", segmentNumber));
            enqueueIfPresent(new TranscodeTaskEvent(videoId, key, "medium", segmentNumber));
            enqueueIfPresent(new TranscodeTaskEvent(videoId, key, "high", segmentNumber));
        }
    }

    private void enqueueIfPresent(TranscodeTaskEvent event) {
        TranscodingTask task = ProcessingServiceApplication.onTranscodeTaskEvent(event);
        if (task != null) {
            taskQueue.offer(task);
        }
    }

    private void sendMeta(String videoId, int totalSegments) {
        ProcessingServiceApplication.onStatusEvent(new UploadMetaEvent(videoId, totalSegments), null, null);
    }

    private static int extractSegmentNumber(String chunkKey) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(\\d+)").matcher(chunkKey);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last;
    }

}
