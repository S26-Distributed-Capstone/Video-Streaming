package com.distributed26.videostreaming.processing.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProcessingStorageStateTrackerTest {

    @Test
    void transitionsVideoIntoWaitingAndBackToProcessing() {
        RecordingVideoProcessingRepository repository = new RecordingVideoProcessingRepository();
        RecordingStatusEventBus statusEventBus = new RecordingStatusEventBus();
        ProcessingStorageStateTracker tracker = new ProcessingStorageStateTracker(repository, statusEventBus, null);

        tracker.beginStorageWait("video-1", "storage down");

        assertTrue(tracker.isVideoWaiting("video-1"));
        assertFalse(tracker.isServiceReady());
        assertTrue(repository.statusUpdates.contains("video-1:WAITING_FOR_STORAGE"));
        assertEquals("WAITING", statusEventBus.states.get(0));

        tracker.endStorageWait("video-1");

        assertFalse(tracker.isVideoWaiting("video-1"));
        assertTrue(tracker.isServiceReady());
        assertTrue(repository.statusUpdates.contains("video-1:PROCESSING"));
        assertEquals("AVAILABLE", statusEventBus.states.get(1));
    }

    @Test
    void repeatedWaitSignalsForSameVideoDoNotAccumulate() {
        RecordingVideoProcessingRepository repository = new RecordingVideoProcessingRepository();
        RecordingStatusEventBus statusEventBus = new RecordingStatusEventBus();
        ProcessingStorageStateTracker tracker = new ProcessingStorageStateTracker(repository, statusEventBus, null);

        tracker.beginStorageWait("video-1", "storage down");
        tracker.beginStorageWait("video-1", "storage still down");
        tracker.endStorageWait("video-1");

        assertFalse(tracker.isVideoWaiting("video-1"));
        assertTrue(tracker.isServiceReady());
        assertEquals(1, repository.countStatus("video-1:WAITING_FOR_STORAGE"));
        assertEquals(1, statusEventBus.states.stream().filter("WAITING"::equals).count());
        assertEquals(1, repository.countStatus("video-1:PROCESSING"));
        assertEquals(1, statusEventBus.states.stream().filter("AVAILABLE"::equals).count());
    }

    private static final class RecordingVideoProcessingRepository extends VideoProcessingRepository {
        private final List<String> statusUpdates = new ArrayList<>();

        private RecordingVideoProcessingRepository() {
            super("jdbc:unused", "unused", "unused");
        }

        @Override
        public void updateStatus(String videoId, String status) {
            statusUpdates.add(videoId + ":" + status);
        }

        @Override
        public boolean markProcessingIfPending(String videoId) {
            statusUpdates.add(videoId + ":PROCESSING");
            return true;
        }

        private int countStatus(String entry) {
            return (int) statusUpdates.stream().filter(entry::equals).count();
        }
    }

    private static final class RecordingStatusEventBus implements StatusEventBus {
        private final List<String> states = new ArrayList<>();

        @Override
        public void publish(JobEvent event) {
            states.add(((com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent) event).getState());
        }

        @Override
        public void subscribe(String jobId, JobEventListener listener) {
        }

        @Override
        public void unsubscribe(String jobId, JobEventListener listener) {
        }
    }
}
