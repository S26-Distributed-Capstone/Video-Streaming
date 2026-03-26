package com.distributed26.videostreaming.upload.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class StorageStateTrackerTest {

    @Test
    void transitionsVideoIntoWaitingAndBackToProcessing() {
        RecordingVideoUploadRepository repository = new RecordingVideoUploadRepository();
        StatusEventBus statusEventBus = new NoopStatusEventBus();
        StorageStateTracker tracker = new StorageStateTracker(repository, statusEventBus);

        tracker.beginStorageWait("video-1", "storage down");

        assertTrue(tracker.isVideoWaiting("video-1"));
        assertFalse(tracker.isServiceReady());
        assertTrue(repository.statusUpdates.contains("video-1:WAITING_FOR_STORAGE"));

        tracker.endStorageWait("video-1");

        assertFalse(tracker.isVideoWaiting("video-1"));
        assertTrue(tracker.isServiceReady());
        assertTrue(repository.statusUpdates.contains("video-1:PROCESSING"));
    }

    @Test
    void keepsWaitingUntilAllOutstandingOperationsFinish() {
        RecordingVideoUploadRepository repository = new RecordingVideoUploadRepository();
        StatusEventBus statusEventBus = new NoopStatusEventBus();
        StorageStateTracker tracker = new StorageStateTracker(repository, statusEventBus);

        tracker.beginStorageWait("video-1", "storage down");
        tracker.beginStorageWait("video-1", "storage still down");
        tracker.endStorageWait("video-1");

        assertTrue(tracker.isVideoWaiting("video-1"));
        assertFalse(tracker.isServiceReady());
        assertEquals(1, repository.countStatus("video-1:WAITING_FOR_STORAGE"));

        tracker.endStorageWait("video-1");

        assertFalse(tracker.isVideoWaiting("video-1"));
        assertTrue(tracker.isServiceReady());
        assertEquals(1, repository.countStatus("video-1:PROCESSING"));
    }

    private static final class RecordingVideoUploadRepository extends VideoUploadRepository {
        private final List<String> statusUpdates = new ArrayList<>();

        private RecordingVideoUploadRepository() {
            super("jdbc:unused", "unused", "unused");
        }

        @Override
        public void updateStatus(String videoId, String status) {
            statusUpdates.add(videoId + ":" + status);
        }

        private int countStatus(String entry) {
            return (int) statusUpdates.stream().filter(entry::equals).count();
        }
    }

    private static final class NoopStatusEventBus implements StatusEventBus {
        @Override
        public void publish(JobEvent event) {
        }

        @Override
        public void subscribe(String jobId, JobEventListener listener) {
        }

        @Override
        public void unsubscribe(String jobId, JobEventListener listener) {
        }
    }
}
