package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.jobs.TaskType;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TranscodingTaskTest {

    @Mock
    private ObjectStorageClient storageClient;

    // ── Construction & metadata ────────────────────────────────────────────────

    @Test
    void constructor_setsExpectedFields() {
        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);

        assertEquals("t1", task.getId());
        assertEquals("vid1", task.getJobId());
        assertEquals(TaskType.TRANSCODE, task.getType());
        assertEquals(TranscodingProfile.LOW, task.getProfile());
        assertEquals("vid1/chunks/seg0.ts", task.getChunkKey());
        assertEquals(Status.CREATED, task.getStatus());
        assertEquals(0, task.getAttempt());
        assertEquals(3, task.getMaxAttempts());
    }

    // ── Output key derivation ──────────────────────────────────────────────────

    @Test
    void outputKey_low_isCorrect() {
        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);
        assertEquals("vid1/processed/low/seg0.ts", task.getOutputKey());
    }

    @Test
    void outputKey_medium_isCorrect() {
        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.MEDIUM);
        assertEquals("vid1/processed/medium/seg0.ts", task.getOutputKey());
    }

    @Test
    void outputKey_high_isCorrect() {
        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.HIGH);
        assertEquals("vid1/processed/high/seg0.ts", task.getOutputKey());
    }

    @Test
    void outputKey_preservesFileName() {
        TranscodingTask task = new TranscodingTask("t1", "abc-123",
                "abc-123/chunks/output_007.ts", TranscodingProfile.MEDIUM);
        assertEquals("abc-123/processed/medium/output_007.ts", task.getOutputKey());
    }

    @Test
    void outputKey_differentVideoIds_doNotCrossContaminate() {
        TranscodingTask a = new TranscodingTask("t1", "videoA", "videoA/chunks/seg0.ts", TranscodingProfile.LOW);
        TranscodingTask b = new TranscodingTask("t2", "videoB", "videoB/chunks/seg0.ts", TranscodingProfile.LOW);
        assertTrue(a.getOutputKey().startsWith("videoA/"));
        assertTrue(b.getOutputKey().startsWith("videoB/"));
    }

    // ── execute() idempotency ──────────────────────────────────────────────────

    @Test
    void execute_skipsWhenOutputAlreadyExists() throws Exception {
        when(storageClient.fileExists(anyString())).thenReturn(true);

        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);
        task.execute(storageClient);

        // fileExists checked once, nothing else touched
        verify(storageClient, times(1)).fileExists("vid1/processed/low/seg0.ts");
        verify(storageClient, never()).downloadFile(anyString());
        verify(storageClient, never()).uploadFile(anyString(), any(), anyLong());
    }

    // ── Status transitions ─────────────────────────────────────────────────────

    @Test
    void setStatus_updatesStatus() {
        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);
        assertEquals(Status.CREATED, task.getStatus());
        task.setStatus(Status.RUNNING);
        assertEquals(Status.RUNNING, task.getStatus());
        task.setStatus(Status.SUCCEEDED);
        assertEquals(Status.SUCCEEDED, task.getStatus());
    }
}
