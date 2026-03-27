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

    // ── MinIO resilience — fileExists failure falls through to transcode ─────

    @Test
    void execute_proceedsWhenFileExistsThrows() {
        when(storageClient.fileExists(anyString()))
                .thenThrow(new RuntimeException("Connection refused"));
        // Fail first download attempt, then return an empty stream on the second.
        // Files.copy succeeds on the empty stream, then FFmpeg fails immediately
        // on the fake data — no retry sleeps are incurred beyond the first.
        when(storageClient.downloadFile(anyString()))
                .thenThrow(new RuntimeException("Connection refused"))
                .thenReturn(new java.io.ByteArrayInputStream(new byte[0]));

        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);

        // Should not throw the fileExists error; should attempt download
        assertThrows(Exception.class, () -> task.execute(storageClient));
        // Verify it got past fileExists and tried to download the source chunk
        verify(storageClient, atLeastOnce()).downloadFile(anyString());
    }

    @Test
    void transcodeToSpool_proceedsWhenFileExistsThrows() throws Exception {
        when(storageClient.fileExists(anyString()))
                .thenThrow(new RuntimeException("Connection refused"));
        // Fail first download, then return empty stream — avoids full retry backoff sleeps
        when(storageClient.downloadFile(anyString()))
                .thenThrow(new RuntimeException("Connection refused"))
                .thenReturn(new java.io.ByteArrayInputStream(new byte[0]));

        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);
        java.nio.file.Path spoolRoot = java.nio.file.Files.createTempDirectory("spool-test");
        try {
            assertThrows(Exception.class, () -> task.transcodeToSpool(storageClient, spoolRoot));
            // Key: it got past fileExists and tried to download
            verify(storageClient, atLeastOnce()).downloadFile(anyString());
        } finally {
            try (var walk = java.nio.file.Files.walk(spoolRoot)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> { try { java.nio.file.Files.deleteIfExists(p); } catch (Exception ignored) {} });
            }
        }
    }

    @Test
    void transcodeToSpool_returnsNullWhenFileExistsReturnsTrue() throws Exception {
        when(storageClient.fileExists(anyString())).thenReturn(true);

        TranscodingTask task = new TranscodingTask("t1", "vid1",
                "vid1/chunks/seg0.ts", TranscodingProfile.LOW);
        java.nio.file.Path spoolRoot = java.nio.file.Files.createTempDirectory("spool-test");
        try {
            TranscodingTask.CompletedTranscode result = task.transcodeToSpool(storageClient, spoolRoot);
            assertNull(result, "Should return null when output already exists in object storage");
            verify(storageClient, never()).downloadFile(anyString());
        } finally {
            java.nio.file.Files.deleteIfExists(spoolRoot);
        }
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
