package com.distributed26.videostreaming.processing.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.distributed26.videostreaming.processing.LocalSpoolUploadTask;
import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the mid-upload crash recovery pipeline:
 *
 * <ol>
 *   <li>{@link StartupRecoveryService#recoverOrphanedSpoolFiles} scans the spool
 *       directory and creates PENDING upload tasks for orphaned files.</li>
 *   <li>{@link LocalSpoolUploadWorkerPool#uploadSpoolTask} picks up a PENDING task,
 *       reads the spool file, uploads it to object storage, and cleans up.</li>
 *   <li>The end-to-end test chains both: orphaned spool file → recovery creates
 *       task → upload worker uploads it → file lands in object storage.</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
class StartupRecoveryServiceTest {

    private static final TranscodingProfile[] PROFILES = {
            TranscodingProfile.LOW,
            TranscodingProfile.MEDIUM,
            TranscodingProfile.HIGH
    };

    @Mock private ObjectStorageClient storageClient;
    @Mock private ProcessingUploadTaskRepository uploadTaskRepo;
    @Mock private ProcessingTaskClaimRepository claimRepo;
    @Mock private TranscodedSegmentStatusRepository transcodeStatusRepo;
    @Mock private VideoProcessingRepository videoProcessingRepo;
    @Mock private StatusEventBus statusBus;
    @Mock private TranscodeTaskBus transcodeTaskBus;

    @TempDir
    Path spoolRoot;

    private ProcessingRuntime runtime;
    private StartupRecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        runtime = new ProcessingRuntime(
                transcodeStatusRepo,
                videoProcessingRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeTaskBus,
                null,   // manifestService
                null,   // manifestExecutor
                spoolRoot,
                "test-instance"
        );
        recoveryService = new StartupRecoveryService(PROFILES, runtime);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 1 — recoverOrphanedSpoolFiles
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    class SpoolRecovery {

        @Test
        void orphanedSpoolFile_createsUploadTask() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(false);
            when(storageClient.fileExists(videoId + "/processed/low/output0.ts")).thenReturn(false);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo).upsertPending(
                    eq(videoId), eq("low"), eq(0),
                    eq(videoId + "/chunks/output0.ts"),
                    eq(videoId + "/processed/low/output0.ts"),
                    eq(file.toAbsolutePath().toString()),
                    eq(Files.size(file)), anyDouble());
        }

        @Test
        void orphanedSpoolFiles_multipleProfiles_createsTaskForEach() throws Exception {
            String videoId = UUID.randomUUID().toString();
            createSpoolFile(videoId, "low", "output0.ts");
            createSpoolFile(videoId, "medium", "output0.ts");
            createSpoolFile(videoId, "high", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(anyString(), anyString(), anyInt())).thenReturn(false);
            when(storageClient.fileExists(anyString())).thenReturn(false);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo).upsertPending(eq(videoId), eq("low"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo).upsertPending(eq(videoId), eq("medium"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo).upsertPending(eq(videoId), eq("high"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void existingUploadTask_skipsCreation() throws Exception {
            String videoId = UUID.randomUUID().toString();
            createSpoolFile(videoId, "low", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(true);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void alreadyUploadedToObjectStorage_cleansUpSpoolFile() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(false);
            when(storageClient.fileExists(videoId + "/processed/low/output0.ts")).thenReturn(true);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            assertFalse(Files.exists(file));
            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(transcodeStatusRepo).upsertState(videoId, "low", 0, TranscodeSegmentState.DONE);
        }

        @Test
        void failedVideo_spoolDirectoryIsCleanedUp() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            runtime.markVideoFailed(videoId);
            when(videoProcessingRepo.isFailed(videoId)).thenReturn(true);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            assertFalse(Files.exists(file));
            assertFalse(Files.exists(spoolRoot.resolve(videoId)));
            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void partFiles_areCleaned() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path profileDir = spoolRoot.resolve(videoId).resolve("low");
            Files.createDirectories(profileDir);
            Path partFile = profileDir.resolve("output0.ts.part");
            Files.writeString(partFile, "incomplete data");

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            assertFalse(Files.exists(partFile));
        }

        @Test
        void nonUuidDirectory_isSkipped() throws Exception {
            Path notAVideo = spoolRoot.resolve("not-a-uuid").resolve("low");
            Files.createDirectories(notAVideo);
            Files.writeString(notAVideo.resolve("output0.ts"), "data");

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void unknownProfile_isSkipped() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path unknownProfileDir = spoolRoot.resolve(videoId).resolve("ultraHD");
            Files.createDirectories(unknownProfileDir);
            Files.writeString(unknownProfileDir.resolve("output0.ts"), "data");

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void emptySpool_doesNothing() {
            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(storageClient, never()).fileExists(anyString());
        }

        @Test
        void nullSpoolRoot_doesNotThrow() {
            recoveryService.recoverOrphanedSpoolFiles(storageClient, null);

            verify(uploadTaskRepo, never()).upsertPending(
                    anyString(), anyString(), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void multipleVideos_allRecovered() throws Exception {
            String videoA = UUID.randomUUID().toString();
            String videoB = UUID.randomUUID().toString();
            createSpoolFile(videoA, "low", "output0.ts");
            createSpoolFile(videoA, "low", "output1.ts");
            createSpoolFile(videoB, "high", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(anyString(), anyString(), anyInt())).thenReturn(false);
            when(storageClient.fileExists(anyString())).thenReturn(false);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo).upsertPending(eq(videoA), eq("low"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo).upsertPending(eq(videoA), eq("low"), eq(1), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo).upsertPending(eq(videoB), eq("high"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void mixedScenario_someOrphansAndSomeExisting() throws Exception {
            String videoId = UUID.randomUUID().toString();
            createSpoolFile(videoId, "low", "output0.ts");
            createSpoolFile(videoId, "medium", "output0.ts");
            createSpoolFile(videoId, "high", "output0.ts");

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(false);
            when(uploadTaskRepo.hasOpenTask(videoId, "medium", 0)).thenReturn(true);
            when(uploadTaskRepo.hasOpenTask(videoId, "high", 0)).thenReturn(false);
            when(storageClient.fileExists(videoId + "/processed/low/output0.ts")).thenReturn(false);
            when(storageClient.fileExists(videoId + "/processed/high/output0.ts")).thenReturn(true);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo, times(1)).upsertPending(
                    eq(videoId), eq("low"), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo, never()).upsertPending(
                    eq(videoId), eq("medium"), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
            verify(uploadTaskRepo, never()).upsertPending(
                    eq(videoId), eq("high"), anyInt(), anyString(), anyString(), anyString(), anyLong(), anyDouble());
        }

        @Test
        void noUploadTaskRepo_skipsRecovery() {
            ProcessingRuntime bareRuntime = new ProcessingRuntime(
                    null, null, null, null, null, null, null, null, null, "test"
            );
            StartupRecoveryService bareRecovery = new StartupRecoveryService(PROFILES, bareRuntime);

            bareRecovery.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(storageClient, never()).fileExists(anyString());
        }

        @Test
        void spoolFileSize_isPassedToUploadTask() throws Exception {
            String videoId = UUID.randomUUID().toString();
            String content = "x".repeat(12345);
            createSpoolFile(videoId, "low", "output0.ts", content);

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(false);
            when(storageClient.fileExists(anyString())).thenReturn(false);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo).upsertPending(
                    eq(videoId), eq("low"), eq(0), anyString(), anyString(), anyString(),
                    eq((long) content.length()), anyDouble());
        }
    }

    @Nested
    class IncompleteVideoRecovery {

        @Test
        void activeClaim_preventsRequeueForThatSegment() {
            String videoId = UUID.randomUUID().toString();

            when(videoProcessingRepo.findVideoIdsByStatus("PROCESSING")).thenReturn(List.of(videoId));
            when(storageClient.listFiles(videoId + "/chunks/")).thenReturn(List.of(
                    videoId + "/chunks/output0.ts",
                    videoId + "/chunks/output1.ts"
            ));
            when(transcodeStatusRepo.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(Set.of());
            when(transcodeStatusRepo.findSegmentNumbersByState(videoId, "medium", TranscodeSegmentState.DONE)).thenReturn(Set.of());
            when(transcodeStatusRepo.findSegmentNumbersByState(videoId, "high", TranscodeSegmentState.DONE)).thenReturn(Set.of());
            when(uploadTaskRepo.findOpenSegmentNumbers(videoId, "low")).thenReturn(Set.of());
            when(uploadTaskRepo.findOpenSegmentNumbers(videoId, "medium")).thenReturn(Set.of());
            when(uploadTaskRepo.findOpenSegmentNumbers(videoId, "high")).thenReturn(Set.of());
            when(claimRepo.findClaimedSegmentNumbers(videoId, "low", runtime.claimStaleMillis())).thenReturn(Set.of(0));
            when(claimRepo.findClaimedSegmentNumbers(videoId, "medium", runtime.claimStaleMillis())).thenReturn(Set.of());
            when(claimRepo.findClaimedSegmentNumbers(videoId, "high", runtime.claimStaleMillis())).thenReturn(Set.of());

            recoveryService.recoverIncompleteVideos(storageClient);

            verify(transcodeTaskBus, never()).publish(argThat((TranscodeTaskEvent ev) ->
                    videoId.equals(ev.getJobId())
                            && "low".equals(ev.getProfile())
                            && ev.getSegmentNumber() == 0
            ));
            verify(transcodeTaskBus).publish(argThat((TranscodeTaskEvent ev) ->
                    videoId.equals(ev.getJobId())
                            && "low".equals(ev.getProfile())
                            && ev.getSegmentNumber() == 1
            ));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 2 — Upload worker picks up the PENDING task and uploads it
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    class UploadWorker {

        private LocalSpoolUploadWorkerPool uploadWorkerPool;

        @BeforeEach
        void setUpWorker() {
            uploadWorkerPool = new LocalSpoolUploadWorkerPool(PROFILES, runtime);
        }

        @Test
        void recoveredTask_uploadsFileToObjectStorage() throws Exception {
            String videoId = UUID.randomUUID().toString();
            String content = "recovered segment data";
            Path file = createSpoolFile(videoId, "low", "output0.ts", content);

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    1L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    content.length(), 0d, 1
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            // The file should have been uploaded to object storage
            verify(storageClient).uploadFile(eq(task.outputKey()), any(InputStream.class), eq((long) content.length()));
            // Spool file should be cleaned up
            assertFalse(Files.exists(file), "Spool file should be deleted after successful upload");
            // Task should be deleted from the DB
            verify(uploadTaskRepo).deleteById(1L);
            // Should publish DONE state
            verify(transcodeStatusRepo).upsertState(videoId, "low", 0, TranscodeSegmentState.DONE);
        }

        @Test
        void recoveredTask_storageFailurePublishesWaitingAndRecovery() throws Exception {
            String videoId = UUID.randomUUID().toString();
            String content = "recovered segment data";
            Path file = createSpoolFile(videoId, "low", "output0.ts", content);

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    11L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    content.length(), 0d, 1
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);
            doThrow(new RuntimeException("MinIO down"))
                    .doNothing()
                    .when(storageClient).uploadFile(eq(task.outputKey()), any(InputStream.class), eq((long) content.length()));

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            verify(uploadTaskRepo).markPending(11L);
            verify(claimRepo, atLeastOnce()).release(videoId, "low", 0);
            verify(videoProcessingRepo).updateStatus(videoId, "WAITING_FOR_STORAGE");
            verify(statusBus).publish(argThat(event ->
                    event instanceof UploadStorageStatusEvent storageEvent
                            && videoId.equals(storageEvent.getJobId())
                            && "WAITING".equals(storageEvent.getState())
            ));

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            verify(videoProcessingRepo).updateStatus(videoId, "PROCESSING");
            verify(statusBus).publish(argThat(event ->
                    event instanceof UploadStorageStatusEvent storageEvent
                            && videoId.equals(storageEvent.getJobId())
                            && "AVAILABLE".equals(storageEvent.getState())
            ));
            verify(uploadTaskRepo).deleteById(11L);
            assertFalse(Files.exists(file), "Spool file should be deleted after recovery upload succeeds");
        }

        @Test
        void recoveredTask_alreadyInStorage_cleansUpWithoutReUpload() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    2L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    100L, 0d, 1
            );

            // Already in storage (uploaded by another instance before this one recovered)
            when(storageClient.fileExists(task.outputKey())).thenReturn(true);

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            verify(storageClient, never()).uploadFile(anyString(), any(InputStream.class), anyLong());
            assertFalse(Files.exists(file), "Spool file should still be cleaned up");
            verify(uploadTaskRepo).deleteById(2L);
        }

        @Test
        void recoveredTask_spoolFileMissing_requeuesTranscode() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path missingFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
            // Note: do NOT create the file — simulates a lost spool file

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    3L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    missingFile.toAbsolutePath().toString(),
                    100L, 0d, 1
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            // Should not attempt upload
            verify(storageClient, never()).uploadFile(anyString(), any(InputStream.class), anyLong());
            // Should delete the stale DB row
            verify(uploadTaskRepo).deleteById(3L);
            // Should re-publish a transcode task so the segment gets re-transcoded
            verify(transcodeTaskBus).publish(argThat(ev ->
                    videoId.equals(ev.getJobId())
                            && "low".equals(ev.getProfile())
                            && ev.getSegmentNumber() == 0
            ));
        }

        @Test
        void recoveredTask_forFailedVideo_discardsWithoutUpload() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    4L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    100L, 0d, 1
            );

            runtime.markVideoFailed(videoId);
            when(videoProcessingRepo.isFailed(videoId)).thenReturn(true);

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            verify(storageClient, never()).uploadFile(anyString(), any(InputStream.class), anyLong());
            assertFalse(Files.exists(file), "Spool file should be discarded for failed videos");
            verify(uploadTaskRepo).deleteById(4L);
        }

        @Test
        void recoveredTask_uploadFails_markedPendingForRetry() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    5L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    100L, 0d, 1
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);
            doThrow(new RuntimeException("network error"))
                    .when(storageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            // Task should be marked PENDING again for retry
            verify(uploadTaskRepo).markPending(5L);
            // Spool file should be kept for the retry
            assertTrue(Files.exists(file), "Spool file should be kept when upload fails so it can be retried");
            // Should NOT be deleted or marked done
            verify(uploadTaskRepo, never()).deleteById(5L);
        }

        @Test
        void recoveredTask_publishesUploadingStateDuringUpload() throws Exception {
            String videoId = UUID.randomUUID().toString();
            Path file = createSpoolFile(videoId, "low", "output0.ts");

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    6L, videoId, "low", 0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    file.toAbsolutePath().toString(),
                    100L, 0d, 1
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);

            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            // Should transition through UPLOADING → DONE
            var stateCaptor = ArgumentCaptor.forClass(TranscodeSegmentState.class);
            verify(transcodeStatusRepo, atLeast(2))
                    .upsertState(eq(videoId), eq("low"), eq(0), stateCaptor.capture());

            var states = stateCaptor.getAllValues();
            assertTrue(states.contains(TranscodeSegmentState.UPLOADING),
                    "Should publish UPLOADING state during upload");
            assertTrue(states.contains(TranscodeSegmentState.DONE),
                    "Should publish DONE state after upload");
            int uploadingIdx = states.indexOf(TranscodeSegmentState.UPLOADING);
            int doneIdx = states.lastIndexOf(TranscodeSegmentState.DONE);
            assertTrue(uploadingIdx < doneIdx,
                    "UPLOADING should come before DONE");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // End-to-end: spool file → recovery → upload worker → object storage
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    class EndToEnd {

        @Test
        void fullRecoveryPipeline_orphanedFile_endsUpInObjectStorage() throws Exception {
            // ── Arrange: processing-service crashed between transcodeToSpool and
            //    upsertPending, leaving only a file on disk ──────────────────────
            String videoId = UUID.randomUUID().toString();
            String segmentContent = "transcoded-segment-bytes-here";
            Path spoolFile = createSpoolFile(videoId, "low", "output0.ts", segmentContent);

            when(uploadTaskRepo.hasOpenTask(videoId, "low", 0)).thenReturn(false);
            when(storageClient.fileExists(videoId + "/processed/low/output0.ts")).thenReturn(false);

            // ── Phase 1: recovery scans spool and creates a PENDING upload task ─
            //
            // Capture the upsertPending call so we can build the LocalSpoolUploadTask
            // exactly as the real upload worker would receive it from claimNextReady.
            var videoIdCaptor  = ArgumentCaptor.forClass(String.class);
            var profileCaptor  = ArgumentCaptor.forClass(String.class);
            var segNumCaptor   = ArgumentCaptor.forClass(Integer.class);
            var chunkCaptor    = ArgumentCaptor.forClass(String.class);
            var outputCaptor   = ArgumentCaptor.forClass(String.class);
            var pathCaptor     = ArgumentCaptor.forClass(String.class);
            var sizeCaptor     = ArgumentCaptor.forClass(Long.class);
            var offsetCaptor   = ArgumentCaptor.forClass(Double.class);

            recoveryService.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepo).upsertPending(
                    videoIdCaptor.capture(), profileCaptor.capture(), segNumCaptor.capture(),
                    chunkCaptor.capture(), outputCaptor.capture(), pathCaptor.capture(),
                    sizeCaptor.capture(), offsetCaptor.capture());

            // ── Phase 2: simulate the upload worker claiming and processing it ──
            LocalSpoolUploadTask claimed = new LocalSpoolUploadTask(
                    42L,
                    videoIdCaptor.getValue(),
                    profileCaptor.getValue(),
                    segNumCaptor.getValue(),
                    chunkCaptor.getValue(),
                    outputCaptor.getValue(),
                    pathCaptor.getValue(),
                    sizeCaptor.getValue(),
                    offsetCaptor.getValue(),
                    1 // attemptCount
            );

            LocalSpoolUploadWorkerPool uploadWorkerPool = new LocalSpoolUploadWorkerPool(PROFILES, runtime);
            uploadWorkerPool.uploadSpoolTask(claimed, storageClient);

            // ── Assert: file is in object storage and spool is clean ────────────

            // The file was uploaded to the correct key with the right size
            verify(storageClient).uploadFile(
                    eq(videoId + "/processed/low/output0.ts"),
                    any(InputStream.class),
                    eq((long) segmentContent.length()));

            // The spool file was cleaned up
            assertFalse(Files.exists(spoolFile),
                    "Spool file should be deleted after successful upload");

            // The DB task row was deleted
            verify(uploadTaskRepo).deleteById(42L);

            // Final state is DONE
            verify(transcodeStatusRepo).upsertState(
                    videoId, "low", 0, TranscodeSegmentState.DONE);
        }

        @Test
        void fullRecoveryPipeline_resetUploadingTask_endsUpInObjectStorage() throws Exception {
            // ── Arrange: processing-service crashed mid-upload.
            //    The processing_upload_task row was in UPLOADING state.
            //    On startup, resetUploadingTasks() would set it back to PENDING.
            //    Then the upload worker picks it up.
            String videoId = UUID.randomUUID().toString();
            String content = "segment-data-mid-upload-crash";
            Path spoolFile = createSpoolFile(videoId, "medium", "output3.ts", content);

            LocalSpoolUploadTask task = new LocalSpoolUploadTask(
                    99L, videoId, "medium", 3,
                    videoId + "/chunks/output3.ts",
                    videoId + "/processed/medium/output3.ts",
                    spoolFile.toAbsolutePath().toString(),
                    content.length(), 30.0d, 2
            );

            when(storageClient.fileExists(task.outputKey())).thenReturn(false);

            LocalSpoolUploadWorkerPool uploadWorkerPool = new LocalSpoolUploadWorkerPool(PROFILES, runtime);
            uploadWorkerPool.uploadSpoolTask(task, storageClient);

            verify(storageClient).uploadFile(
                    eq(videoId + "/processed/medium/output3.ts"),
                    any(InputStream.class),
                    eq((long) content.length()));
            assertFalse(Files.exists(spoolFile));
            verify(uploadTaskRepo).deleteById(99L);
            verify(transcodeStatusRepo).upsertState(videoId, "medium", 3, TranscodeSegmentState.DONE);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private Path createSpoolFile(String videoId, String profile, String fileName) throws IOException {
        return createSpoolFile(videoId, profile, fileName, "fake transcoded segment data");
    }

    private Path createSpoolFile(String videoId, String profile, String fileName, String content) throws IOException {
        Path dir = spoolRoot.resolve(videoId).resolve(profile);
        Files.createDirectories(dir);
        Path file = dir.resolve(fileName);
        Files.writeString(file, content);
        return file;
    }
}
