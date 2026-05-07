package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.processing.runtime.LocalSpoolUploadWorkerPool;
import com.distributed26.videostreaming.processing.runtime.ProcessingRuntime;
import com.distributed26.videostreaming.processing.runtime.StartupRecoveryService;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskListener;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import io.github.cdimascio.dotenv.Dotenv;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class ProcessingServiceIntegrationTest {
    private String jdbcUrl;
    private String username;
    private String password;

    @AfterEach
    void tearDown() {
        // per-test cleanup is handled explicitly once videoId is known
    }

    @Test
    void startupRecoveryRequeuesFailedTranscodeAndDownloadsSourceAgain() throws Exception {
        assumeFfmpegAvailable();
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";
        Path validChunk = createValidTransportStreamChunk();
        byte[] validChunkBytes = Files.readAllBytes(validChunk);
        Files.deleteIfExists(validChunk);

        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-spool-it-");
        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "processing-it",
                1
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime,
                false,
                1
        );
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()
        );
        CountingStorageClient storageClient = new CountingStorageClient(chunkKey, validChunkBytes);

        try {
            TranscodeTaskEvent originalEvent =
                    new TranscodeTaskEvent(videoId, chunkKey, TranscodingProfile.LOW.getName(), 0, 0d);

            boolean firstAttempt = runtime.submitTranscodeTask(
                    originalEvent,
                    executor,
                    storageClient,
                    new ConcurrentHashMap<Thread, Worker>(),
                    new TranscodingProfile[]{TranscodingProfile.LOW}
            ).toCompletableFuture().get(60, TimeUnit.SECONDS);

            assertFalse(firstAttempt, "first attempt should fail during transcoding");
            assertEquals(1, storageClient.downloadAttempts(chunkKey),
                    "first attempt should download the source chunk once");
            assertTrue(transcodeRepo.hasState(videoId, "low", 0, TranscodeSegmentState.FAILED),
                    "failed attempt should persist FAILED transcode state");
            assertFalse(uploadTaskRepo.hasOpenTask(videoId, "low", 0),
                    "failed attempt should not create a local upload task");

            recovery.recoverIncompleteVideos(storageClient);

            TranscodeTaskEvent republished = transcodeBus.awaitPublished(videoId, "low", 0, 5, TimeUnit.SECONDS);
            assertNotNull(republished, "startup recovery should republish the missing transcode task");
            assertEquals(chunkKey, republished.getChunkKey());

            boolean secondAttempt = runtime.submitTranscodeTask(
                    republished,
                    executor,
                    storageClient,
                    new ConcurrentHashMap<Thread, Worker>(),
                    new TranscodingProfile[]{TranscodingProfile.LOW}
            ).toCompletableFuture().get(60, TimeUnit.SECONDS);

            assertTrue(secondAttempt, "republished task should succeed on retry");
            assertEquals(2, storageClient.downloadAttempts(chunkKey),
                    "retry should download the source chunk again from storage");
            assertTrue(uploadTaskRepo.hasOpenTask(videoId, "low", 0),
                    "successful retry should create a pending local upload task");
            assertTrue(transcodeRepo.hasState(videoId, "low", 0, TranscodeSegmentState.TRANSCODED),
                    "successful retry should advance the segment to TRANSCODED");
        } finally {
            shutdownExecutor(executor);
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void startupRecoveryRequeuesStaleQueuedTranscodeWithoutClaimOrUploadTask() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";
        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-stale-queued-recovery-");
        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "processing-it",
                1
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        try {
            transcodeRepo.upsertState(videoId, "low", 0, TranscodeSegmentState.QUEUED);
            Thread.sleep(5L);

            recovery.recoverIncompleteVideos(new CountingStorageClient(chunkKey, new byte[0]));

            TranscodeTaskEvent republished = transcodeBus.awaitPublished(videoId, "low", 0, 5, TimeUnit.SECONDS);
            assertNotNull(republished, "startup recovery should republish stale QUEUED work");
            assertEquals(chunkKey, republished.getChunkKey());
            assertFalse(uploadTaskRepo.hasOpenTask(videoId, "low", 0),
                    "recovery should republish the transcode task before any local upload task exists");
        } finally {
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void startupResetsUploadingTasksBackToPending() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId, 1, "PROCESSING");

        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        Path spoolRoot = Files.createTempDirectory("processing-reset-uploading-");
        Path spoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(spoolFile.getParent());
        Files.write(spoolFile, "pending-upload".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        try {
            uploadTaskRepo.upsertPending(
                    videoId,
                    "startup-reset-test",
                    "low",
                    0,
                    videoId + "/chunks/output0.ts",
                    videoId + "/processed/low/output0.ts",
                    spoolFile.toString(),
                    Files.size(spoolFile),
                    0d
            );

            assertTrue(uploadTaskRepo.claimNextReady("startup-reset-test", 0).isPresent(),
                    "test setup should move the task into UPLOADING");
            assertEquals(1, uploadTaskRepo.countByState("UPLOADING"));

            int reset = uploadTaskRepo.resetUploadingTasks();

            assertEquals(1, reset, "startup should reset stranded UPLOADING tasks");
            assertEquals(0, uploadTaskRepo.countByState("UPLOADING"));
            assertEquals(1, uploadTaskRepo.countByState("PENDING"));
            assertTrue(uploadTaskRepo.hasOpenTask(videoId, "low", 0));
            assertTrue(uploadTaskRepo.claimNextReady("startup-reset-test", 0).isPresent(),
                    "reset task should be claimable again as PENDING");
        } finally {
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void startupRecoveryCreatesPendingUploadTaskForOrphanedSpoolFile() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-orphan-spool-");
        Path orphanedFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(orphanedFile.getParent());
        byte[] payload = "orphaned-transcode".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(orphanedFile, payload);

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "startup-recovery-test",
                1_000
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        try {
            recovery.recoverOrphanedSpoolFiles(new UploadingStorageClient(), spoolRoot);

            assertTrue(uploadTaskRepo.hasOpenTask(videoId, "low", 0),
                    "orphaned spool file should become a pending local upload task");
            assertEquals(1, uploadTaskRepo.countByState("PENDING"));

            Optional<LocalSpoolUploadTask> claimed = uploadTaskRepo.claimNextReady("startup-recovery-test", 0);
            assertTrue(claimed.isPresent(), "recovered orphaned spool file should be claimable by upload workers");
            assertEquals("startup-recovery-test", claimed.get().spoolOwner());
            assertEquals(orphanedFile.toAbsolutePath().toString(), claimed.get().spoolPath());
            assertEquals(videoId + "/processed/low/output0.ts", claimed.get().outputKey());
            assertEquals(payload.length, claimed.get().sizeBytes());
        } finally {
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void localUploadWorkerOnSameInstanceUploadsPendingSpoolFile() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-upload-same-instance-");
        Path spoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(spoolFile.getParent());
        byte[] payload = "processed-segment".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(spoolFile, payload);

        String chunkKey = videoId + "/chunks/output0.ts";
        String outputKey = videoId + "/processed/low/output0.ts";
        uploadTaskRepo.upsertPending(
                videoId,
                "same-instance",
                "low",
                0,
                chunkKey,
                outputKey,
                spoolFile.toString(),
                payload.length,
                0d
        );

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "same-instance",
                1_000
        );
        UploadingStorageClient storageClient = new UploadingStorageClient();
        LocalSpoolUploadWorkerPool pool = new LocalSpoolUploadWorkerPool(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        var executor = pool.startUploadWorkers(1, 50, 250, storageClient);
        try {
            awaitCondition(Duration.ofSeconds(5), () -> !uploadTaskRepo.hasOpenTask(videoId, "low", 0));
            awaitCondition(Duration.ofSeconds(5),
                    () -> transcodeRepo.hasState(videoId, "low", 0, TranscodeSegmentState.DONE));
            assertEquals(1, storageClient.uploadAttempts(outputKey));
            assertArrayEquals(payload, storageClient.uploadedPayload(outputKey));
            assertFalse(Files.exists(spoolFile), "spool file should be deleted after successful upload");
            assertNull(transcodeBus.findPublished(videoId, "low", 0),
                    "successful same-instance upload should not republish a transcode task");
        } finally {
            shutdownExecutor(executor);
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void differentInstanceDoesNotClaimRemoteSpoolUploadTask() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-upload-different-instance-");
        Path missingSpoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");

        String chunkKey = videoId + "/chunks/output0.ts";
        String outputKey = videoId + "/processed/low/output0.ts";
        uploadTaskRepo.upsertPending(
                videoId,
                "same-instance",
                "low",
                0,
                chunkKey,
                outputKey,
                missingSpoolFile.toString(),
                123L,
                0d
        );

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "different-instance",
                1_000
        );
        UploadingStorageClient storageClient = new UploadingStorageClient();
        LocalSpoolUploadWorkerPool pool = new LocalSpoolUploadWorkerPool(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        var executor = pool.startUploadWorkers(1, 50, 250, storageClient);
        try {
            Thread.sleep(500);
            assertTrue(uploadTaskRepo.hasOpenTask(videoId, "low", 0),
                    "different instance should leave the remote-owned upload task alone");
            assertNull(uploadTaskRepo.claimNextReady("different-instance", 0).orElse(null),
                    "different instance should not be able to claim a remote-owned spool task");
            assertEquals(0, storageClient.totalUploads(),
                    "different instance without the local spool file should not upload anything");
            assertNull(transcodeBus.findPublished(videoId, "low", 0),
                    "different instance should not republish a task it does not own");
        } finally {
            shutdownExecutor(executor);
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void transcodeToSpoolContinuesWhenFileExistsCheckFails() throws Exception {
        assumeFfmpegAvailable();

        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";
        Path validChunk = createValidTransportStreamChunk();
        byte[] validChunkBytes = Files.readAllBytes(validChunk);
        Files.deleteIfExists(validChunk);

        Path spoolRoot = Files.createTempDirectory("processing-safe-file-exists-");
        FileExistsFailureStorageClient storageClient = new FileExistsFailureStorageClient(chunkKey, validChunkBytes);
        TranscodingTask task = new TranscodingTask(
                UUID.randomUUID().toString(),
                videoId,
                chunkKey,
                TranscodingProfile.LOW
        );

        try {
            TranscodingTask.CompletedTranscode completed = task.transcodeToSpool(storageClient, spoolRoot);

            assertNotNull(completed, "transcode should continue even when fileExists throws");
            assertEquals(1, storageClient.fileExistsAttempts(),
                    "safeFileExists should still attempt the optimistic existence check");
            assertEquals(1, storageClient.downloadAttempts(chunkKey),
                    "transcoding should proceed to source download after fileExists failure");
            assertTrue(Files.exists(completed.localPath()), "transcoded output should be spooled locally");
            assertEquals(task.getOutputKey(), completed.outputKey());
            assertTrue(completed.sizeBytes() > 0, "spooled output should not be empty");
        } finally {
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void transcodeToSpoolRetriesSourceChunkDownloadUntilMinioRecovers() throws Exception {
        assumeFfmpegAvailable();

        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";
        Path validChunk = createValidTransportStreamChunk();
        byte[] validChunkBytes = Files.readAllBytes(validChunk);
        Files.deleteIfExists(validChunk);

        Path spoolRoot = Files.createTempDirectory("processing-download-retry-");
        RecoveringDownloadStorageClient storageClient =
                new RecoveringDownloadStorageClient(chunkKey, validChunkBytes, 2);
        TranscodingTask task = new TranscodingTask(
                UUID.randomUUID().toString(),
                videoId,
                chunkKey,
                TranscodingProfile.LOW
        );

        try {
            TranscodingTask.CompletedTranscode completed = task.transcodeToSpool(storageClient, spoolRoot);

            assertNotNull(completed, "transcode should succeed once source download recovers");
            assertEquals(3, storageClient.downloadAttempts(chunkKey),
                    "download should be retried until the recovered attempt succeeds");
            assertTrue(Files.exists(completed.localPath()), "recovered download should still produce local spool output");
            assertTrue(completed.sizeBytes() > 0, "spooled output should not be empty after retry recovery");
        } finally {
            deleteDirectory(spoolRoot);
        }
    }

    @Test
    void localUploadWorkerRetriesPendingTaskWhenMinioUploadRecovers() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId, 1, "PROCESSING");

        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        VideoProcessingRepository videoRepo = new VideoProcessingRepository(jdbcUrl, username, password);
        ProcessingUploadTaskRepository uploadTaskRepo =
                new ProcessingUploadTaskRepository(jdbcUrl, username, password);
        ProcessingTaskClaimRepository claimRepo =
                new ProcessingTaskClaimRepository(jdbcUrl, username, password);

        TestStatusEventBus statusBus = new TestStatusEventBus();
        RecordingTranscodeTaskBus transcodeBus = new RecordingTranscodeTaskBus();
        Path spoolRoot = Files.createTempDirectory("processing-upload-retry-");
        Path spoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(spoolFile.getParent());
        byte[] payload = "processed-segment-retry".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(spoolFile, payload);

        String chunkKey = videoId + "/chunks/output0.ts";
        String outputKey = videoId + "/processed/low/output0.ts";
        uploadTaskRepo.upsertPending(
                videoId,
                "upload-retry-instance",
                "low",
                0,
                chunkKey,
                outputKey,
                spoolFile.toString(),
                payload.length,
                0d
        );

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeRepo,
                videoRepo,
                uploadTaskRepo,
                claimRepo,
                statusBus,
                transcodeBus,
                null,
                null,
                spoolRoot,
                "upload-retry-instance",
                1_000
        );
        FailThenRecoverUploadingStorageClient storageClient = new FailThenRecoverUploadingStorageClient();
        LocalSpoolUploadWorkerPool pool = new LocalSpoolUploadWorkerPool(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        var executor = pool.startUploadWorkers(1, 50, 250, storageClient);
        try {
            assertTrue(storageClient.awaitFirstFailure(5, TimeUnit.SECONDS),
                    "first local upload attempt should fail while MinIO is unavailable");
            awaitCondition(Duration.ofSeconds(5), () -> uploadTaskRepo.hasOpenTask(videoId, "low", 0));
            assertTrue(Files.exists(spoolFile), "spool file should be kept after failed upload");
            assertEquals(1, storageClient.uploadAttempts(outputKey),
                    "first upload attempt should have reached storage before failure");

            storageClient.allowRecovery();

            awaitCondition(Duration.ofSeconds(5), () -> !uploadTaskRepo.hasOpenTask(videoId, "low", 0));
            awaitCondition(Duration.ofSeconds(5),
                    () -> transcodeRepo.hasState(videoId, "low", 0, TranscodeSegmentState.DONE));
            assertEquals(2, storageClient.uploadAttempts(outputKey),
                    "worker should retry the same upload task after resetting it to PENDING");
            assertArrayEquals(payload, storageClient.uploadedPayload(outputKey));
            assertFalse(Files.exists(spoolFile), "spool file should be deleted after recovered upload");
        } finally {
            storageClient.allowRecovery();
            shutdownExecutor(executor);
            runtime.resetForTests();
            cleanupProcessingRows(videoId);
            deleteDirectory(spoolRoot);
        }
    }

    private void loadDatabaseConfig() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = normalizeJdbcUrl(firstNonBlank(System.getenv("PG_URL"), dotenv.get("PG_URL")));
        username = firstNonBlank(System.getenv("PG_USER"), dotenv.get("PG_USER"));
        password = firstNonBlank(System.getenv("PG_PASSWORD"), dotenv.get("PG_PASSWORD"));
    }

    private void assumeDatabaseReachable() {
        Assumptions.assumeTrue(jdbcUrl != null && !jdbcUrl.isBlank(), "PG_URL is not set");
        Assumptions.assumeTrue(username != null && !username.isBlank(), "PG_USER is not set");
        try (Connection ignored = DriverManager.getConnection(jdbcUrl, username, password)) {
            // reachable
        } catch (SQLException e) {
            Assumptions.assumeTrue(false, "Skipping integration test because Postgres is not reachable: " + e.getMessage());
        }
    }

    private void assumeFfmpegAvailable() {
        try {
            Process process = new ProcessBuilder("ffmpeg", "-version")
                    .redirectErrorStream(true)
                    .start();
            int exitCode = process.waitFor();
            Assumptions.assumeTrue(exitCode == 0, "Skipping integration test because ffmpeg is not available");
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Skipping integration test because ffmpeg is not available: " + e.getMessage());
        }
    }

    private void seedVideoRecord(String videoId, int totalSegments, String status) {
        String sql = """
                INSERT INTO video_upload (video_id, video_name, total_segments, status, machine_id, container_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (video_id) DO UPDATE
                SET total_segments = EXCLUDED.total_segments,
                    status = EXCLUDED.status,
                    machine_id = EXCLUDED.machine_id,
                    container_id = EXCLUDED.container_id
                """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, "Processing Failure Scenario");
            ps.setInt(3, totalSegments);
            ps.setString(4, status);
            ps.setString(5, "machine-a");
            ps.setString(6, "container-a");
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to seed video_upload", e);
        }
    }

    private void cleanupProcessingRows(String videoId) {
        if (videoId == null || jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            deleteByVideoId(conn, "processing_task_claim", videoId);
            deleteByVideoId(conn, "processing_upload_task", videoId);
            deleteByVideoId(conn, "transcoded_segment_status", videoId);
            deleteByVideoId(conn, "video_upload", videoId);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up processing integration test data", e);
        }
    }

    private void deleteByVideoId(Connection conn, String tableName, String videoId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE video_id = ?")) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.executeUpdate();
        }
    }

    private void deleteDirectory(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                }
            });
        }
    }

    private void awaitCondition(Duration timeout, CheckedBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not satisfied within " + timeout);
    }

    private void shutdownExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    private TranscodeTaskEvent awaitRepublished(
            RecordingTranscodeTaskBus bus,
            String videoId,
            String profile,
            int segmentNumber,
            Duration timeout
    ) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            TranscodeTaskEvent event = bus.findPublished(videoId, profile, segmentNumber);
            if (event != null) {
                return event;
            }
            Thread.sleep(25);
        }
        return bus.findPublished(videoId, profile, segmentNumber);
    }

    private Path createValidTransportStreamChunk() throws Exception {
        Path output = Files.createTempFile("processing-chunk-", ".ts");
        ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg",
                "-f", "lavfi",
                "-i", "testsrc=duration=6:size=320x240:rate=25",
                "-f", "lavfi",
                "-i", "sine=frequency=1000:duration=6",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-g", "25",
                "-keyint_min", "25",
                "-sc_threshold", "0",
                "-c:a", "aac",
                "-f", "mpegts",
                "-shortest",
                "-y",
                output.toString()
        );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            while (reader.readLine() != null) {
                // drain
            }
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            Files.deleteIfExists(output);
            throw new RuntimeException("Failed to create transport stream chunk, exit code: " + exitCode);
        }
        return output;
    }

    private String normalizeJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        return url
                .replace("jdbc:postgresql://postgres:", "jdbc:postgresql://localhost:")
                .replace("jdbc:postgresql://host.docker.internal:", "jdbc:postgresql://localhost:");
    }

    private String firstNonBlank(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private static final class CountingStorageClient implements ObjectStorageClient {
        private final String chunkKey;
        private final byte[] validChunkBytes;
        private final Map<String, Integer> downloads = new ConcurrentHashMap<>();

        private CountingStorageClient(String chunkKey, byte[] validChunkBytes) {
            this.chunkKey = chunkKey;
            this.validChunkBytes = validChunkBytes;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream downloadFile(String key) {
            downloads.merge(key, 1, Integer::sum);
            if (!chunkKey.equals(key)) {
                throw new RuntimeException("Unexpected chunk key: " + key);
            }
            if (downloads.get(key) == 1) {
                return new ByteArrayInputStream("not-a-real-ts".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            return new ByteArrayInputStream(validChunkBytes);
        }

        @Override
        public void deleteFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean fileExists(String key) {
            return false;
        }

        @Override
        public List<String> listFiles(String prefix) {
            return chunkKey.startsWith(prefix) ? List.of(chunkKey) : List.of();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private int downloadAttempts(String key) {
            return downloads.getOrDefault(key, 0);
        }
    }

    private static final class FileExistsFailureStorageClient implements ObjectStorageClient {
        private final String chunkKey;
        private final byte[] validChunkBytes;
        private int fileExistsAttempts;
        private final Map<String, Integer> downloads = new ConcurrentHashMap<>();

        private FileExistsFailureStorageClient(String chunkKey, byte[] validChunkBytes) {
            this.chunkKey = chunkKey;
            this.validChunkBytes = validChunkBytes;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream downloadFile(String key) {
            downloads.merge(key, 1, Integer::sum);
            if (!chunkKey.equals(key)) {
                throw new RuntimeException("Unexpected chunk key: " + key);
            }
            return new ByteArrayInputStream(validChunkBytes);
        }

        @Override
        public void deleteFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean fileExists(String key) {
            fileExistsAttempts++;
            throw new RuntimeException("simulated MinIO outage during fileExists");
        }

        @Override
        public List<String> listFiles(String prefix) {
            return List.of();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private int fileExistsAttempts() {
            return fileExistsAttempts;
        }

        private int downloadAttempts(String key) {
            return downloads.getOrDefault(key, 0);
        }
    }

    private static final class RecoveringDownloadStorageClient implements ObjectStorageClient {
        private final String chunkKey;
        private final byte[] validChunkBytes;
        private final int failuresBeforeSuccess;
        private final Map<String, Integer> downloads = new ConcurrentHashMap<>();

        private RecoveringDownloadStorageClient(String chunkKey, byte[] validChunkBytes, int failuresBeforeSuccess) {
            this.chunkKey = chunkKey;
            this.validChunkBytes = validChunkBytes;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream downloadFile(String key) {
            int attempt = downloads.merge(key, 1, Integer::sum);
            if (!chunkKey.equals(key)) {
                throw new RuntimeException("Unexpected chunk key: " + key);
            }
            if (attempt <= failuresBeforeSuccess) {
                throw new UncheckedIOException(new IOException(
                        "simulated MinIO outage during download attempt " + attempt));
            }
            return new ByteArrayInputStream(validChunkBytes);
        }

        @Override
        public void deleteFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean fileExists(String key) {
            return false;
        }

        @Override
        public List<String> listFiles(String prefix) {
            return List.of();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private int downloadAttempts(String key) {
            return downloads.getOrDefault(key, 0);
        }
    }

    private static final class TestStatusEventBus implements StatusEventBus {
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

    private static final class RecordingTranscodeTaskBus implements TranscodeTaskBus {
        private final List<TranscodeTaskEvent> published = new CopyOnWriteArrayList<>();

        @Override
        public void publish(TranscodeTaskEvent event) {
            published.add(event);
        }

        @Override
        public void subscribe(TranscodeTaskListener listener) {
        }

        private TranscodeTaskEvent awaitPublished(String videoId, String profile, int segmentNumber, long timeout, TimeUnit unit)
                throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (System.nanoTime() < deadline) {
                Optional<TranscodeTaskEvent> match = published.stream()
                        .filter(ev -> ev.getJobId().equals(videoId))
                        .filter(ev -> ev.getProfile().equals(profile))
                        .filter(ev -> ev.getSegmentNumber() == segmentNumber)
                        .findFirst();
                if (match.isPresent()) {
                    return match.get();
                }
                Thread.sleep(25);
            }
            return null;
        }

        private TranscodeTaskEvent findPublished(String videoId, String profile, int segmentNumber) {
            return published.stream()
                    .filter(ev -> ev.getJobId().equals(videoId))
                    .filter(ev -> ev.getProfile().equals(profile))
                    .filter(ev -> ev.getSegmentNumber() == segmentNumber)
                    .findFirst()
                    .orElse(null);
        }
    }

    private static final class UploadingStorageClient implements ObjectStorageClient {
        private final Map<String, byte[]> uploads = new ConcurrentHashMap<>();
        private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            attempts.merge(key, 1, Integer::sum);
            try {
                uploads.put(key, data.readAllBytes());
            } catch (IOException e) {
                throw new RuntimeException("Failed to read upload payload", e);
            }
        }

        @Override
        public InputStream downloadFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String key) {
            uploads.remove(key);
        }

        @Override
        public boolean fileExists(String key) {
            return uploads.containsKey(key);
        }

        @Override
        public List<String> listFiles(String prefix) {
            return uploads.keySet().stream().filter(key -> key.startsWith(prefix)).toList();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private int uploadAttempts(String key) {
            return attempts.getOrDefault(key, 0);
        }

        private byte[] uploadedPayload(String key) {
            return uploads.get(key);
        }

        private int totalUploads() {
            return attempts.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    private static final class FailThenRecoverUploadingStorageClient implements ObjectStorageClient {
        private final Map<String, byte[]> uploads = new ConcurrentHashMap<>();
        private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
        private final CountDownLatch firstFailure = new CountDownLatch(1);
        private final CountDownLatch recoveryGate = new CountDownLatch(1);
        private volatile boolean failedOnce;

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            int attempt = attempts.merge(key, 1, Integer::sum);
            try {
                if (!failedOnce) {
                    failedOnce = true;
                    data.readAllBytes();
                    firstFailure.countDown();
                    throw new RuntimeException("simulated MinIO outage during spool upload attempt " + attempt);
                }
                recoveryGate.await(5, TimeUnit.SECONDS);
                uploads.put(key, data.readAllBytes());
            } catch (IOException e) {
                throw new RuntimeException("Failed to read upload payload", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for MinIO recovery", e);
            }
        }

        @Override
        public InputStream downloadFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String key) {
            uploads.remove(key);
        }

        @Override
        public boolean fileExists(String key) {
            return uploads.containsKey(key);
        }

        @Override
        public List<String> listFiles(String prefix) {
            return uploads.keySet().stream().filter(key -> key.startsWith(prefix)).toList();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private boolean awaitFirstFailure(long timeout, TimeUnit unit) throws InterruptedException {
            return firstFailure.await(timeout, unit);
        }

        private void allowRecovery() {
            recoveryGate.countDown();
        }

        private int uploadAttempts(String key) {
            return attempts.getOrDefault(key, 0);
        }

        private byte[] uploadedPayload(String key) {
            return uploads.get(key);
        }
    }
}
