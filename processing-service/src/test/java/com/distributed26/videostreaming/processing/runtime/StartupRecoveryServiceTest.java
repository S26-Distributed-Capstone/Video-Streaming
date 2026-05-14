package com.distributed26.videostreaming.processing.runtime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository.UploadTaskMetadata;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StartupRecoveryServiceTest {

    @Test
    void recoverOrphanedSpoolFilesSkipsHealthyLocalUploadTask() throws Exception {
        ProcessingUploadTaskRepository uploadTaskRepository = mock(ProcessingUploadTaskRepository.class);
        ProcessingTaskClaimRepository claimRepository = mock(ProcessingTaskClaimRepository.class);
        ObjectStorageClient storageClient = mock(ObjectStorageClient.class);

        Path spoolRoot = Files.createTempDirectory("startup-recovery-safe-");
        String videoId = UUID.randomUUID().toString();
        Path spoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(spoolFile.getParent());
        Files.write(spoolFile, "segment".getBytes(StandardCharsets.UTF_8));

        ProcessingRuntime runtime = new ProcessingRuntime(
                null,
                null,
                uploadTaskRepository,
                claimRepository,
                null,
                null,
                null,
                null,
                spoolRoot,
                "processor-a",
                60_000L
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        try {
            String outputKey = videoId + "/processed/low/output0.ts";
            when(storageClient.fileExists(outputKey)).thenReturn(false);
            when(uploadTaskRepository.findTask(videoId, "low", 0)).thenReturn(Optional.of(
                    new UploadTaskMetadata(
                            "processor-a",
                            spoolFile.toAbsolutePath().toString(),
                            "UPLOADING",
                            "processor-a"
                    )
            ));
            when(claimRepository.hasActiveClaim(videoId, "low", 0, 60_000L)).thenReturn(true);

            recovery.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepository, never()).upsertPending(
                    anyString(), anyString(), anyString(), eq(0), anyString(), anyString(), anyString(), anyLong(), anyDouble()
            );
        } finally {
            runtime.resetForTests();
            Files.deleteIfExists(spoolFile);
            Files.deleteIfExists(spoolFile.getParent());
            Files.deleteIfExists(spoolFile.getParent().getParent());
            Files.deleteIfExists(spoolRoot);
        }
    }

    @Test
    void recoverOrphanedSpoolFilesReassignsVisibleSpoolFileFromDifferentOwner() throws Exception {
        ProcessingUploadTaskRepository uploadTaskRepository = mock(ProcessingUploadTaskRepository.class);
        ProcessingTaskClaimRepository claimRepository = mock(ProcessingTaskClaimRepository.class);
        ObjectStorageClient storageClient = mock(ObjectStorageClient.class);

        Path spoolRoot = Files.createTempDirectory("startup-recovery-takeover-");
        String videoId = UUID.randomUUID().toString();
        Path spoolFile = spoolRoot.resolve(videoId).resolve("low").resolve("output0.ts");
        Files.createDirectories(spoolFile.getParent());
        Files.write(spoolFile, "segment".getBytes(StandardCharsets.UTF_8));

        ProcessingRuntime runtime = new ProcessingRuntime(
                null,
                null,
                uploadTaskRepository,
                claimRepository,
                null,
                null,
                null,
                null,
                spoolRoot,
                "processor-b",
                60_000L
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime
        );

        try {
            String chunkKey = videoId + "/chunks/output0.ts";
            String outputKey = videoId + "/processed/low/output0.ts";
            when(storageClient.fileExists(outputKey)).thenReturn(false);
            when(uploadTaskRepository.findTask(videoId, "low", 0)).thenReturn(Optional.of(
                    new UploadTaskMetadata(
                            "dead-owner",
                            spoolFile.toAbsolutePath().toString(),
                            "PENDING",
                            null
                    )
            ));
            when(claimRepository.hasActiveClaim(videoId, "low", 0, 60_000L)).thenReturn(false);

            recovery.recoverOrphanedSpoolFiles(storageClient, spoolRoot);

            verify(uploadTaskRepository).upsertPending(
                    eq(videoId),
                    eq("processor-b"),
                    eq("low"),
                    eq(0),
                    eq(chunkKey),
                    eq(outputKey),
                    eq(spoolFile.toAbsolutePath().toString()),
                    eq((long) Files.size(spoolFile)),
                    eq(0d)
            );
        } finally {
            runtime.resetForTests();
            Files.deleteIfExists(spoolFile);
            Files.deleteIfExists(spoolFile.getParent());
            Files.deleteIfExists(spoolFile.getParent().getParent());
            Files.deleteIfExists(spoolRoot);
        }
    }

    @Test
    void recoverIncompleteVideosDefersStaleQueuedWorkDuringBacklogProgress() {
        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";

        TranscodedSegmentStatusRepository transcodeStatusRepository = mock(TranscodedSegmentStatusRepository.class);
        VideoProcessingRepository videoProcessingRepository = mock(VideoProcessingRepository.class);
        ProcessingUploadTaskRepository uploadTaskRepository = mock(ProcessingUploadTaskRepository.class);
        ProcessingTaskClaimRepository claimRepository = mock(ProcessingTaskClaimRepository.class);
        TranscodeTaskBus transcodeTaskBus = mock(TranscodeTaskBus.class);
        ObjectStorageClient storageClient = mock(ObjectStorageClient.class);

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeStatusRepository,
                videoProcessingRepository,
                uploadTaskRepository,
                claimRepository,
                null,
                transcodeTaskBus,
                null,
                null,
                null,
                "processor-a",
                60_000L
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime,
                false,
                45_000L
        );

        when(videoProcessingRepository.findVideoIdsByStatus("PROCESSING")).thenReturn(List.of(videoId));
        when(videoProcessingRepository.findVideoIdsByStatus("UPLOADED")).thenReturn(List.of());
        when(videoProcessingRepository.findStatusByVideoId(videoId)).thenReturn(Optional.of("PROCESSING"));
        when(videoProcessingRepository.findTotalSegments(videoId)).thenReturn(OptionalInt.of(1));
        when(storageClient.listFiles(videoId + "/chunks/")).thenReturn(List.of(chunkKey));
        when(storageClient.fileExists(anyString())).thenReturn(false);
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.QUEUED)).thenReturn(Set.of(0));
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODING)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODED)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.UPLOADING)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByStateUpdatedSince(
                eq(videoId), eq("low"), eq(TranscodeSegmentState.QUEUED), anyLong())
        ).thenAnswer(invocation -> ((Long) invocation.getArgument(3)) >= 180_000L ? Set.of(0) : Set.of());
        when(claimRepository.findClaimedSegmentNumbers(videoId, "low", 60_000L)).thenReturn(Set.of(7));
        when(uploadTaskRepository.findOpenSegmentNumbers(videoId, "low")).thenReturn(Set.of());
        when(transcodeStatusRepository.countByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(0);

        recovery.recoverIncompleteVideos(storageClient);

        verify(transcodeTaskBus, never()).publish(any());
        runtime.resetForTests();
    }

    @Test
    void recoverIncompleteVideosRepublishesIdleStaleQueuedWork() {
        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";

        TranscodedSegmentStatusRepository transcodeStatusRepository = mock(TranscodedSegmentStatusRepository.class);
        VideoProcessingRepository videoProcessingRepository = mock(VideoProcessingRepository.class);
        ProcessingUploadTaskRepository uploadTaskRepository = mock(ProcessingUploadTaskRepository.class);
        ProcessingTaskClaimRepository claimRepository = mock(ProcessingTaskClaimRepository.class);
        TranscodeTaskBus transcodeTaskBus = mock(TranscodeTaskBus.class);
        ObjectStorageClient storageClient = mock(ObjectStorageClient.class);

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeStatusRepository,
                videoProcessingRepository,
                uploadTaskRepository,
                claimRepository,
                null,
                transcodeTaskBus,
                null,
                null,
                null,
                "processor-a",
                60_000L
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime,
                false,
                45_000L
        );

        when(videoProcessingRepository.findVideoIdsByStatus("PROCESSING")).thenReturn(List.of(videoId));
        when(videoProcessingRepository.findVideoIdsByStatus("UPLOADED")).thenReturn(List.of());
        when(videoProcessingRepository.findStatusByVideoId(videoId)).thenReturn(Optional.of("PROCESSING"));
        when(videoProcessingRepository.findTotalSegments(videoId)).thenReturn(OptionalInt.of(1));
        when(storageClient.listFiles(videoId + "/chunks/")).thenReturn(List.of(chunkKey));
        when(storageClient.fileExists(anyString())).thenReturn(false);
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.QUEUED)).thenReturn(Set.of(0));
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODING)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODED)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.UPLOADING)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByStateUpdatedSince(
                eq(videoId), eq("low"), eq(TranscodeSegmentState.QUEUED), anyLong())
        ).thenReturn(Set.of());
        when(claimRepository.findClaimedSegmentNumbers(videoId, "low", 60_000L)).thenReturn(Set.of());
        when(uploadTaskRepository.findOpenSegmentNumbers(videoId, "low")).thenReturn(Set.of());
        when(transcodeStatusRepository.countByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(0);

        recovery.recoverIncompleteVideos(storageClient);

        verify(transcodeTaskBus).publish(any());
        runtime.resetForTests();
    }

    @Test
    void recoverIncompleteVideosRepublishesMissingTaskWithoutQueuedStatus() {
        String videoId = UUID.randomUUID().toString();
        String chunkKey = videoId + "/chunks/output0.ts";

        TranscodedSegmentStatusRepository transcodeStatusRepository = mock(TranscodedSegmentStatusRepository.class);
        VideoProcessingRepository videoProcessingRepository = mock(VideoProcessingRepository.class);
        ProcessingUploadTaskRepository uploadTaskRepository = mock(ProcessingUploadTaskRepository.class);
        ProcessingTaskClaimRepository claimRepository = mock(ProcessingTaskClaimRepository.class);
        TranscodeTaskBus transcodeTaskBus = mock(TranscodeTaskBus.class);
        ObjectStorageClient storageClient = mock(ObjectStorageClient.class);

        ProcessingRuntime runtime = new ProcessingRuntime(
                transcodeStatusRepository,
                videoProcessingRepository,
                uploadTaskRepository,
                claimRepository,
                null,
                transcodeTaskBus,
                null,
                null,
                null,
                "processor-a",
                60_000L
        );
        StartupRecoveryService recovery = new StartupRecoveryService(
                new TranscodingProfile[]{TranscodingProfile.LOW},
                runtime,
                false,
                45_000L
        );

        when(videoProcessingRepository.findVideoIdsByStatus("PROCESSING")).thenReturn(List.of(videoId));
        when(videoProcessingRepository.findVideoIdsByStatus("UPLOADED")).thenReturn(List.of());
        when(videoProcessingRepository.findStatusByVideoId(videoId)).thenReturn(Optional.of("PROCESSING"));
        when(videoProcessingRepository.findTotalSegments(videoId)).thenReturn(OptionalInt.of(1));
        when(storageClient.listFiles(videoId + "/chunks/")).thenReturn(List.of(chunkKey));
        when(storageClient.fileExists(anyString())).thenReturn(false);
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.QUEUED)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODING)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.TRANSCODED)).thenReturn(Set.of());
        when(transcodeStatusRepository.findSegmentNumbersByState(videoId, "low", TranscodeSegmentState.UPLOADING)).thenReturn(Set.of());
        when(claimRepository.findClaimedSegmentNumbers(videoId, "low", 60_000L)).thenReturn(Set.of());
        when(uploadTaskRepository.findOpenSegmentNumbers(videoId, "low")).thenReturn(Set.of());
        when(transcodeStatusRepository.countByState(videoId, "low", TranscodeSegmentState.DONE)).thenReturn(0);

        recovery.recoverIncompleteVideos(storageClient);

        verify(transcodeTaskBus).publish(any());
        runtime.resetForTests();
    }
}



