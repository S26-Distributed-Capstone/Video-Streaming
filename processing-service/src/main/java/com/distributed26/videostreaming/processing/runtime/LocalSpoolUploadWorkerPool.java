package com.distributed26.videostreaming.processing.runtime;

import com.distributed26.videostreaming.processing.LocalSpoolUploadTask;
import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class LocalSpoolUploadWorkerPool {
    private static final Logger LOGGER = LogManager.getLogger(LocalSpoolUploadWorkerPool.class);

    private final TranscodingProfile[] profiles;
    private final ProcessingRuntime runtime;

    public LocalSpoolUploadWorkerPool(TranscodingProfile[] profiles, ProcessingRuntime runtime) {
        this.profiles = profiles;
        this.runtime = runtime;
    }

    public ExecutorService startUploadWorkers(
            int uploadWorkerCount,
            long uploadPollMillis,
            long uploadClaimTimeoutMillis,
            ObjectStorageClient storageClient
    ) {
        ExecutorService executor = Executors.newFixedThreadPool(
                Math.max(1, uploadWorkerCount),
                new ThreadFactory() {
                    private int index = 0;

                    @Override
                    public synchronized Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable, "processing-uploader-" + index);
                        index += 1;
                        return thread;
                    }
                }
        );
        for (int i = 0; i < Math.max(1, uploadWorkerCount); i++) {
            final int uploaderIndex = i;
            executor.execute(() -> runUploadLoop(
                    "processing-uploader-" + uploaderIndex,
                    uploadPollMillis,
                    uploadClaimTimeoutMillis,
                    storageClient
            ));
        }
        LOGGER.info("Started {} local upload worker(s)", Math.max(1, uploadWorkerCount));
        return executor;
    }

    private void runUploadLoop(
            String uploaderId,
            long uploadPollMillis,
            long uploadClaimTimeoutMillis,
            ObjectStorageClient storageClient
    ) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (runtime.processingUploadTaskRepository() == null) {
                    return;
                }
                var task = runtime.processingUploadTaskRepository().claimNextReady(runtime.processorInstanceId(), uploadClaimTimeoutMillis);
                if (task.isEmpty()) {
                    Thread.sleep(Math.max(50L, uploadPollMillis));
                    continue;
                }
                uploadSpoolTask(task.get(), storageClient);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.error("Local upload worker {} failed in polling loop", uploaderId, e);
                try {
                    Thread.sleep(Math.max(250L, uploadPollMillis));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Package-private for testability. */
    void uploadSpoolTask(LocalSpoolUploadTask task, ObjectStorageClient storageClient) {
        Path spoolPath = Path.of(task.spoolPath());
        if (runtime.processingTaskClaimRepository() != null) {
            runtime.processingTaskClaimRepository().claim(
                    task.videoId(),
                    task.profile(),
                    task.segmentNumber(),
                    "UPLOADING",
                    runtime.processorInstanceId()
            );
        }
        try {
            if (runtime.isVideoFailed(task.videoId())) {
                LOGGER.info("Dropping local upload task {} for failed videoId={}", task.id(), task.videoId());
                discardUploadTask(task, spoolPath);
                return;
            }
            if (storageClient.fileExists(task.outputKey())) {
                LOGGER.info("Local upload task {} already present in object storage, cleaning up spool", task.id());
                completeUploadTask(task, spoolPath);
                return;
            }
            if (!Files.exists(spoolPath)) {
                LOGGER.warn("Local spool file missing for upload task {} path={}, requeueing transcode", task.id(), spoolPath);
                runtime.processingUploadTaskRepository().deleteById(task.id());
                if (runtime.processingTaskClaimRepository() != null) {
                    runtime.processingTaskClaimRepository().release(task.videoId(), task.profile(), task.segmentNumber());
                }
                runtime.publishTranscodeState(task.videoId(), task.profile(), task.segmentNumber(),
                        TranscodeSegmentState.FAILED, profiles);
                TranscodeTaskBus transcodeTaskBusRef = runtime.transcodeTaskBusRef();
                if (transcodeTaskBusRef != null) {
                    transcodeTaskBusRef.publish(new TranscodeTaskEvent(
                            task.videoId(),
                            task.chunkKey(),
                            task.profile(),
                            task.segmentNumber(),
                            task.outputTsOffsetSeconds()
                    ));
                }
                return;
            }
            if (runtime.isVideoFailed(task.videoId())) {
                LOGGER.info("Dropping local upload task {} before upload for failed videoId={}", task.id(), task.videoId());
                discardUploadTask(task, spoolPath);
                return;
            }
            runtime.publishTranscodeState(task.videoId(), task.profile(), task.segmentNumber(),
                    TranscodeSegmentState.UPLOADING, profiles);
            try (InputStream is = Files.newInputStream(spoolPath)) {
                storageClient.uploadFile(task.outputKey(), is, task.sizeBytes());
            }
            completeUploadTask(task, spoolPath);
        } catch (Exception e) {
            LOGGER.warn("Failed local upload task {} videoId={} profile={} segment={} attempt={}",
                    task.id(), task.videoId(), task.profile(), task.segmentNumber(), task.attemptCount(), e);
            runtime.processingUploadTaskRepository().markPending(task.id());
            if (runtime.processingTaskClaimRepository() != null) {
                runtime.processingTaskClaimRepository().release(task.videoId(), task.profile(), task.segmentNumber());
            }
        }
    }

    private void discardUploadTask(LocalSpoolUploadTask task, Path spoolPath) {
        try {
            Files.deleteIfExists(spoolPath);
        } catch (Exception e) {
            LOGGER.warn("Failed to delete discarded spool file for upload task {} path={}", task.id(), spoolPath, e);
        }
        runtime.processingUploadTaskRepository().deleteById(task.id());
        if (runtime.processingTaskClaimRepository() != null) {
            runtime.processingTaskClaimRepository().release(task.videoId(), task.profile(), task.segmentNumber());
        }
    }

    private void completeUploadTask(LocalSpoolUploadTask task, Path spoolPath) {
        try {
            Files.deleteIfExists(spoolPath);
        } catch (Exception e) {
            LOGGER.warn("Failed to delete local spool file after upload task {} path={}", task.id(), spoolPath, e);
        }
        runtime.processingUploadTaskRepository().deleteById(task.id());
        if (runtime.processingTaskClaimRepository() != null) {
            runtime.processingTaskClaimRepository().release(task.videoId(), task.profile(), task.segmentNumber());
        }
        runtime.publishTranscodeState(task.videoId(), task.profile(), task.segmentNumber(),
                TranscodeSegmentState.DONE, profiles);
    }
}
