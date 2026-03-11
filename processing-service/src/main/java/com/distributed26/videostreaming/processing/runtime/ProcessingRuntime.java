package com.distributed26.videostreaming.processing.runtime;

import com.distributed26.videostreaming.processing.AbrManifestService;
import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.processing.TranscodingTask;
import com.distributed26.videostreaming.processing.TranscodingTask.CompletedTranscode;
import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.jobs.WorkerStatus;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ProcessingRuntime {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingRuntime.class);
    private static final java.util.regex.Pattern SEGMENT_NUMBER_PATTERN = java.util.regex.Pattern.compile("(\\d+)");
    private final Set<String> manifestsInFlight = ConcurrentHashMap.newKeySet();

    private TranscodedSegmentStatusRepository transcodeStatusRepository;
    private VideoProcessingRepository videoProcessingRepository;
    private ProcessingUploadTaskRepository processingUploadTaskRepository;
    private ProcessingTaskClaimRepository processingTaskClaimRepository;
    private StatusEventBus statusBus;
    private TranscodeTaskBus transcodeTaskBusRef;
    private AbrManifestService manifestServiceRef;
    private ExecutorService manifestExecutorRef;
    private Path localUploadSpoolRoot;
    private final String processorInstanceId;

    public ProcessingRuntime(
            TranscodedSegmentStatusRepository transcodeStatusRepository,
            VideoProcessingRepository videoProcessingRepository,
            ProcessingUploadTaskRepository processingUploadTaskRepository,
            ProcessingTaskClaimRepository processingTaskClaimRepository,
            StatusEventBus statusBus,
            TranscodeTaskBus transcodeTaskBus,
            AbrManifestService manifestService,
            ExecutorService manifestExecutor,
            Path localUploadSpoolRoot,
            String processorInstanceId
    ) {
        this.transcodeStatusRepository = transcodeStatusRepository;
        this.videoProcessingRepository = videoProcessingRepository;
        this.processingUploadTaskRepository = processingUploadTaskRepository;
        this.processingTaskClaimRepository = processingTaskClaimRepository;
        this.statusBus = statusBus;
        this.transcodeTaskBusRef = transcodeTaskBus;
        this.manifestServiceRef = manifestService;
        this.manifestExecutorRef = manifestExecutor;
        this.localUploadSpoolRoot = localUploadSpoolRoot;
        this.processorInstanceId = processorInstanceId;
    }

    public void resetForTests() {
        manifestsInFlight.clear();
        transcodeStatusRepository = null;
        videoProcessingRepository = null;
        processingUploadTaskRepository = null;
        processingTaskClaimRepository = null;
        statusBus = null;
        transcodeTaskBusRef = null;
        manifestServiceRef = null;
        manifestExecutorRef = null;
        localUploadSpoolRoot = null;
    }

    public void setTranscodeStatusRepository(TranscodedSegmentStatusRepository repository) {
        transcodeStatusRepository = repository;
    }

    public void setProcessingUploadTaskRepository(ProcessingUploadTaskRepository repository) {
        processingUploadTaskRepository = repository;
    }

    public void setProcessingTaskClaimRepository(ProcessingTaskClaimRepository repository) {
        processingTaskClaimRepository = repository;
    }

    public void setStatusBus(StatusEventBus bus) {
        statusBus = bus;
    }

    public void setTranscodeTaskBusRef(TranscodeTaskBus bus) {
        transcodeTaskBusRef = bus;
    }

    public void setManifestServiceRef(AbrManifestService manifestService) {
        manifestServiceRef = manifestService;
    }

    public void setManifestExecutorRef(ExecutorService manifestExecutor) {
        manifestExecutorRef = manifestExecutor;
    }

    public void setLocalUploadSpoolRoot(Path spoolRoot) {
        localUploadSpoolRoot = spoolRoot;
    }

    public void onStatusEvent(JobEvent event) {
        String videoId = event.getJobId();

        if (event instanceof UploadMetaEvent meta) {
            if (manifestServiceRef == null || manifestExecutorRef == null) {
                LOGGER.warn("Ignoring UploadMetaEvent for videoId={} because manifest generator is not configured", videoId);
                return;
            }
            LOGGER.info("UploadMetaEvent: videoId={} totalSegments={} (tasks already in flight)",
                    videoId, meta.getTotalSegments());
            if (meta.getTotalSegments() > 0 && areAllProfilesDone(videoId, meta.getTotalSegments())) {
                scheduleManifestGeneration(videoId, meta.getTotalSegments());
            } else {
                LOGGER.info("Deferring manifest generation until all profiles are DONE for videoId={}", videoId);
            }
            return;
        }
        if (event instanceof TranscodeProgressEvent || event instanceof UploadFailedEvent || event instanceof TranscodeTaskEvent) {
            LOGGER.debug("Ignoring non-manifest status event in processing pipeline: type={}",
                    event.getClass().getSimpleName());
        }
    }

    public TranscodingTask onTranscodeTaskEvent(TranscodeTaskEvent taskEvent, TranscodingProfile[] profiles) {
        String videoId = taskEvent.getJobId();
        String chunkKey = taskEvent.getChunkKey();
        int segmentNumber = taskEvent.getSegmentNumber();
        if (chunkKey == null || !chunkKey.contains("/chunks/") || !chunkKey.endsWith(".ts")) {
            LOGGER.debug("Ignoring malformed transcode task chunkKey={} for videoId={}", chunkKey, videoId);
            return null;
        }
        TranscodingProfile profile = profileFromName(taskEvent.getProfile(), profiles);
        if (profile == null) {
            LOGGER.warn("Ignoring transcode task with unknown profile={} for videoId={} chunk={}",
                    taskEvent.getProfile(), videoId, chunkKey);
            return null;
        }
        if (segmentNumber < 0) {
            segmentNumber = parseSegmentNumber(chunkKey);
        }
        if (isAlreadyDone(videoId, profile.getName(), segmentNumber)) {
            LOGGER.info("Skipping already uploaded segment videoId={} profile={} segment={}",
                    videoId, profile.getName(), segmentNumber);
            publishTranscodeState(videoId, profile.getName(), segmentNumber, TranscodeSegmentState.DONE, profiles);
            return null;
        }
        if (hasOpenLocalUploadTask(videoId, profile.getName(), segmentNumber)) {
            LOGGER.info("Skipping segment with existing local upload task videoId={} profile={} segment={}",
                    videoId, profile.getName(), segmentNumber);
            return null;
        }
        publishTranscodeState(videoId, profile.getName(), segmentNumber, TranscodeSegmentState.QUEUED, profiles);
        return new TranscodingTask(
                UUID.randomUUID().toString(),
                videoId,
                chunkKey,
                profile,
                taskEvent.getOutputTsOffsetSeconds()
        );
    }

    public CompletionStage<Boolean> submitTranscodeTask(
            TranscodeTaskEvent taskEvent,
            ThreadPoolExecutor taskExecutor,
            ObjectStorageClient storageClient,
            java.util.Map<Thread, Worker> workersByThread,
            TranscodingProfile[] profiles
    ) {
        TranscodingTask task = onTranscodeTaskEvent(taskEvent, profiles);
        if (task == null) {
            return CompletableFuture.completedFuture(true);
        }
        if (processingTaskClaimRepository != null) {
            processingTaskClaimRepository.claim(
                    task.getJobId(),
                    task.getProfile().getName(),
                    parseSegmentNumber(task.getChunkKey()),
                    "TRANSCODING",
                    processorInstanceId
            );
        }
        return CompletableFuture.supplyAsync(
                () -> executeTranscodingTask(task, storageClient, workersByThread, profiles),
                taskExecutor
        ).exceptionally(e -> {
            LOGGER.error("Transcode task execution crashed jobId={} chunk={} profile={}",
                    task.getJobId(), task.getChunkKey(), task.getProfile().getName(), e);
            return false;
        }).whenComplete((ignored, error) -> {
            if (processingTaskClaimRepository != null) {
                processingTaskClaimRepository.release(
                        task.getJobId(),
                        task.getProfile().getName(),
                        parseSegmentNumber(task.getChunkKey())
                );
            }
        });
    }

    public void publishTranscodeState(
            String videoId,
            String profile,
            int segmentNumber,
            TranscodeSegmentState state,
            TranscodingProfile[] profiles
    ) {
        if (transcodeStatusRepository == null || statusBus == null || segmentNumber < 0) {
            return;
        }
        try {
            transcodeStatusRepository.upsertState(videoId, profile, segmentNumber, state);
            int done = transcodeStatusRepository.countByState(videoId, profile, TranscodeSegmentState.DONE);
            int total = findTotalSegments(videoId);
            statusBus.publish(new TranscodeProgressEvent(videoId, profile, segmentNumber, state, done, total));
            if (state == TranscodeSegmentState.DONE
                    && total > 0
                    && !manifestsInFlight.contains(videoId)
                    && areAllProfilesDone(videoId, total)) {
                scheduleManifestGeneration(videoId, total);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to persist/publish transcode progress videoId={} profile={} segment={} state={}",
                    videoId, profile, segmentNumber, state, e);
        }
    }

    public ProcessingUploadTaskRepository processingUploadTaskRepository() {
        return processingUploadTaskRepository;
    }

    public ProcessingTaskClaimRepository processingTaskClaimRepository() {
        return processingTaskClaimRepository;
    }

    public String processorInstanceId() {
        return processorInstanceId;
    }

    public VideoProcessingRepository videoProcessingRepository() {
        return videoProcessingRepository;
    }

    public TranscodedSegmentStatusRepository transcodeStatusRepository() {
        return transcodeStatusRepository;
    }

    public TranscodeTaskBus transcodeTaskBusRef() {
        return transcodeTaskBusRef;
    }

    public boolean areAllProfilesDone(String videoId, int totalSegments) {
        return areAllProfilesDone(videoId, totalSegments, null);
    }

    public boolean areAllProfilesDone(String videoId, int totalSegments, TranscodingProfile[] profiles) {
        if (transcodeStatusRepository == null || totalSegments <= 0) {
            return false;
        }
        TranscodingProfile[] activeProfiles = profiles == null ? defaultProfiles() : profiles;
        for (TranscodingProfile profile : activeProfiles) {
            int done = transcodeStatusRepository.countByState(videoId, profile.getName(), TranscodeSegmentState.DONE);
            if (done < totalSegments) {
                return false;
            }
        }
        return true;
    }

    public int findTotalSegments(String videoId) {
        if (videoProcessingRepository == null) {
            return 0;
        }
        try {
            return videoProcessingRepository.findTotalSegments(videoId).orElse(0);
        } catch (Exception e) {
            LOGGER.warn("Failed to load totalSegments for videoId={}", videoId, e);
            return 0;
        }
    }

    public void scheduleManifestGeneration(String videoId, int totalSegments) {
        if (manifestServiceRef == null || manifestExecutorRef == null) {
            LOGGER.warn("Cannot schedule manifest generation for videoId={} because manifest generator is not configured",
                    videoId);
            return;
        }
        if (!manifestsInFlight.add(videoId)) {
            LOGGER.debug("Manifest generation already running/skipped for videoId={}", videoId);
            return;
        }
        try {
            manifestExecutorRef.execute(() -> {
                try {
                    manifestServiceRef.generateIfNeeded(videoId, totalSegments);
                    if (videoProcessingRepository != null) {
                        videoProcessingRepository.updateStatus(videoId, "COMPLETED");
                    }
                } catch (Exception e) {
                    LOGGER.error("Manifest generation failed for videoId={}", videoId, e);
                } finally {
                    manifestsInFlight.remove(videoId);
                }
            });
        } catch (RuntimeException e) {
            manifestsInFlight.remove(videoId);
            LOGGER.error("Failed to submit manifest generation task for videoId={}", videoId, e);
        }
    }

    public static int parseSegmentNumber(String chunkKey) {
        if (chunkKey == null || chunkKey.isBlank()) {
            return -1;
        }
        java.util.regex.Matcher matcher = SEGMENT_NUMBER_PATTERN.matcher(chunkKey);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last;
    }

    public static double fallbackOffsetForSegment(int segmentNumber) {
        return segmentNumber < 0 ? 0d : (double) segmentNumber * Math.max(1, TranscodingTask.chunkDurationSeconds());
    }

    private boolean executeTranscodingTask(
            TranscodingTask task,
            ObjectStorageClient storageClient,
            java.util.Map<Thread, Worker> workersByThread,
            TranscodingProfile[] profiles
    ) {
        Worker worker = workersByThread.get(Thread.currentThread());
        if (worker != null) {
            worker.setStatus(WorkerStatus.BUSY);
        }
        task.setStatus(Status.RUNNING);
        LOGGER.info("Worker {} picked up task {} (chunk={} profile={})",
                worker == null ? "unknown" : worker.getId(),
                task.getId(),
                task.getChunkKey(),
                task.getProfile().getName());
        emitState(task, TranscodeSegmentState.TRANSCODING, profiles);
        try {
            CompletedTranscode completed = task.transcodeToSpool(storageClient, localUploadSpoolRoot);
            if (completed == null) {
                task.setStatus(Status.SUCCEEDED);
                emitState(task, TranscodeSegmentState.DONE, profiles);
                LOGGER.info("Task {} skipped because output already exists", task.getId());
                return true;
            }
            processingUploadTaskRepository.upsertPending(
                    task.getJobId(),
                    task.getProfile().getName(),
                    parseSegmentNumber(task.getChunkKey()),
                    task.getChunkKey(),
                    completed.outputKey(),
                    completed.localPath().toString(),
                    completed.sizeBytes(),
                    completed.outputTsOffsetSeconds()
            );
            emitState(task, TranscodeSegmentState.TRANSCODED, profiles);
            task.setStatus(Status.SUCCEEDED);
            LOGGER.info("Task {} succeeded", task.getId());
            return true;
        } catch (Exception e) {
            task.setStatus(Status.FAILED);
            emitState(task, TranscodeSegmentState.FAILED, profiles);
            LOGGER.error("Task {} failed: {}", task.getId(), e.getMessage(), e);
            return false;
        } finally {
            if (worker != null) {
                worker.setStatus(WorkerStatus.IDLE);
                worker.heartbeat();
            }
        }
    }

    private void emitState(TranscodingTask task, TranscodeSegmentState state, TranscodingProfile[] profiles) {
        int segmentNumber = parseSegmentNumber(task.getChunkKey());
        if (segmentNumber < 0) {
            return;
        }
        publishTranscodeState(task.getJobId(), task.getProfile().getName(), segmentNumber, state, profiles);
    }

    private static TranscodingProfile profileFromName(String profileName, TranscodingProfile[] profiles) {
        if (profileName == null || profileName.isBlank()) {
            return null;
        }
        for (TranscodingProfile profile : profiles) {
            if (profile.getName().equalsIgnoreCase(profileName)) {
                return profile;
            }
        }
        return null;
    }

    private boolean isAlreadyDone(String videoId, String profile, int segmentNumber) {
        if (segmentNumber < 0) {
            return false;
        }
        try {
            return transcodeStatusRepository != null
                    && transcodeStatusRepository.hasState(videoId, profile, segmentNumber, TranscodeSegmentState.DONE);
        } catch (Exception e) {
            LOGGER.warn("Failed transcode-state lookup videoId={} profile={} segment={}", videoId, profile, segmentNumber, e);
            return false;
        }
    }

    private boolean hasOpenLocalUploadTask(String videoId, String profile, int segmentNumber) {
        if (segmentNumber < 0 || processingUploadTaskRepository == null) {
            return false;
        }
        try {
            return processingUploadTaskRepository.hasOpenTask(videoId, profile, segmentNumber);
        } catch (Exception e) {
            LOGGER.warn("Failed local upload-task lookup videoId={} profile={} segment={}", videoId, profile, segmentNumber, e);
            return false;
        }
    }

    private static TranscodingProfile[] defaultProfiles() {
        return new TranscodingProfile[] {TranscodingProfile.LOW, TranscodingProfile.MEDIUM, TranscodingProfile.HIGH};
    }
}
