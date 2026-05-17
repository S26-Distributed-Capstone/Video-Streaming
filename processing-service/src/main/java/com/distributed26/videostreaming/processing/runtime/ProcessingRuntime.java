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
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;

import net.bramp.ffmpeg.builder.FFmpegBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ProcessingRuntime {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingRuntime.class);
    private static final java.util.regex.Pattern SEGMENT_NUMBER_PATTERN = java.util.regex.Pattern.compile("(\\d+)");
    private static final long DEFAULT_CLAIM_STALE_MILLIS = 10_000L;
    private static final int MAX_TRANSCODE_ATTEMPTS = 5;  // Skip after this many failures
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
    private FailedVideoRegistry failedVideoRegistry;
    private final String processorInstanceId;
    private final long claimStaleMillis;
    private final ScheduledExecutorService claimHeartbeatExecutor;

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
        this(
                transcodeStatusRepository,
                videoProcessingRepository,
                processingUploadTaskRepository,
                processingTaskClaimRepository,
                statusBus,
                transcodeTaskBus,
                manifestService,
                manifestExecutor,
                localUploadSpoolRoot,
                processorInstanceId,
                DEFAULT_CLAIM_STALE_MILLIS
        );
    }

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
            String processorInstanceId,
            long claimStaleMillis
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
        this.failedVideoRegistry = new FailedVideoRegistry();
        this.processorInstanceId = processorInstanceId;
        this.claimStaleMillis = Math.max(0L, claimStaleMillis);
        this.claimHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "processing-claim-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
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
        failedVideoRegistry = new FailedVideoRegistry();
        claimHeartbeatExecutor.shutdownNow();
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

    public void setVideoProcessingRepository(VideoProcessingRepository repository) {
        videoProcessingRepository = repository;
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
        if (event instanceof UploadFailedEvent failed) {
            markVideoFailed(failed.getJobId());
            LOGGER.info("Marked videoId={} cancelled from failure event reason={}", failed.getJobId(), failed.getReason());
        }
        if (isVideoFailed(videoId)) {
            LOGGER.info("Ignoring status event for failed videoId={} type={}",
                    videoId, event.getClass().getSimpleName());
            return;
        }

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
        if (isVideoFailed(videoId)) {
            LOGGER.info("Skipping transcode task for failed videoId={} profile={} chunk={}",
                    videoId, taskEvent.getProfile(), taskEvent.getChunkKey());
            return null;
        }
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
        if (hasActiveClaim(videoId, profile.getName(), segmentNumber)) {
            LOGGER.info("Skipping segment already claimed by another processing instance videoId={} profile={} segment={}",
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
        if (isVideoFailed(taskEvent.getJobId())) {
            LOGGER.info("Dropping submitted transcode task for failed videoId={} profile={} chunk={}",
                    taskEvent.getJobId(), taskEvent.getProfile(), taskEvent.getChunkKey());
            return CompletableFuture.completedFuture(true);
        }
        TranscodingTask task = onTranscodeTaskEvent(taskEvent, profiles);
        if (task == null) {
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.supplyAsync(
                () -> executeTranscodingTask(task, storageClient, workersByThread, profiles),
                taskExecutor
        ).exceptionally(e -> {
            LOGGER.error("Transcode task execution crashed jobId={} chunk={} profile={}",
                    task.getJobId(), task.getChunkKey(), task.getProfile().getName(), e);
            return false;
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

    public long claimStaleMillis() {
        return claimStaleMillis;
    }

    public long claimHeartbeatMillis() {
        if (claimStaleMillis <= 0L) {
            return 5_000L;
        }
        return Math.max(1_000L, claimStaleMillis / 2L);
    }

    public VideoProcessingRepository videoProcessingRepository() {
        return videoProcessingRepository;
    }

    public StatusEventBus statusBus() {
        return statusBus;
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

    public Optional<String> findVideoStatus(String videoId) {
        if (videoProcessingRepository == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(videoProcessingRepository.findStatusByVideoId(videoId))
                    .orElse(Optional.empty());
        } catch (Exception e) {
            LOGGER.warn("Failed to load status for videoId={}", videoId, e);
            return Optional.empty();
        }
    }

    public boolean hasRequiredManifests(String videoId) {
        if (manifestServiceRef == null) {
            return false;
        }
        try {
            return manifestServiceRef.hasRequiredManifests(videoId);
        } catch (Exception e) {
            LOGGER.warn("Failed to verify manifests for videoId={}", videoId, e);
            return false;
        }
    }

    public void scheduleManifestGeneration(String videoId, int totalSegments) {
        if (manifestServiceRef == null || manifestExecutorRef == null) {
            LOGGER.warn("Cannot schedule manifest generation for videoId={} because manifest generator is not configured",
                    videoId);
            return;
        }
        Optional<String> currentStatus = findVideoStatus(videoId);
        if (currentStatus.isPresent()
                && "COMPLETED".equalsIgnoreCase(currentStatus.get())
                && hasRequiredManifests(videoId)) {
            LOGGER.debug("Skipping manifest reconciliation for already completed videoId={}", videoId);
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

    public boolean isVideoFailed(String videoId) {
        if (failedVideoRegistry == null || !failedVideoRegistry.isFailed(videoId)) {
            return false;
        }
        if (videoProcessingRepository == null) {
            return true;
        }
        try {
            if (videoProcessingRepository.isFailed(videoId)) {
                return true;
            }
            failedVideoRegistry.clear(videoId);
            LOGGER.info("Cleared stale failed marker for videoId={} after new processing attempt", videoId);
            return false;
        } catch (Exception e) {
            LOGGER.warn("Failed to reconcile failed-state for videoId={}", videoId, e);
            return true;
        }
    }

    public void markVideoFailed(String videoId) {
        if (failedVideoRegistry == null) {
            failedVideoRegistry = new FailedVideoRegistry();
        }
        failedVideoRegistry.markFailed(videoId);
    }

    private void markVideoProcessing(String videoId) {
        if (videoProcessingRepository == null || videoId == null || videoId.isBlank()) {
            return;
        }
        try {
            videoProcessingRepository.markProcessingIfPending(videoId);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to mark video PROCESSING for videoId={}", videoId, e);
        }
    }

    private boolean executeTranscodingTask(
            TranscodingTask task,
            ObjectStorageClient storageClient,
            java.util.Map<Thread, Worker> workersByThread,
            TranscodingProfile[] profiles
    ) {
        Worker worker = workersByThread.get(Thread.currentThread());
        int segmentNumber = parseSegmentNumber(task.getChunkKey());
        boolean claimAcquired = false;
        try {
            if (worker != null) {
                worker.setStatus(WorkerStatus.BUSY);
            }
            if (isVideoFailed(task.getJobId())) {
                LOGGER.info("Skipping queued transcode task for failed videoId={} profile={} chunk={}",
                        task.getJobId(), task.getProfile().getName(), task.getChunkKey());
                return true;
            }
            
            if (processingTaskClaimRepository != null) {
                ProcessingTaskClaimRepository.ClaimResult claimResult = processingTaskClaimRepository.claim(
                        task.getJobId(),
                        task.getProfile().getName(),
                        segmentNumber,
                        "TRANSCODING",
                        processorInstanceId,
                        claimStaleMillis
                );
                if (claimResult != ProcessingTaskClaimRepository.ClaimResult.ACQUIRED) {
                    if (claimResult == ProcessingTaskClaimRepository.ClaimResult.HELD_BY_OTHER) {
                        LOGGER.info("Skipping execution of transcode task already claimed elsewhere videoId={} profile={} chunk={}",
                                task.getJobId(), task.getProfile().getName(), task.getChunkKey());
                    }
                    return true;
                }
                claimAcquired = true;
            }
            markVideoProcessing(task.getJobId());
            task.setStatus(Status.RUNNING);
            LOGGER.info("Worker {} picked up task {} (chunk={} profile={})",
                    worker == null ? "unknown" : worker.getId(),
                    task.getId(),
                    task.getChunkKey(),
                    task.getProfile().getName());
            emitState(task, TranscodeSegmentState.TRANSCODING, profiles);
            try (ClaimHeartbeat ignored = startClaimHeartbeat(
                    task.getJobId(),
                    task.getProfile().getName(),
                    segmentNumber,
                    "TRANSCODING"
            )) {
                CompletedTranscode completed = task.transcodeToSpool(storageClient, localUploadSpoolRoot);
                if (completed == null) {
                    return finishSuccessfulTranscode(task, profiles, segmentNumber, null);
                }
                if (isVideoFailed(task.getJobId())) {
                    Files.deleteIfExists(completed.localPath());
                    LOGGER.info("Discarded transcoded spool for failed videoId={} profile={} segment={}",
                            task.getJobId(), task.getProfile().getName(), segmentNumber);
                    return true;
                }
                processingUploadTaskRepository.upsertPending(
                        task.getJobId(),
                        processorInstanceId,
                        task.getProfile().getName(),
                        segmentNumber,
                        task.getChunkKey(),
                        completed.outputKey(),
                        completed.localPath().toString(),
                        completed.sizeBytes(),
                        completed.outputTsOffsetSeconds()
                );
                return finishSuccessfulTranscode(task, profiles, segmentNumber, completed);
            }
        } catch (Exception e) {
            task.setStatus(Status.FAILED);
            emitState(task, TranscodeSegmentState.FAILED, profiles);
            LOGGER.error("Task {} failed: {} (chunk={})", task.getId(), e.getMessage(), task.getChunkKey(), e);
            
            // Check if THIS SEGMENT has failed too many times already - if so, attempt recovery
            if (segmentNumber >= 0 && transcodeStatusRepository != null) {
                try {
                    int failureCount = transcodeStatusRepository.countFailuresForSegment(
                            task.getJobId(),
                            task.getProfile().getName(),
                            segmentNumber
                    );
                    if (failureCount >= MAX_TRANSCODE_ATTEMPTS) {
                        LOGGER.warn("Segment has failed {} times (limit={}), attempting automatic recovery: videoId={} profile={} segment={}",
                                failureCount, MAX_TRANSCODE_ATTEMPTS, task.getJobId(), task.getProfile().getName(), segmentNumber);
                        // Attempt recovery: re-encode the segment with safe settings
                        try {
                            publishTranscodeState(task.getJobId(), task.getProfile().getName(), segmentNumber, TranscodeSegmentState.RECOVERY_IN_PROGRESS, profiles);
                            boolean recoverySucceeded = attemptSegmentRecovery(
                                    task.getJobId(),
                                    task.getChunkKey(),
                                    storageClient
                            );
                            if (recoverySucceeded) {
                                // Clear failure count and re-queue for retry
                                transcodeStatusRepository.clearFailuresForSegment(
                                        task.getJobId(),
                                        task.getProfile().getName(),
                                        segmentNumber
                                );
                                LOGGER.info("Segment recovery succeeded, resetting failure count and retrying: videoId={} profile={} segment={}",
                                        task.getJobId(), task.getProfile().getName(), segmentNumber);
                                emitState(task, TranscodeSegmentState.QUEUED, profiles);
                                task.setStatus(Status.QUEUED);

                                // Retry immediately in the same worker so the recovered segment does not
                                // sit in the queue and leave the UI stuck at 39/40 while waiting.
                                CompletedTranscode recovered = task.transcodeToSpool(storageClient, localUploadSpoolRoot);
                                if (recovered == null) {
                                    return finishSuccessfulTranscode(task, profiles, segmentNumber, null);
                                }
                                processingUploadTaskRepository.upsertPending(
                                    task.getJobId(),
                                    processorInstanceId,
                                    task.getProfile().getName(),
                                    segmentNumber,
                                    task.getChunkKey(),
                                    recovered.outputKey(),
                                    recovered.localPath().toString(),
                                    recovered.sizeBytes(),
                                    recovered.outputTsOffsetSeconds()
                                );
                                return finishSuccessfulTranscode(task, profiles, segmentNumber, recovered);
                            } else {
                                LOGGER.warn("Segment recovery failed, giving up: videoId={} profile={} segment={}",
                                        task.getJobId(), task.getProfile().getName(), segmentNumber);
                                return true;  // Give up
                            }
                        } catch (Exception recoveryError) {
                            LOGGER.warn("Segment recovery attempt failed with exception: videoId={} profile={} segment={}",
                                    task.getJobId(), task.getProfile().getName(), segmentNumber, recoveryError);
                            return true;  // Give up
                        }
                    }
                } catch (Exception dbError) {
                    LOGGER.warn("Failed to check failure count, will retry anyway", dbError);
                }
            }
            return false;  // Re-queue the task for retry
        } finally {
            if (claimAcquired && processingTaskClaimRepository != null) {
                processingTaskClaimRepository.release(
                        task.getJobId(),
                        task.getProfile().getName(),
                        segmentNumber
                );
            }
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

    private boolean finishSuccessfulTranscode(
            TranscodingTask task,
            TranscodingProfile[] profiles,
            int segmentNumber,
            CompletedTranscode completed
    ) {
        if (completed == null) {
            LOGGER.info("Task {} skipped because output already exists", task.getId());
        } else {
            LOGGER.info("Task {} succeeded", task.getId());
        }
        emitState(task, TranscodeSegmentState.TRANSCODED, profiles);
        task.setStatus(Status.SUCCEEDED);
        return true;
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

    private boolean hasActiveClaim(String videoId, String profile, int segmentNumber) {
        if (segmentNumber < 0 || processingTaskClaimRepository == null) {
            return false;
        }
        try {
            return processingTaskClaimRepository.hasActiveClaim(videoId, profile, segmentNumber, claimStaleMillis);
        } catch (Exception e) {
            LOGGER.warn("Failed processing-claim lookup videoId={} profile={} segment={}", videoId, profile, segmentNumber, e);
            return false;
        }
    }

    public AutoCloseable startUploadClaimHeartbeat(String videoId, String profile, int segmentNumber) {
        return startClaimHeartbeat(videoId, profile, segmentNumber, "UPLOADING");
    }

    private ClaimHeartbeat startClaimHeartbeat(String videoId, String profile, int segmentNumber, String stage) {
        if (segmentNumber < 0 || processingTaskClaimRepository == null) {
            return ClaimHeartbeat.NOOP;
        }
        ClaimHeartbeat heartbeat = new ClaimHeartbeat(
                videoId,
                profile,
                segmentNumber,
                stage,
                processorInstanceId,
                processingTaskClaimRepository,
                claimHeartbeatExecutor,
                claimHeartbeatMillis()
        );
        heartbeat.start();
        return heartbeat;
    }

    private static final class ClaimHeartbeat implements AutoCloseable {
        private static final ClaimHeartbeat NOOP = new ClaimHeartbeat();

        private final AtomicBoolean running;
        private final ScheduledFuture<?> future;

        private ClaimHeartbeat() {
            this.running = null;
            this.future = null;
        }

        private ClaimHeartbeat(
                String videoId,
                String profile,
                int segmentNumber,
                String stage,
                String claimedBy,
                ProcessingTaskClaimRepository repository,
                ScheduledExecutorService scheduler,
                long intervalMillis
        ) {
            this.running = new AtomicBoolean(true);
            this.future = scheduler.scheduleAtFixedRate(() -> {
                if (!running.get()) {
                    return;
                }
                repository.heartbeat(videoId, profile, segmentNumber, stage, claimedBy);
            }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        }

        private void start() {
            // Scheduled on construction.
        }

        @Override
        public void close() {
            if (running == null || future == null) {
                return;
            }
            running.set(false);
            future.cancel(false);
        }
    }

    private static TranscodingProfile[] defaultProfiles() {
        return new TranscodingProfile[] {TranscodingProfile.LOW, TranscodingProfile.MEDIUM, TranscodingProfile.HIGH};
    }

    /**
     * Attempts to recover a malformed segment by downloading it, re-encoding with
     * safe x264 settings (strong keyframe forcing), and re-uploading in-place.
     *
     * @return true if recovery succeeded, false otherwise
     */
    private boolean attemptSegmentRecovery(
            String videoId,
            String chunkKey,
            ObjectStorageClient storageClient
    ) {
        Path inputTemp = null;
        Path outputTemp = null;
        try {
            inputTemp = Files.createTempFile("recover-in-", ".ts");
            outputTemp = Files.createTempFile("recover-out-", ".ts");

            // Download the existing segment
            LOGGER.info("Recovery: downloading malformed segment chunk={}", chunkKey);
            try (java.io.InputStream is = storageClient.downloadFile(chunkKey)) {
                Files.copy(is, inputTemp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }

            // Re-encode with safe settings: strong keyframe forcing, explicit encoding, aac audio
            LOGGER.info("Recovery: re-encoding segment with safe settings chunk={}", chunkKey);
            int keyint = 10 * 30;  // Keyframe every 10 seconds at 30fps
            FFmpegBuilder builder = new FFmpegBuilder()
                    .setInput(inputTemp.toString())
                    .addOutput(outputTemp.toString())
                    .setFormat("mpegts")
                    .addExtraArgs("-c:v", "libx264")
                    .addExtraArgs("-preset", "fast")  // Fast preset for recovery, not ultrafast
                    .addExtraArgs("-crf", "21")  // Higher quality for recovery
                    .addExtraArgs("-pix_fmt", "yuv420p")
                    .addExtraArgs("-x264-params", "keyint=" + keyint + ":scenecut=0")
                    .addExtraArgs("-force_key_frames", "expr:gte(t,n_forced*1)")  // Keyframe every second
                    .addExtraArgs("-c:a", "aac")
                    .addExtraArgs("-b:a", "128k")
                    .addExtraArgs("-ac", "2")
                    .addExtraArgs("-ar", "48000")
                    .done();

            runRecoveryFfmpeg(builder, chunkKey);

            // Upload recovered segment back in-place
            long size = Files.size(outputTemp);
            LOGGER.info("Recovery: uploading recovered segment ({} bytes) chunk={}", size, chunkKey);
            try (java.io.FileInputStream is = new java.io.FileInputStream(outputTemp.toFile())) {
                storageClient.uploadFile(chunkKey, is, size);
            }

            LOGGER.info("Recovery: segment recovery completed successfully chunk={}", chunkKey);
            return true;

        } catch (Exception e) {
            LOGGER.error("Recovery: segment recovery failed chunk={}", chunkKey, e);
            return false;
        } finally {
            if (inputTemp != null) {
                try {
                    Files.deleteIfExists(inputTemp);
                } catch (java.io.IOException ignored) {
                }
            }
            if (outputTemp != null) {
                try {
                    Files.deleteIfExists(outputTemp);
                } catch (java.io.IOException ignored) {
                }
            }
        }
    }

    /**
     * Runs FFmpeg for segment recovery with diagnostic logging.
     */
    private void runRecoveryFfmpeg(FFmpegBuilder builder, String chunkKey) throws java.io.IOException {
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add("ffmpeg");
        command.addAll(builder.build());

        LOGGER.debug("Recovery FFmpeg command: {}", () -> String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);
        Process process;
        try {
            process = pb.start();
        } catch (java.io.IOException e) {
            throw new java.io.IOException("Failed to start FFmpeg for segment recovery chunk=" + chunkKey + ": " + e.getMessage(), e);
        }

        // Read stdout and stderr in parallel to prevent pipe buffer deadlocks
        CompletableFuture<String> stdoutFuture = CompletableFuture.supplyAsync(() -> {
            try { return new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8); }
            catch (java.io.IOException e) { return "[failed to read stdout: " + e.getMessage() + "]"; }
        });
        CompletableFuture<String> stderrFuture = CompletableFuture.supplyAsync(() -> {
            try { return new String(process.getErrorStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8); }
            catch (java.io.IOException e) { return "[failed to read stderr: " + e.getMessage() + "]"; }
        });

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new java.io.IOException("FFmpeg interrupted for segment recovery chunk=" + chunkKey, e);
        }

        String stdout = stdoutFuture.join();
        String stderr = stderrFuture.join();

        if (exitCode != 0) {
            String stderrTrunc = stderr.length() > 2000
                    ? "…(truncated)…\n" + stderr.substring(stderr.length() - 2000)
                    : stderr;
            LOGGER.error("Recovery FFmpeg failed exit={} chunk={}.\nstderr:\n{}", exitCode, chunkKey, stderrTrunc);
            throw new java.io.IOException("Recovery FFmpeg returned exit code " + exitCode + " for chunk=" + chunkKey);
        }

        LOGGER.debug("Recovery FFmpeg completed successfully chunk={}", chunkKey);
    }
}
