package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.jobs.WorkerStatus;
import com.distributed26.videostreaming.shared.upload.RabbitMQStatusEventBus;
import com.distributed26.videostreaming.shared.upload.RabbitMQTranscodeTaskBus;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Entry point for the processing service.
 *
 * <p>Consumes one transcode task per RabbitMQ message and executes that work on
 * a local worker pool. Each task represents one ({@code chunk × profile})
 * combination, so different profiles for the same source chunk may run on
 * different containers. {@link UploadMetaEvent}s still arrive on the status
 * stream and are used only for manifest readiness checks.
 */
public class ProcessingServiceApplication {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingServiceApplication.class);
    private static final String MANIFEST_PROCESSOR_EXECUTOR_NAME = "AbrManifestGenerator";
    private static final Set<String> MANIFESTS_IN_FLIGHT = ConcurrentHashMap.newKeySet();
    private static volatile TranscodedSegmentStatusRepository transcodeStatusRepository;
    private static volatile VideoProcessingRepository videoProcessingRepository;
    private static volatile StatusEventBus statusBus;
    private static volatile AbrManifestService manifestServiceRef;
    private static volatile ExecutorService manifestExecutorRef;

    static final TranscodingProfile[] PROFILES = {
        TranscodingProfile.LOW,
        TranscodingProfile.MEDIUM,
        TranscodingProfile.HIGH
    };
    private static final java.util.regex.Pattern EXTINF_PATTERN = java.util.regex.Pattern.compile("^#EXTINF:([^,]+),?");

    public static void main(String[] args) throws Exception {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();

        // Storage
        StorageConfig storageConfig = new StorageConfig(
                getEnvOrDotenv(dotenv, "MINIO_ENDPOINT",    "http://localhost:9000"),
                getEnvOrDotenv(dotenv, "MINIO_ACCESS_KEY",  "minioadmin"),
                getEnvOrDotenv(dotenv, "MINIO_SECRET_KEY",  "minioadmin"),
                getEnvOrDotenv(dotenv, "MINIO_BUCKET_NAME", "uploads"),
                getEnvOrDotenv(dotenv, "MINIO_REGION",      "us-east-1")
        );
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        storageClient.ensureBucketExists();
        LOGGER.info("Storage ready — bucket={}", storageConfig.getDefaultBucketName());

        StatusEventBus statusEventBus = RabbitMQStatusEventBus.fromEnv();
        TranscodeTaskBus transcodeTaskBus = RabbitMQTranscodeTaskBus.fromEnv();
        statusBus = statusEventBus;
        AbrManifestService manifestService = new AbrManifestService(
                storageClient,
                Integer.parseInt(getEnvOrDotenv(dotenv, "ABR_MANIFEST_WAIT_SECONDS", "120"))
        );
        manifestServiceRef = manifestService;
        ExecutorService manifestExecutor = Executors.newSingleThreadExecutor(
                r -> new Thread(r, MANIFEST_PROCESSOR_EXECUTOR_NAME)
        );
        manifestExecutorRef = manifestExecutor;
        LOGGER.info("RabbitMQ status/task buses connected");

        // Executor threads run one transcode task at a time. The executor's internal queue
        // is now the local buffer between RabbitMQ intake and FFmpeg execution.
        int poolSize = Integer.parseInt(getEnvOrDotenv(dotenv, "WORKER_POOL_SIZE", "4"));
        transcodeStatusRepository = createTranscodeStatusRepository();
        videoProcessingRepository = createVideoProcessingRepository();
        List<Worker> workers = createWorkers(poolSize);
        Map<Thread, Worker> workersByThread = new ConcurrentHashMap<>();
        ThreadPoolExecutor taskExecutor = createTaskExecutor(poolSize, workers, workersByThread);
        taskExecutor.prestartAllCoreThreads();
        LOGGER.info("Started {} transcoding worker(s)", poolSize);

        transcodeTaskBus.subscribe(ev -> submitTranscodeTask(ev, taskExecutor, storageClient, workersByThread));
        statusEventBus.subscribeAll(ev -> onStatusEvent(ev, manifestService, manifestExecutor));
        recoverIncompleteVideos(storageClient, transcodeTaskBus, manifestService, manifestExecutor);

        LOGGER.info("Processing service ready — waiting for transcode tasks...");

        int port = Integer.parseInt(getEnvOrDotenv(dotenv, "PROCESSING_PORT", "8082"));
        startApp(port, workers, taskExecutor);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown: stopping workers...");
            taskExecutor.shutdownNow();
            workers.forEach(worker -> worker.setStatus(WorkerStatus.OFFLINE));
            manifestExecutor.shutdownNow();
            manifestExecutorRef = null;
            manifestServiceRef = null;
            try { transcodeTaskBus.close(); } catch (Exception e) { LOGGER.warn("Error closing transcode task bus", e); }
            try { statusEventBus.close(); } catch (Exception e) { LOGGER.warn("Error closing status event bus", e); }
        }));

        Thread.currentThread().join();
    }

    /**
     * Creates the Javalin app with health and worker-status endpoints.
     * Mirrors the {@code createUploadApp} / {@code createStatusApp} factory pattern
     * in the upload service.
     */
    static Javalin createApp(List<Worker> workers, ThreadPoolExecutor taskExecutor) {
        ensureLogsDirectory();
        Javalin app = Javalin.create();

        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET, OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type");
        });
        app.options("/*", ctx -> ctx.status(204));

        // GET /health — liveness probe used by Docker and load balancers
        app.get("/health", ctx -> ctx.json(java.util.Map.of("status", "ok")));

        // GET /workers — worker pool snapshot for operational visibility
        app.get("/workers", ctx -> {
            var snapshot = workers.stream().map(w -> java.util.Map.of(
                    "id",     w.getId(),
                    "status", w.getStatus().name()
            )).toList();
            ctx.json(java.util.Map.of(
                    "workers",   snapshot,
                    "queued",    taskExecutor.getQueue().size()
            ));
        });

        return app;
    }

    static void startApp(int port, List<Worker> workers, ThreadPoolExecutor taskExecutor) {
        Javalin app = createApp(workers, taskExecutor);
        LOGGER.info("Starting processing HTTP server on port {}", port);
        app.start(port);
    }

    private static void ensureLogsDirectory() {
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("logs"));
        } catch (java.io.IOException e) {
            LOGGER.warn("Failed to create logs directory", e);
        }
    }

    // ── Event handling ─────────────────────────────────────────────────────────

    static void onStatusEvent(JobEvent event, AbrManifestService manifestService, ExecutorService manifestExecutor) {
        String videoId = event.getJobId();

        if (event instanceof UploadMetaEvent meta) {
            if (manifestService == null || manifestExecutor == null) {
                LOGGER.warn("Ignoring UploadMetaEvent for videoId={} because manifest generator is not configured",
                        videoId);
                return;
            }
            // Build manifests once the upload service has published total source segments
            // and every transcoded output has reached shared storage.
            LOGGER.info("UploadMetaEvent: videoId={} totalSegments={} (tasks already in flight)",
                        videoId, meta.getTotalSegments());
            if (meta.getTotalSegments() > 0 && areAllProfilesDone(videoId, meta.getTotalSegments())) {
                scheduleManifestGeneration(videoId, meta.getTotalSegments(), manifestService, manifestExecutor);
            } else {
                LOGGER.info("Deferring manifest generation until all profiles are DONE for videoId={}", videoId);
            }
            return;
        }
        if (event instanceof TranscodeProgressEvent || event instanceof UploadFailedEvent || event instanceof TranscodeTaskEvent) {
            LOGGER.debug("Ignoring non-manifest status event in processing pipeline: type={}",
                    event.getClass().getSimpleName());
            return;
        }
    }

    static TranscodingTask onTranscodeTaskEvent(TranscodeTaskEvent taskEvent) {
        String videoId = taskEvent.getJobId();
        String chunkKey = taskEvent.getChunkKey();
        int segmentNumber = taskEvent.getSegmentNumber();
        if (chunkKey == null || !chunkKey.contains("/chunks/") || !chunkKey.endsWith(".ts")) {
            LOGGER.debug("Ignoring malformed transcode task chunkKey={} for videoId={}", chunkKey, videoId);
            return null;
        }
        TranscodingProfile profile = profileFromName(taskEvent.getProfile());
        if (profile == null) {
            LOGGER.warn("Ignoring transcode task with unknown profile={} for videoId={} chunk={}",
                    taskEvent.getProfile(), videoId, chunkKey);
            return null;
        }
        if (segmentNumber < 0) {
            segmentNumber = parseSegmentNumber(chunkKey);
        }
        if (isAlreadyTranscoded(videoId, profile.getName(), segmentNumber)) {
            LOGGER.info("Skipping already transcoded segment videoId={} profile={} segment={}",
                    videoId, profile.getName(), segmentNumber);
            publishTranscodeState(videoId, profile.getName(), segmentNumber, TranscodeSegmentState.DONE);
            return null;
        }
        publishTranscodeState(videoId, profile.getName(), segmentNumber, TranscodeSegmentState.QUEUED);
        return new TranscodingTask(
                UUID.randomUUID().toString(),
                videoId,
                chunkKey,
                profile,
                taskEvent.getOutputTsOffsetSeconds()
        );
    }

    // ── Startup helpers ────────────────────────────────────────────────────────

    private static void recoverIncompleteVideos(
            ObjectStorageClient storageClient,
            TranscodeTaskBus transcodeTaskBus,
            AbrManifestService manifestService,
            ExecutorService manifestExecutor
    ) {
        if (videoProcessingRepository == null) {
            LOGGER.info("Startup recovery skipped because video metadata repository is not configured");
            return;
        }
        List<String> videoIds;
        try {
            videoIds = videoProcessingRepository.findVideoIdsByStatus("PROCESSING");
        } catch (Exception e) {
            LOGGER.warn("Startup recovery failed to load PROCESSING videos", e);
            return;
        }
        if (videoIds.isEmpty()) {
            LOGGER.info("Startup recovery found no PROCESSING videos");
            return;
        }
        LOGGER.info("Startup recovery inspecting {} PROCESSING video(s)", videoIds.size());
        for (String videoId : videoIds) {
            recoverVideo(videoId, storageClient, transcodeTaskBus, manifestService, manifestExecutor);
        }
    }

    private static void recoverVideo(
            String videoId,
            ObjectStorageClient storageClient,
            TranscodeTaskBus transcodeTaskBus,
            AbrManifestService manifestService,
            ExecutorService manifestExecutor
    ) {
        try {
            List<String> chunkKeys = listSourceChunkKeys(videoId, storageClient);
            if (chunkKeys.isEmpty()) {
                LOGGER.info("Startup recovery: no source chunks found for videoId={}", videoId);
                return;
            }

            int totalSegments = Math.max(findTotalSegments(videoId), chunkKeys.size());
            Map<String, Set<Integer>> doneSegmentsByProfile = loadDoneSegmentsByProfile(videoId);
            int republished = 0;
            Set<String> touchedProfiles = new HashSet<>();

            Map<Integer, Double> offsetsBySegment = loadSourceSegmentOffsets(videoId, storageClient);
            for (String chunkKey : chunkKeys) {
                int segmentNumber = parseSegmentNumber(chunkKey);
                if (segmentNumber < 0) {
                    LOGGER.warn("Startup recovery: skipping chunk with unparseable segment number videoId={} key={}",
                            videoId, chunkKey);
                    continue;
                }
                for (TranscodingProfile profile : PROFILES) {
                    Set<Integer> doneSegments = doneSegmentsByProfile.getOrDefault(profile.getName(), Set.of());
                    if (doneSegments.contains(segmentNumber)) {
                        continue;
                    }
                    transcodeTaskBus.publish(new TranscodeTaskEvent(
                            videoId,
                            chunkKey,
                            profile.getName(),
                            segmentNumber,
                            offsetsBySegment.getOrDefault(segmentNumber, fallbackOffsetForSegment(segmentNumber))
                    ));
                    touchedProfiles.add(profile.getName());
                    republished += 1;
                }
            }

            if (republished > 0) {
                LOGGER.info("Startup recovery requeued {} missing transcode task(s) for videoId={} profiles={}",
                        republished, videoId, touchedProfiles);
            } else {
                LOGGER.info("Startup recovery found no missing transcode tasks for videoId={}", videoId);
            }

            if (totalSegments > 0 && areAllProfilesDone(videoId, totalSegments)) {
                scheduleManifestGeneration(videoId, totalSegments, manifestService, manifestExecutor);
            }
        } catch (Exception e) {
            LOGGER.warn("Startup recovery failed for videoId={}", videoId, e);
        }
    }

    private static List<String> listSourceChunkKeys(String videoId, ObjectStorageClient storageClient) {
        String prefix = videoId + "/chunks/";
        List<String> chunkKeys = new ArrayList<>(storageClient.listFiles(prefix).stream()
                .filter(key -> key.endsWith(".ts"))
                .toList());
        chunkKeys.sort(Comparator.comparingInt(ProcessingServiceApplication::parseSegmentNumber));
        return chunkKeys;
    }

    private static Map<String, Set<Integer>> loadDoneSegmentsByProfile(String videoId) {
        Map<String, Set<Integer>> doneSegmentsByProfile = new HashMap<>();
        if (transcodeStatusRepository == null) {
            return doneSegmentsByProfile;
        }
        for (TranscodingProfile profile : PROFILES) {
            try {
                doneSegmentsByProfile.put(
                        profile.getName(),
                        transcodeStatusRepository.findSegmentNumbersByState(
                                videoId,
                                profile.getName(),
                                TranscodeSegmentState.DONE
                        )
                );
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load DONE segments videoId={} profile={}",
                        videoId, profile.getName(), e);
            }
        }
        return doneSegmentsByProfile;
    }

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) { return envVal; }
        String dotenvVal = dotenv.get(key);
        return (dotenvVal == null || dotenvVal.isBlank()) ? defaultValue : dotenvVal;
    }

    /** Clears accumulated per-video state. Package-private for use in tests only. */
    static void resetState() {
        MANIFESTS_IN_FLIGHT.clear();
    }

    static void publishTranscodeState(String videoId, String profile, int segmentNumber, TranscodeSegmentState state) {
        if (transcodeStatusRepository == null || statusBus == null || segmentNumber < 0) {
            return;
        }
        try {
            transcodeStatusRepository.upsertState(videoId, profile, segmentNumber, state);
            int done = transcodeStatusRepository.countByState(videoId, profile, TranscodeSegmentState.DONE);
            int total = findTotalSegments(videoId);
            if (state == TranscodeSegmentState.DONE) {
                done = transcodeStatusRepository.countByState(videoId, profile, TranscodeSegmentState.DONE);
            }
            statusBus.publish(new TranscodeProgressEvent(videoId, profile, segmentNumber, state, done, total));
            if (state == TranscodeSegmentState.DONE
                    && total > 0
                    && !MANIFESTS_IN_FLIGHT.contains(videoId)
                    && areAllProfilesDone(videoId, total)) {
                scheduleManifestGeneration(videoId, total, manifestServiceRef, manifestExecutorRef);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to persist/publish transcode progress videoId={} profile={} segment={} state={}",
                    videoId, profile, segmentNumber, state, e);
        }
    }

    private static List<Worker> createWorkers(int poolSize) {
        List<Worker> workers = new ArrayList<>(poolSize);
        java.time.Instant now = java.time.Instant.now();
        for (int i = 0; i < poolSize; i++) {
            workers.add(new Worker("worker-" + i, now));
        }
        return Collections.unmodifiableList(workers);
    }

    private static ThreadPoolExecutor createTaskExecutor(int poolSize, List<Worker> workers, Map<Thread, Worker> workersByThread) {
        ThreadFactory factory = new ThreadFactory() {
            private int index = 0;

            @Override
            public synchronized Thread newThread(Runnable runnable) {
                Worker worker = workers.get(index);
                Thread thread = new Thread(runnable, "processing-worker-" + index);
                workersByThread.put(thread, worker);
                index++;
                return thread;
            }
        };
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory
        );
    }

    private static CompletionStage<Boolean> submitTranscodeTask(
            TranscodeTaskEvent taskEvent,
            ThreadPoolExecutor taskExecutor,
            ObjectStorageClient storageClient,
            Map<Thread, Worker> workersByThread
    ) {
        TranscodingTask task = onTranscodeTaskEvent(taskEvent);
        if (task == null) {
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.supplyAsync(
                () -> executeTranscodingTask(task, storageClient, workersByThread),
                taskExecutor
        ).exceptionally(e -> {
            LOGGER.error("Transcode task execution crashed jobId={} chunk={} profile={}",
                    task.getJobId(), task.getChunkKey(), task.getProfile().getName(), e);
            return false;
        });
    }

    private static boolean executeTranscodingTask(
            TranscodingTask task,
            ObjectStorageClient storageClient,
            Map<Thread, Worker> workersByThread
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
        emitState(task, TranscodeSegmentState.TRANSCODING);
        try {
            task.execute(storageClient, () -> emitState(task, TranscodeSegmentState.UPLOADING));
            task.setStatus(Status.SUCCEEDED);
            emitState(task, TranscodeSegmentState.DONE);
            LOGGER.info("Task {} succeeded", task.getId());
            return true;
        } catch (Exception e) {
            task.setStatus(Status.FAILED);
            emitState(task, TranscodeSegmentState.FAILED);
            LOGGER.error("Task {} failed: {}", task.getId(), e.getMessage(), e);
            return false;
        } finally {
            if (worker != null) {
                worker.setStatus(WorkerStatus.IDLE);
                worker.heartbeat();
            }
        }
    }

    private static void emitState(TranscodingTask task, TranscodeSegmentState state) {
        int segmentNumber = parseSegmentNumber(task.getChunkKey());
        if (segmentNumber < 0) {
            return;
        }
        publishTranscodeState(
                task.getJobId(),
                task.getProfile().getName(),
                segmentNumber,
                state
        );
    }

    private static int findTotalSegments(String videoId) {
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

    private static boolean areAllProfilesDone(String videoId, int totalSegments) {
        if (transcodeStatusRepository == null || totalSegments <= 0) {
            return false;
        }
        for (TranscodingProfile profile : PROFILES) {
            int done = transcodeStatusRepository.countByState(videoId, profile.getName(), TranscodeSegmentState.DONE);
            if (done < totalSegments) {
                return false;
            }
        }
        return true;
    }

    private static void scheduleManifestGeneration(
            String videoId,
            int totalSegments,
            AbrManifestService manifestService,
            ExecutorService manifestExecutor
    ) {
        if (manifestService == null || manifestExecutor == null) {
            LOGGER.warn("Cannot schedule manifest generation for videoId={} because manifest generator is not configured",
                    videoId);
            return;
        }
        if (!MANIFESTS_IN_FLIGHT.add(videoId)) {
            LOGGER.debug("Manifest generation already running/skipped for videoId={}", videoId);
            return;
        }
        try {
            manifestExecutor.execute(() -> {
                try {
                    manifestService.generateIfNeeded(videoId, totalSegments);
                } catch (Exception e) {
                    LOGGER.error("Manifest generation failed for videoId={}", videoId, e);
                } finally {
                    MANIFESTS_IN_FLIGHT.remove(videoId);
                }
            });
        } catch (RuntimeException e) {
            MANIFESTS_IN_FLIGHT.remove(videoId);
            LOGGER.error("Failed to submit manifest generation task for videoId={}", videoId, e);
        }
    }

    private static int parseSegmentNumber(String chunkKey) {
        if (chunkKey == null || chunkKey.isBlank()) {
            return -1;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(\\d+)").matcher(chunkKey);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last;
    }

    private static Map<Integer, Double> loadSourceSegmentOffsets(String videoId, ObjectStorageClient storageClient) {
        Map<Integer, Double> offsetsBySegment = new HashMap<>();
        String manifestKey = videoId + "/chunks/output.m3u8";
        try {
            if (!storageClient.fileExists(manifestKey)) {
                return offsetsBySegment;
            }
            try (InputStream is = storageClient.downloadFile(manifestKey)) {
                String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                double pendingDuration = 0d;
                double runningOffset = 0d;
                for (String line : content.split("\\R")) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    if (trimmed.startsWith("#")) {
                        if (trimmed.startsWith("#EXTINF:")) {
                            pendingDuration = parseExtinfDuration(trimmed);
                        }
                        continue;
                    }
                    if (!trimmed.endsWith(".ts")) {
                        continue;
                    }
                    int segmentNumber = parseSegmentNumber(trimmed);
                    if (segmentNumber >= 0) {
                        offsetsBySegment.put(segmentNumber, runningOffset);
                    }
                    runningOffset += pendingDuration;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to load source segment offsets for videoId={}", videoId, e);
        }
        return offsetsBySegment;
    }

    private static double parseExtinfDuration(String line) {
        java.util.regex.Matcher matcher = EXTINF_PATTERN.matcher(line);
        if (!matcher.find()) {
            return 0d;
        }
        try {
            return Double.parseDouble(matcher.group(1).trim());
        } catch (NumberFormatException e) {
            return 0d;
        }
    }

    private static double fallbackOffsetForSegment(int segmentNumber) {
        return segmentNumber < 0 ? 0d : (double) segmentNumber * Math.max(1, TranscodingTask.CHUNK_DURATION_SECONDS);
    }

    private static TranscodingProfile profileFromName(String profileName) {
        if (profileName == null || profileName.isBlank()) {
            return null;
        }
        for (TranscodingProfile profile : PROFILES) {
            if (profile.getName().equalsIgnoreCase(profileName)) {
                return profile;
            }
        }
        return null;
    }

    private static boolean isAlreadyTranscoded(String videoId, String profile, int segmentNumber) {
        if (transcodeStatusRepository == null || segmentNumber < 0) {
            return false;
        }
        try {
            return transcodeStatusRepository.hasState(videoId, profile, segmentNumber, TranscodeSegmentState.DONE);
        } catch (Exception e) {
            LOGGER.warn("Failed transcode-state lookup videoId={} profile={} segment={}",
                    videoId, profile, segmentNumber, e);
            return false;
        }
    }

    private static TranscodedSegmentStatusRepository createTranscodeStatusRepository() {
        try {
            return TranscodedSegmentStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; transcoding progress disabled: {}", e.getMessage());
            return null;
        }
    }

    private static VideoProcessingRepository createVideoProcessingRepository() {
        try {
            return VideoProcessingRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; video metadata lookups disabled: {}", e.getMessage());
            return null;
        }
    }
}
