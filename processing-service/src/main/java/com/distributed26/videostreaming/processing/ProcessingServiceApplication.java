package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.RabbitMQJobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Entry point for the processing service.
 *
 * <p>Listens on the {@code upload.events} TOPIC exchange for {@link JobTaskEvent}s
 * published by the upload service. For each received video segment (chunk), a
 * {@link TranscodingTask} is queued immediately for every (chunk × profile)
 * combination and executed by the worker pool. {@link UploadMetaEvent}s may be
 * consumed for auxiliary metadata, but are not used to defer queuing until all
 * segments have arrived.
 *
 * <p>Required .env additions:
 * <pre>
 *   SERVICE_MODE=processing
 *   RABBITMQ_STATUS_QUEUE=processing.events.queue
 *   RABBITMQ_STATUS_BINDING=upload.status.*
 * </pre>
 */
public class ProcessingServiceApplication {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingServiceApplication.class);
    private static final String MANIFEST_PROCESSOR_EXECUTOR_NAME = "AbrManifestGenerator";
    private static final Set<String> MANIFESTS_IN_FLIGHT = ConcurrentHashMap.newKeySet();
    private static final Map<String, Integer> TOTAL_SEGMENTS_BY_VIDEO = new ConcurrentHashMap<>();
    private static volatile TranscodedSegmentStatusRepository transcodeStatusRepository;
    private static volatile RabbitMQJobTaskBus statusBus;
    private static volatile AbrManifestService manifestServiceRef;
    private static volatile ExecutorService manifestExecutorRef;

    static final TranscodingProfile[] PROFILES = {
        TranscodingProfile.LOW,
        TranscodingProfile.MEDIUM,
        TranscodingProfile.HIGH
    };

    /** Chunk keys already queued for transcoding, keyed by videoId. Used for dedup only. */
    private static final Map<String, Set<String>> CHUNKS_BY_VIDEO = new ConcurrentHashMap<>();

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

        // RabbitMQ bus — fromEnv() reads SERVICE_MODE; any value other than
        // "upload" enables the AMQP consumer.  Set SERVICE_MODE=processing and
        // use a dedicated RABBITMQ_STATUS_QUEUE so the processing service gets
        // its own copy of every upload.status.* message (TOPIC fan-out).
        RabbitMQJobTaskBus bus = RabbitMQJobTaskBus.fromEnv();
        statusBus = bus;
        AbrManifestService manifestService = new AbrManifestService(
                storageClient,
                Integer.parseInt(getEnvOrDotenv(dotenv, "ABR_MANIFEST_WAIT_SECONDS", "120"))
        );
        manifestServiceRef = manifestService;
        ExecutorService manifestExecutor = Executors.newSingleThreadExecutor(
                r -> new Thread(r, MANIFEST_PROCESSOR_EXECUTOR_NAME)
        );
        manifestExecutorRef = manifestExecutor;
        LOGGER.info("RabbitMQJobTaskBus connected");

        // Worker pool
        int poolSize = Integer.parseInt(getEnvOrDotenv(dotenv, "WORKER_POOL_SIZE", "4"));
        BlockingQueue<TranscodingTask> taskQueue = new LinkedBlockingQueue<>();
        transcodeStatusRepository = createTranscodeStatusRepository();
        List<TranscodingWorker> workers = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            TranscodingWorker w = new TranscodingWorker("worker-" + i, storageClient, taskQueue, bus, transcodeStatusRepository);
            w.start();
            workers.add(w);
        }
        LOGGER.info("Started {} transcoding worker(s)", poolSize);

        // subscribeAll ensures every incoming event reaches onEvent() regardless
        // of whether we've seen that videoId before — fixing the first-event drop.
        bus.subscribeAll(ev -> onEvent(ev, taskQueue, manifestService, manifestExecutor));

        LOGGER.info("Processing service ready — waiting for upload events...");

        int port = Integer.parseInt(getEnvOrDotenv(dotenv, "PROCESSING_PORT", "8082"));
        startApp(port, workers, taskQueue);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown: stopping workers...");
            workers.forEach(TranscodingWorker::stop);
            manifestExecutor.shutdownNow();
            manifestExecutorRef = null;
            manifestServiceRef = null;
            try { bus.close(); } catch (Exception e) { LOGGER.warn("Error closing bus", e); }
        }));

        Thread.currentThread().join();
    }

    /**
     * Creates the Javalin app with health and worker-status endpoints.
     * Mirrors the {@code createUploadApp} / {@code createStatusApp} factory pattern
     * in the upload service.
     */
    static Javalin createApp(List<TranscodingWorker> workers, BlockingQueue<TranscodingTask> taskQueue) {
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
                    "queued",    taskQueue.size()
            ));
        });

        return app;
    }

    static void startApp(int port, List<TranscodingWorker> workers, BlockingQueue<TranscodingTask> taskQueue) {
        Javalin app = createApp(workers, taskQueue);
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

    static void onEvent(JobTaskEvent event, BlockingQueue<TranscodingTask> taskQueue,
                        RabbitMQJobTaskBus bus) {
        onEvent(event, taskQueue, null, null);
    }

    static void onEvent(JobTaskEvent event, BlockingQueue<TranscodingTask> taskQueue,
                        AbrManifestService manifestService, ExecutorService manifestExecutor) {
        String videoId = event.getJobId();

        if (event instanceof UploadMetaEvent meta) {
            if (manifestService == null || manifestExecutor == null) {
                LOGGER.warn("Ignoring UploadMetaEvent for videoId={} because manifest generator is not configured",
                        videoId);
                return;
            }
            // Build variant + master manifests after source segmentation is done.
            LOGGER.info("UploadMetaEvent: videoId={} totalSegments={} (tasks already in flight)",
                        videoId, meta.getTotalSegments());
            TOTAL_SEGMENTS_BY_VIDEO.put(videoId, meta.getTotalSegments());
            scheduleManifestGeneration(videoId, meta.getTotalSegments(), manifestService, manifestExecutor);
            return;
        }

        // Queue transcoding tasks immediately — each .ts chunk is self-contained.
        // The CHUNKS_BY_VIDEO set guards against duplicate chunk keys (e.g. retried publishes).
        String chunkKey = event.getTaskId();
        Set<String> seen = CHUNKS_BY_VIDEO.computeIfAbsent(videoId, k -> ConcurrentHashMap.newKeySet());
        if (!seen.add(chunkKey)) {
            LOGGER.debug("Duplicate chunk key={}, skipping", chunkKey);
            return;
        }
        LOGGER.info("Chunk received: videoId={} key={} — queuing {} tasks",
                    videoId, chunkKey, PROFILES.length);
        int segmentNumber = parseSegmentNumber(chunkKey);
        for (TranscodingProfile profile : PROFILES) {
            taskQueue.offer(new TranscodingTask(UUID.randomUUID().toString(), videoId, chunkKey, profile));
            publishTranscodeState(videoId, profile.getName(), segmentNumber, TranscodeSegmentState.QUEUED);
        }
    }

    // ── Startup helpers ────────────────────────────────────────────────────────

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) { return envVal; }
        String dotenvVal = dotenv.get(key);
        return (dotenvVal == null || dotenvVal.isBlank()) ? defaultValue : dotenvVal;
    }

    /** Clears accumulated per-video state. Package-private for use in tests only. */
    static void resetState() {
        CHUNKS_BY_VIDEO.clear();
        MANIFESTS_IN_FLIGHT.clear();
        TOTAL_SEGMENTS_BY_VIDEO.clear();
    }

    static void publishTranscodeState(String videoId, String profile, int segmentNumber, TranscodeSegmentState state) {
        if (transcodeStatusRepository == null || statusBus == null || segmentNumber < 0) {
            return;
        }
        try {
            transcodeStatusRepository.upsertState(videoId, profile, segmentNumber, state);
            int done = transcodeStatusRepository.countByState(videoId, profile, TranscodeSegmentState.DONE);
            int total = TOTAL_SEGMENTS_BY_VIDEO.getOrDefault(videoId, 0);
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

    private static TranscodedSegmentStatusRepository createTranscodeStatusRepository() {
        try {
            return TranscodedSegmentStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; transcoding progress disabled: {}", e.getMessage());
            return null;
        }
    }
}
