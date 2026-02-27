package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.RabbitMQJobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
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
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Entry point for the processing service.
 *
 * <p>Listens on the {@code upload.events} TOPIC exchange for {@link JobTaskEvent}s
 * published by the upload service.  Once all segments for a video have arrived
 * (signalled by {@link UploadMetaEvent}), a {@link TranscodingTask} is queued for
 * every (chunk × profile) combination and executed by the worker pool.
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

        // Worker pool
        int poolSize = Integer.parseInt(getEnvOrDotenv(dotenv, "WORKER_POOL_SIZE", "4"));
        BlockingQueue<TranscodingTask> taskQueue = new LinkedBlockingQueue<>();
        List<TranscodingWorker> workers = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            TranscodingWorker w = new TranscodingWorker("worker-" + i, storageClient, taskQueue);
            w.start();
            workers.add(w);
        }
        LOGGER.info("Started {} transcoding worker(s)", poolSize);

        // RabbitMQ bus — fromEnv() reads SERVICE_MODE; any value other than
        // "upload" enables the AMQP consumer.  Set SERVICE_MODE=processing and
        // use a dedicated RABBITMQ_STATUS_QUEUE so the processing service gets
        // its own copy of every upload.status.* message (TOPIC fan-out).
        RabbitMQJobTaskBus bus = RabbitMQJobTaskBus.fromEnv();
        LOGGER.info("RabbitMQJobTaskBus connected");

        // subscribeAll ensures every incoming event reaches onEvent() regardless
        // of whether we've seen that videoId before — fixing the first-event drop.
        bus.subscribeAll(ev -> onEvent(ev, taskQueue, bus));

        LOGGER.info("Processing service ready — waiting for upload events...");

        int port = Integer.parseInt(getEnvOrDotenv(dotenv, "PROCESSING_PORT", "8082"));
        startApp(port, workers, taskQueue);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown: stopping workers...");
            workers.forEach(TranscodingWorker::stop);
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
        String videoId = event.getJobId();

        if (event instanceof UploadMetaEvent meta) {
            // All chunks have already been queued as they arrived; nothing to do here.
            LOGGER.info("UploadMetaEvent: videoId={} totalSegments={} (tasks already in flight)",
                        videoId, meta.getTotalSegments());
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
        for (TranscodingProfile profile : PROFILES) {
            taskQueue.offer(new TranscodingTask(UUID.randomUUID().toString(), videoId, chunkKey, profile));
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
    }
}
