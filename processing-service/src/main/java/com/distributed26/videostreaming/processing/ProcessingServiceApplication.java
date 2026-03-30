package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository;
import com.distributed26.videostreaming.processing.db.ProcessingTaskClaimRepository;
import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.processing.db.VideoProcessingRepository;
import com.distributed26.videostreaming.processing.runtime.LocalSpoolUploadWorkerPool;
import com.distributed26.videostreaming.processing.runtime.ProcessingRuntime;
import com.distributed26.videostreaming.processing.runtime.StartupRecoveryService;
import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.jobs.WorkerStatus;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private static final String DEV_LOG_SERVICE = "Processing-service";
    private static final String MANIFEST_PROCESSOR_EXECUTOR_NAME = "AbrManifestGenerator";
    private static final long DEFAULT_CLAIM_STALE_MILLIS = 10_000L;
    private static final long DEFAULT_RECOVERY_RECONCILIATION_MILLIS = 10_000L;
    private static volatile ExecutorService uploadExecutorRef;
    private static volatile ProcessingRuntime runtimeRef;
    private static volatile Thread bucketEnsureThread;

    static final TranscodingProfile[] PROFILES = {
        TranscodingProfile.LOW,
        TranscodingProfile.MEDIUM,
        TranscodingProfile.HIGH
    };

    public static void main(String[] args) throws Exception {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        RabbitMQDevLogPublisher devLogPublisher = createDevLogPublisher();
        boolean devLogPublisherOwnedByShutdownHook = false;
        try {
            // Storage
            StorageConfig storageConfig = new StorageConfig(
                    getEnvOrDotenv(dotenv, "MINIO_ENDPOINT",    "http://localhost:9000"),
                    getEnvOrDotenv(dotenv, "MINIO_ACCESS_KEY",  "minioadmin"),
                    getEnvOrDotenv(dotenv, "MINIO_SECRET_KEY",  "minioadmin"),
                    getEnvOrDotenv(dotenv, "MINIO_BUCKET_NAME", "uploads"),
                    getEnvOrDotenv(dotenv, "MINIO_REGION",      "us-east-1")
            );
            ObjectStorageClient storageClient = new S3StorageClient(storageConfig);

            StatusEventBus statusEventBus = RabbitMQStatusEventBus.fromEnv();
            TranscodeTaskBus transcodeTaskBus = RabbitMQTranscodeTaskBus.fromEnv();
            AbrManifestService manifestService = new AbrManifestService(
                    storageClient,
                    Integer.parseInt(getEnvOrDotenv(dotenv, "ABR_MANIFEST_WAIT_SECONDS", "120"))
            );
            ExecutorService manifestExecutor = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, MANIFEST_PROCESSOR_EXECUTOR_NAME)
            );
            LOGGER.info("RabbitMQ status/task buses connected");
            publishDevLogInfo(devLogPublisher, "Processing service connected to RabbitMQ");

            // Executor threads run one transcode task at a time. The executor's internal queue
            // is now the local buffer between RabbitMQ intake and FFmpeg execution.
            // Default: 3/4 of available processors (leaves headroom for uploaders, GC, OS).
            int availableCpus = Runtime.getRuntime().availableProcessors();
            int dynamicDefault = Math.max(1, (availableCpus * 3) / 4);
            int poolSize = Integer.parseInt(getEnvOrDotenv(dotenv, "WORKER_POOL_SIZE", String.valueOf(dynamicDefault)));
            LOGGER.info("Detected {} available CPU(s); transcoding worker pool size = {} (default would be {})",
                    availableCpus, poolSize, dynamicDefault);
            TranscodedSegmentStatusRepository transcodeStatusRepository = createTranscodeStatusRepository();
            VideoProcessingRepository videoProcessingRepository = createVideoProcessingRepository();
            ProcessingUploadTaskRepository processingUploadTaskRepository = createProcessingUploadTaskRepository();
            ProcessingTaskClaimRepository processingTaskClaimRepository = createProcessingTaskClaimRepository();
            Path localUploadSpoolRoot = initializeSpoolRoot(getEnvOrDotenv(dotenv, "PROCESSING_SPOOL_ROOT", "processing-spool"));
            String processorInstanceId = getEnvOrDotenv(dotenv, "HOSTNAME", "processing-service");
            long claimStaleMillis = Long.parseLong(getEnvOrDotenv(
                    dotenv,
                    "PROCESSING_TASK_CLAIM_STALE_MILLIS",
                    String.valueOf(DEFAULT_CLAIM_STALE_MILLIS)
            ));
            long recoveryReconciliationMillis = Long.parseLong(getEnvOrDotenv(
                    dotenv,
                    "PROCESSING_RECOVERY_RECONCILIATION_MILLIS",
                    String.valueOf(DEFAULT_RECOVERY_RECONCILIATION_MILLIS)
            ));
            ProcessingRuntime runtime = new ProcessingRuntime(
                    transcodeStatusRepository,
                    videoProcessingRepository,
                    processingUploadTaskRepository,
                    processingTaskClaimRepository,
                    statusEventBus,
                    transcodeTaskBus,
                    manifestService,
                    manifestExecutor,
                    localUploadSpoolRoot,
                    processorInstanceId,
                    claimStaleMillis
            );
            runtimeRef = runtime;
            if (videoProcessingRepository != null) {
                try {
                    videoProcessingRepository.findVideoIdsByStatus("FAILED").forEach(runtime::markVideoFailed);
                } catch (Exception e) {
                    LOGGER.warn("Failed to preload failed videos into processing runtime", e);
                }
            }
            List<Worker> workers = createWorkers(poolSize);
            Map<Thread, Worker> workersByThread = new ConcurrentHashMap<>();
            ThreadPoolExecutor taskExecutor = createTaskExecutor(poolSize, workers, workersByThread);
            taskExecutor.prestartAllCoreThreads();
            LOGGER.info("Started {} transcoding worker(s)", poolSize);

            if (processingUploadTaskRepository == null) {
                throw new IllegalStateException("Processing upload queue requires Postgres configuration");
            }
            if (localUploadSpoolRoot == null) {
                throw new IllegalStateException("Processing upload spool could not be initialized");
            }
            int resetUploads = processingUploadTaskRepository.resetUploadingTasks();
            if (resetUploads > 0) {
                LOGGER.info("Reset {} local upload task(s) from UPLOADING to PENDING during startup", resetUploads);
            }

            // Register listeners BEFORE recovery so that any tasks published to RabbitMQ
            // (by recovery or already sitting in the queue) are consumed by the listener
            // instead of being ack'd into the void by the empty-listener fast path.
            transcodeTaskBus.subscribe(ev -> runtime.submitTranscodeTask(ev, taskExecutor, storageClient, workersByThread, PROFILES));
            statusEventBus.subscribeAll(runtime::onStatusEvent);

            StartupRecoveryService startupRecovery = new StartupRecoveryService(PROFILES, runtime);
            startBucketEnsureThread(
                    storageClient,
                    startupRecovery,
                    localUploadSpoolRoot,
                    devLogPublisher,
                    recoveryReconciliationMillis
            );

            int pendingUploads = processingUploadTaskRepository.countByState("PENDING");
            if (pendingUploads > 0) {
                LOGGER.info("Upload queue has {} PENDING task(s) ready for upload workers", pendingUploads);
            }

            int uploadWorkerCount = Integer.parseInt(getEnvOrDotenv(dotenv, "LOCAL_UPLOAD_WORKER_COUNT", "2"));
            long uploadPollMillis = Long.parseLong(getEnvOrDotenv(dotenv, "LOCAL_UPLOAD_POLL_MILLIS", "500"));
            long uploadClaimTimeoutMillis = Long.parseLong(getEnvOrDotenv(dotenv, "LOCAL_UPLOAD_CLAIM_TIMEOUT_MILLIS", "60000"));
            ExecutorService uploadExecutor = new LocalSpoolUploadWorkerPool(PROFILES, runtime, devLogPublisher).startUploadWorkers(
                    uploadWorkerCount,
                    uploadPollMillis,
                    uploadClaimTimeoutMillis,
                    storageClient
            );
            uploadExecutorRef = uploadExecutor;


            LOGGER.info("Processing service ready — waiting for transcode tasks...");
            publishDevLogInfo(devLogPublisher, "Processing service ready for transcode tasks");

            int port = Integer.parseInt(getEnvOrDotenv(dotenv, "PROCESSING_PORT", "8082"));
            startApp(port, workers, taskExecutor);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown: stopping workers...");
                taskExecutor.shutdownNow();
                workers.forEach(worker -> worker.setStatus(WorkerStatus.OFFLINE));
                manifestExecutor.shutdownNow();
                if (uploadExecutorRef != null) {
                    uploadExecutorRef.shutdownNow();
                    uploadExecutorRef = null;
                }
                if (bucketEnsureThread != null) {
                    bucketEnsureThread.interrupt();
                    bucketEnsureThread = null;
                }
                resetState();
                try { transcodeTaskBus.close(); } catch (Exception e) { LOGGER.warn("Error closing transcode task bus", e); }
                try { statusEventBus.close(); } catch (Exception e) { LOGGER.warn("Error closing status event bus", e); }
                try { storageClient.close(); } catch (Exception e) { LOGGER.warn("Error closing storage client", e); }
                closeDevLogPublisher(devLogPublisher);
            }));
            devLogPublisherOwnedByShutdownHook = true;

            Thread.currentThread().join();
        } finally {
            if (!devLogPublisherOwnedByShutdownHook) {
                closeDevLogPublisher(devLogPublisher);
            }
        }
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

    private static void startBucketEnsureThread(
            ObjectStorageClient storageClient,
            StartupRecoveryService startupRecovery,
            Path localUploadSpoolRoot,
            RabbitMQDevLogPublisher devLogPublisher,
            long recoveryReconciliationMillis
    ) {
        Thread priorThread = bucketEnsureThread;
        if (priorThread != null && priorThread.isAlive()) {
            priorThread.interrupt();
        }

        Thread thread = new Thread(() -> runBucketEnsureLoop(
                storageClient,
                startupRecovery,
                localUploadSpoolRoot,
                devLogPublisher,
                recoveryReconciliationMillis
        ), "processing-bucket-ensure");
        thread.setDaemon(true);
        bucketEnsureThread = thread;
        thread.start();
    }

    private static void runBucketEnsureLoop(
            ObjectStorageClient storageClient,
            StartupRecoveryService startupRecovery,
            Path localUploadSpoolRoot,
            RabbitMQDevLogPublisher devLogPublisher,
            long recoveryReconciliationMillis
    ) {
        long delayMillis = 500L;
        long normalizedRecoveryReconciliationMillis = Math.max(1_000L, recoveryReconciliationMillis);
        boolean waitingForStorage = false;
        boolean storageReady = false;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                storageClient.ensureBucketExists();
                if (!storageReady) {
                    LOGGER.info("Storage ready — bucket ensure completed");
                }
                if (waitingForStorage) {
                    publishDevLogInfo(devLogPublisher, "MinIO recovered; processing service resumed startup recovery");
                } else if (!storageReady) {
                    publishDevLogInfo(devLogPublisher, "Processing service storage ready");
                }
                waitingForStorage = false;
                storageReady = true;
                delayMillis = 500L;

                startupRecovery.recoverOrphanedSpoolFiles(storageClient, localUploadSpoolRoot);
                startupRecovery.recoverIncompleteVideos(storageClient);
                Thread.sleep(normalizedRecoveryReconciliationMillis);
            } catch (RuntimeException e) {
                storageReady = false;
                if (!waitingForStorage) {
                    waitingForStorage = true;
                    LOGGER.warn("MinIO unavailable during processing startup; continuing and retrying bucket ensure", e);
                    publishDevLogWarn(devLogPublisher, "MinIO is down, continuing to process and polling for restart");
                } else {
                    LOGGER.warn("Retrying processing startup bucket ensure in {} ms", delayMillis, e);
                }
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    bucketEnsureThread = null;
                    return;
                }
                delayMillis = Math.min(30_000L, delayMillis * 2);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                bucketEnsureThread = null;
                return;
            } catch (Exception e) {
                LOGGER.warn("Unexpected failure in processing startup recovery/reconciliation loop", e);
                try {
                    Thread.sleep(normalizedRecoveryReconciliationMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    bucketEnsureThread = null;
                    return;
                }
            }
        }
        bucketEnsureThread = null;
    }

    private static RabbitMQDevLogPublisher createDevLogPublisher() {
        try {
            return RabbitMQDevLogPublisher.fromEnv();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to initialize dev log publisher", e);
            return null;
        }
    }

    private static void publishDevLogInfo(RabbitMQDevLogPublisher publisher, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishInfo(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing dev log info message={}", message, e);
        }
    }

    private static void publishDevLogError(RabbitMQDevLogPublisher publisher, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishError(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing dev log error message={}", message, e);
        }
    }

    private static void publishDevLogWarn(RabbitMQDevLogPublisher publisher, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishWarn(DEV_LOG_SERVICE, message);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to publish processing dev log warning message={}", message, e);
        }
    }

    private static void closeDevLogPublisher(RabbitMQDevLogPublisher publisher) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing dev log publisher", e);
        }
    }

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) { return envVal; }
        String dotenvVal = dotenv.get(key);
        return (dotenvVal == null || dotenvVal.isBlank()) ? defaultValue : dotenvVal;
    }

    /** Clears accumulated per-video state. Package-private for use in tests only. */
    static void resetState() {
        ProcessingRuntime runtime = runtimeRef;
        runtimeRef = null;
        if (runtime != null) {
            runtime.resetForTests();
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

    private static ProcessingUploadTaskRepository createProcessingUploadTaskRepository() {
        try {
            return ProcessingUploadTaskRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; cannot start local upload queue: {}", e.getMessage());
            return null;
        }
    }

    private static ProcessingTaskClaimRepository createProcessingTaskClaimRepository() {
        try {
            return ProcessingTaskClaimRepository.fromEnv();
        } catch (IllegalStateException e) {
            LOGGER.warn("Postgres not configured; cannot track processing task claims: {}", e.getMessage());
            return null;
        }
    }

    private static Path initializeSpoolRoot(String rawPath) {
        try {
            Path path = Path.of(rawPath).toAbsolutePath().normalize();
            Files.createDirectories(path);
            LOGGER.info("Local processing upload spool ready: {}", path);
            return path;
        } catch (Exception e) {
            LOGGER.warn("Failed to initialize local processing upload spool '{}'",
                    rawPath, e);
            return null;
        }
    }

    /**
     * Spawns a daemon thread that retries {@code ensureBucketExists()} with
     * exponential backoff until it succeeds. Called only when the synchronous
     * startup check fails (MinIO unreachable). Without this, a truly missing
     * bucket (fresh deployment) would cause every subsequent S3 operation to hit
     * {@code NoSuchBucketException} (404) — which {@code ResilientStorageClient}
     * treats as non-transient and fails immediately.
     *
     * @return the background thread, so the shutdown hook can interrupt it
     */
    private static Thread startBucketEnsureBackground(S3StorageClient rawClient, String bucketName,
                                                      long initialDelayMs, long maxDelayMs) {
        Thread t = new Thread(() -> {
            long delay = initialDelayMs;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Background bucket-ensure thread interrupted — shutting down");
                    return;
                }
                try {
                    rawClient.ensureBucketExists();
                    LOGGER.info("Background bucket check succeeded — bucket '{}' is ready", bucketName);
                    bucketEnsureThread = null;
                    return;
                } catch (Exception e) {
                    LOGGER.warn("Background bucket check for '{}' still failing — next retry in {} ms: {}",
                            bucketName, Math.min(maxDelayMs, delay * 2), e.toString());
                    delay = Math.min(maxDelayMs, delay * 2);
                }
            }
        }, "bucket-ensure-bg");
        t.setDaemon(true);
        t.start();
        return t;
    }

    static void onStatusEvent(JobEvent event, AbrManifestService manifestService, ExecutorService manifestExecutor) {
        ProcessingRuntime runtime = runtime();
        if (manifestService != null) {
            runtime.setManifestServiceRef(manifestService);
        }
        if (manifestExecutor != null) {
            runtime.setManifestExecutorRef(manifestExecutor);
        }
        runtime.onStatusEvent(event);
    }

    static TranscodingTask onTranscodeTaskEvent(TranscodeTaskEvent taskEvent) {
        if (runtimeRef == null) {
            runtimeRef = new ProcessingRuntime(null, null, null, null, null, null, null, null, null, null);
        }
        return runtimeRef.onTranscodeTaskEvent(taskEvent, PROFILES);
    }

    static ProcessingRuntime runtime() {
        if (runtimeRef == null) {
            runtimeRef = new ProcessingRuntime(null, null, null, null, null, null, null, null, null, null);
        }
        return runtimeRef;
    }
}
