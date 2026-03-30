package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ResilientStorageClient;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogReader;
import com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher;
import com.distributed26.videostreaming.shared.upload.RabbitMQStatusEventBus;
import com.distributed26.videostreaming.shared.upload.RabbitMQTranscodeTaskBus;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.upload.processing.StorageRetryExecutor;
import com.distributed26.videostreaming.upload.processing.StorageStateTracker;
import com.distributed26.videostreaming.upload.processing.UploadProcessingConfig;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
import io.javalin.http.staticfiles.Location;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadServiceApplication {

	private static final int DEFAULT_UPLOAD_PORT = 8080;
    private static final int DEFAULT_STATUS_PORT = 8081;
    private static final String MODE_UPLOAD = "upload";
    private static final String MODE_STATUS = "status";
    private static final String DEV_LOG_UPLOAD_SERVICE = "Upload-service";
    private static final String DEV_LOG_STATUS_SERVICE = "Status-service";
    private static final String FRONTEND_DIRECTORY = "frontend";
    private static final String FRONTEND_INDEX_FILE = "index.html";
    private static final String DEV_LOGS_HTML_FILE = "dev-logs.html";
	private static final Logger logger = LogManager.getLogger(UploadServiceApplication.class);
	public static void main(String[] args) {
        String mode = System.getenv("SERVICE_MODE");
        if (mode == null || mode.isBlank()) {
            mode = MODE_UPLOAD;
        }
        if (MODE_STATUS.equalsIgnoreCase(mode)) {
            startStatusApp(DEFAULT_STATUS_PORT);
            return;
        }
        if (MODE_UPLOAD.equalsIgnoreCase(mode)) {
            startUploadApp(DEFAULT_UPLOAD_PORT);
            return;
        }
        throw new IllegalArgumentException("Unknown SERVICE_MODE: " + mode);

	}

    static Javalin createUploadApp(StatusEventBus statusEventBus, TranscodeTaskBus transcodeTaskBus) {
        ensureLogsDirectory();
        StorageConfig storageConfig = loadStorageConfig();
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        UploadProcessingConfig processingConfig =
                UploadProcessingConfig.fromDotenv(io.github.cdimascio.dotenv.Dotenv.configure().directory("./").ignoreIfMissing().load());

        VideoUploadRepository videoUploadRepository = createVideoUploadRepository();
        SegmentUploadRepository segmentUploadRepository = createSegmentUploadRepository();
        String machineId = resolveMachineId();
        String containerId = resolveContainerId();
        FailedVideoRegistry failedVideoRegistry = new FailedVideoRegistry();
        RabbitMQDevLogPublisher devLogPublisher = createDevLogPublisher();
        StorageStateTracker storageStateTracker = new StorageStateTracker(videoUploadRepository, statusEventBus, devLogPublisher);
        statusEventBus.subscribeAll(event -> {
            if (event instanceof UploadFailedEvent failed) {
                failedVideoRegistry.markFailed(failed.getJobId());
            }
        });

        UploadHandler uploadHandler = new UploadHandler(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                videoUploadRepository,
                segmentUploadRepository,
                machineId,
                containerId,
                failedVideoRegistry,
                storageStateTracker,
                processingConfig
        );
        TerminalFailureHandler terminalFailureHandler = new TerminalFailureHandler(
                videoUploadRepository,
                statusEventBus,
                machineId,
                containerId
        );

        StorageRetryExecutor startupRetryExecutor = new StorageRetryExecutor(
                processingConfig.storageRetryInitialDelayMillis(),
                processingConfig.storageRetryMaxDelayMillis()
        );
        ExecutorService storageStartupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "upload-storage-startup");
            thread.setDaemon(true);
            return thread;
        });
        storageStartupExecutor.submit(() -> startupRetryExecutor.run(
                "ensure upload bucket exists",
                new StorageRetryExecutor.RetryObserver() {
                    private boolean waiting;

                    @Override
                    public void onRetrying(int attempt, RuntimeException failure, long nextDelayMillis) {
                        if (!waiting) {
                            waiting = true;
                            storageStateTracker.beginStorageWait(null, failure.getMessage());
                        }
                    }

                    @Override
                    public void onSucceeded(int attempts) {
                        if (waiting) {
                            waiting = false;
                            storageStateTracker.endStorageWait(null);
                        }
                    }

                    @Override
                    public void onCancelled(Exception failure) {
                        if (waiting) {
                            waiting = false;
                            storageStateTracker.endStorageWait(null);
                        }
                    }
                },
                storageClient::ensureBucketExists
        ));

        Javalin app = Javalin.create(config -> {
            config.jetty.multipartConfig.maxFileSize(10, SizeUnit.GB);
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = FRONTEND_DIRECTORY;
                staticFiles.location = Location.EXTERNAL;
            });
        });
        String frontendIndex = loadFrontendIndex();

        app.get("/health", ctx -> ctx.json(java.util.Map.of(
                "status", "ok",
                "storageReady", storageStateTracker.isServiceReady()
        )));
        app.get("/ready", ctx -> {
            if (!storageStateTracker.isServiceReady()) {
                ctx.status(503).json(java.util.Map.of("status", "waiting_for_storage", "storageReady", false));
                return;
            }
            ctx.json(java.util.Map.of("status", "ready", "storageReady", true));
        });
        registerFrontendRoutes(app, frontendIndex);
        app.post("/upload", uploadHandler::upload);
        app.post("/upload/{videoId}/fail", terminalFailureHandler::markFailed);

        app.events(event -> event.serverStopped(() -> {
            try {
                uploadHandler.close();
            } catch (Exception e) {
                logger.warn("Error closing upload handler on shutdown", e);
            }
            try {
                storageClient.close();
            } catch (Exception e) {
                logger.warn("Error closing storage client on shutdown", e);
            }
            closeDevLogPublisher(devLogPublisher);
            storageStartupExecutor.shutdownNow();
        }));

		return app;
    }

    static Javalin createStatusApp(StatusEventBus statusEventBus) {
        ensureLogsDirectory();
        ObjectStorageClient storageClient = new ResilientStorageClient(new S3StorageClient(loadStorageConfig()));
        VideoUploadRepository videoUploadRepository = createVideoUploadRepository();
        SegmentUploadRepository segmentUploadRepository = createSegmentUploadRepository();
        TranscodedSegmentStatusRepository transcodedSegmentStatusRepository = createTranscodedSegmentStatusRepository();
        TerminalFailureStorageCleanup terminalFailureStorageCleanup =
                new TerminalFailureStorageCleanup(storageClient, videoUploadRepository);
        statusEventBus.subscribeAll(terminalFailureStorageCleanup);
        UploadStatusWebSocket uploadStatusWebSocket =
            new UploadStatusWebSocket(statusEventBus, segmentUploadRepository, transcodedSegmentStatusRepository);
        UploadInfoHandler uploadInfoHandler = new UploadInfoHandler(
                videoUploadRepository,
                segmentUploadRepository,
                transcodedSegmentStatusRepository
        );
        RabbitMQDevLogReader devLogReader = createDevLogReader();
        String devLogsHtml = loadDevLogsPage();

        Javalin app = Javalin.create();
        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type,Authorization");
        });
        app.options("/*", ctx -> ctx.status(204));
        app.get("/health", ctx -> ctx.json(java.util.Map.of("status", "ok")));
        app.ws("/upload-status", uploadStatusWebSocket::configure);
        app.get("/upload-info/{videoId}", uploadInfoHandler::getInfo);
        app.get("/dev-logs", ctx -> {
            String format = ctx.queryParam("format");
            boolean wantsJson = "json".equalsIgnoreCase(format);

            if (wantsJson) {
                if (devLogReader == null) {
                    ctx.status(503).json(java.util.Map.of(
                            "status", "unavailable",
                            "message", "Dev log reader is not configured"
                    ));
                    return;
                }
                int limit = parseDevLogLimit(ctx.queryParam("limit"));
                java.util.List<RabbitMQDevLogPublisher.DevLogMessage> all = devLogReader.peekAll();
                java.util.List<RabbitMQDevLogPublisher.DevLogMessage> tail = all.size() > limit
                        ? all.subList(all.size() - limit, all.size())
                        : all;
                ctx.json(java.util.Map.of(
                        "queue", devLogReader.queueName(),
                        "binding", devLogReader.bindingKey(),
                        "limit", limit,
                        "total", all.size(),
                        "logs", tail
                ));
                return;
            }

            if (devLogsHtml == null || devLogsHtml.isBlank()) {
                ctx.status(500).result("Dev logs page unavailable");
                return;
            }
            ctx.html(devLogsHtml);
        });
        app.events(event -> event.serverStopped(() -> {
            try {
                terminalFailureStorageCleanup.close();
            } catch (Exception e) {
                logger.warn("Error closing terminal failure storage cleanup", e);
            }
            try {
                storageClient.close();
            } catch (Exception e) {
                logger.warn("Error closing status storage client", e);
            }
            closeDevLogReader(devLogReader);
        }));
        return app;
    }

    private static StorageConfig loadStorageConfig() {
        Properties props = new Properties();
        try (InputStream input = UploadServiceApplication.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            logger.warn("Could not load application.properties, using defaults", e);
        }

        return new StorageConfig(
            getEnvOrProp("MINIO_ENDPOINT", props, "minio.endpoint", "http://localhost:9000"),
            getEnvOrProp("MINIO_ACCESS_KEY", props, "minio.access-key", "minioadmin"),
            getEnvOrProp("MINIO_SECRET_KEY", props, "minio.secret-key", "minioadmin"),
            getEnvOrProp("MINIO_BUCKET_NAME", props, "minio.bucket-name", "uploads"),
            getEnvOrProp("MINIO_REGION", props, "minio.region", "us-east-1")
        );
    }

    private static String getEnvOrProp(String envKey, Properties props, String propKey, String defaultValue) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) {
            return envVal;
        }
        return props.getProperty(propKey, defaultValue);
    }

    private static VideoUploadRepository createVideoUploadRepository() {
        try {
            return VideoUploadRepository.fromEnv();
        } catch (IllegalStateException e) {
            logger.warn("Postgres not configured; upload info disabled: {}", e.getMessage());
            return null;
        }
    }

    private static SegmentUploadRepository createSegmentUploadRepository() {
        try {
            return SegmentUploadRepository.fromEnv();
        } catch (IllegalStateException e) {
            logger.warn("Postgres not configured; segment uploads disabled: {}", e.getMessage());
            return null;
        }
    }

    private static TranscodedSegmentStatusRepository createTranscodedSegmentStatusRepository() {
        try {
            return TranscodedSegmentStatusRepository.fromEnv();
        } catch (IllegalStateException e) {
            logger.warn("Postgres not configured; transcoded segment status disabled: {}", e.getMessage());
            return null;
        }
    }

    private static void ensureLogsDirectory() {
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("logs"));
        } catch (java.io.IOException e) {
            logger.warn("Failed to create logs directory", e);
        }
    }

    static void registerFrontendRoutes(Javalin app, String frontendIndex) {
        app.get("/processing/{videoId}", ctx -> {
            if (frontendIndex == null || frontendIndex.isBlank()) {
                ctx.status(500).result("Frontend shell unavailable");
                return;
            }
            ctx.html(frontendIndex);
        });
    }

    static String loadFrontendIndex() {
        try {
            return Files.readString(Path.of(FRONTEND_DIRECTORY, FRONTEND_INDEX_FILE));
        } catch (IOException e) {
            logger.error("Failed to load frontend shell from {}/{}", FRONTEND_DIRECTORY, FRONTEND_INDEX_FILE, e);
            return null;
        }
    }

    static String loadDevLogsPage() {
        try {
            return Files.readString(Path.of(FRONTEND_DIRECTORY, DEV_LOGS_HTML_FILE));
        } catch (IOException e) {
            logger.error("Failed to load dev-logs page from {}/{}", FRONTEND_DIRECTORY, DEV_LOGS_HTML_FILE, e);
            return null;
        }
    }

    private static final io.github.cdimascio.dotenv.Dotenv DOTENV = io.github.cdimascio.dotenv.Dotenv.configure().directory("./").ignoreIfMissing().load();

    private static String resolveMachineId() {
        String machineId = DOTENV.get("MACHINE_ID");
        if (machineId == null || machineId.isBlank()) {
            machineId = System.getenv("MACHINE_ID");
        }
        if (machineId == null || machineId.isBlank()) {
            return null;
        }
        return machineId;
    }

    private static String resolveContainerId() {
        String containerId = DOTENV.get("CONTAINER_ID");
        if (containerId != null && !containerId.isBlank()) {
            return containerId;
        }
        return DOTENV.get("HOSTNAME");
    }

	static void startUploadApp(int uploadPort) {
        StatusEventBus statusEventBus = RabbitMQStatusEventBus.fromEnv();
        TranscodeTaskBus transcodeTaskBus = RabbitMQTranscodeTaskBus.fromEnv();
		Javalin uploadApp = createUploadApp(statusEventBus, transcodeTaskBus);
        publishDevLogInfo(createDevLogPublisher(), DEV_LOG_UPLOAD_SERVICE, "Upload service started");
        logger.info("Starting upload app on port {}", uploadPort);
		uploadApp.start(uploadPort);
	}

    static void startStatusApp(int statusPort) {
        StatusEventBus statusEventBus = RabbitMQStatusEventBus.fromEnv();
        Javalin statusApp = createStatusApp(statusEventBus);
        publishDevLogInfo(createDevLogPublisher(), DEV_LOG_STATUS_SERVICE, "Status service started");
        logger.info("Starting status app on port {}", statusPort);
        statusApp.start(statusPort);
    }

    private static RabbitMQDevLogPublisher createDevLogPublisher() {
        try {
            return RabbitMQDevLogPublisher.fromEnv();
        } catch (RuntimeException e) {
            logger.warn("Failed to initialize dev log publisher", e);
            return null;
        }
    }

    private static RabbitMQDevLogReader createDevLogReader() {
        try {
            return RabbitMQDevLogReader.fromEnv();
        } catch (RuntimeException e) {
            logger.warn("Failed to initialize dev log reader", e);
            return null;
        }
    }

    private static void publishDevLogInfo(RabbitMQDevLogPublisher publisher, String serviceName, String message) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.publishInfo(serviceName, message);
        } catch (RuntimeException e) {
            logger.warn("Failed to publish dev log info service={} message={}", serviceName, message, e);
        } finally {
            closeDevLogPublisher(publisher);
        }
    }

    private static void closeDevLogPublisher(RabbitMQDevLogPublisher publisher) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.close();
        } catch (Exception e) {
            logger.warn("Error closing dev log publisher", e);
        }
    }

    private static void closeDevLogReader(RabbitMQDevLogReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            logger.warn("Error closing dev log reader", e);
        }
    }

    private static int parseDevLogLimit(String limitParam) {
        if (limitParam == null || limitParam.isBlank()) {
            return 20;
        }
        try {
            return Math.max(1, Math.min(Integer.parseInt(limitParam.trim()), 1000));
        } catch (NumberFormatException e) {
            return 20;
        }
    }

}
