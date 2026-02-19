package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.InMemoryJobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
import io.javalin.http.staticfiles.Location;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 *
 */
public class UploadServiceApplication {

	private static final int DEFAULT_PORT = 8080;
	private static final Logger logger = LogManager.getLogger(UploadServiceApplication.class);
	public static void main(String[] args) {
		startApp(DEFAULT_PORT);

	}

	static Javalin createApp() {
        JobTaskBus jobTaskBus = new InMemoryJobTaskBus();
        return createApp(jobTaskBus);
    }

    static Javalin createApp(JobTaskBus jobTaskBus) {
        ensureLogsDirectory();
        StorageConfig storageConfig = loadStorageConfig();
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);

        // Ensure the bucket exists before starting the application
        storageClient.ensureBucketExists();

        VideoUploadRepository videoUploadRepository = createVideoUploadRepository();
        String machineId = resolveMachineId();

        UploadHandler uploadHandler = new UploadHandler(
                storageClient,
                jobTaskBus,
                videoUploadRepository,
                machineId
        );
        UploadStatusWebSocket uploadStatusWebSocket = new UploadStatusWebSocket(jobTaskBus);
        UploadInfoHandler uploadInfoHandler = new UploadInfoHandler(videoUploadRepository);

        Javalin app = Javalin.create(config -> {
            config.jetty.multipartConfig.maxFileSize(10, SizeUnit.GB);
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = "frontend";
                staticFiles.location = Location.EXTERNAL;
            });
        });

        app.post("/upload", uploadHandler::upload);
        app.ws("/upload-status", uploadStatusWebSocket::configure);
        app.get("/upload-info/{videoId}", uploadInfoHandler::getInfo);
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

    private static void ensureLogsDirectory() {
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("logs"));
        } catch (java.io.IOException e) {
            logger.warn("Failed to create logs directory", e);
        }
    }

    private static final io.github.cdimascio.dotenv.Dotenv DOTENV = io.github.cdimascio.dotenv.Dotenv.configure().directory("./").ignoreIfMissing().load();

    private static String resolveMachineId() {
        String machineId = System.getenv("MACHINE_ID");
        if (machineId != null && !machineId.isBlank()) {
            return machineId;
        }
        
        machineId = DOTENV.get("MACHINE_ID");

        return machineId;
    }

	static Javalin startApp(int port) {
		Javalin app = createApp();
        logger.info("Creating Javalin app");
		app.start(port);
		return app;
	}

}
