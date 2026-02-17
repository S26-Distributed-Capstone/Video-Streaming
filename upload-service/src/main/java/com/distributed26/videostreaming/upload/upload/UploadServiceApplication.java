package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.shared.upload.InMemoryJobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
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
        StorageConfig storageConfig = loadStorageConfig();
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);

        // Ensure the bucket exists before starting the application
        storageClient.ensureBucketExists();
        
        UploadHandler uploadHandler = new UploadHandler(storageClient);
        UploadStatusWebSocket uploadStatusWebSocket = new UploadStatusWebSocket(jobTaskBus);

		Javalin app = Javalin.create(config -> {
            config.jetty.multipartConfig.maxFileSize(10, SizeUnit.GB);
        });

        app.post("/upload", uploadHandler::upload);
        app.ws("/upload-status", uploadStatusWebSocket::configure);
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

	static Javalin startApp(int port) {
		Javalin app = createApp();
        logger.info("Creating Javalin app");
		app.start(port);
		return app;
	}

}
