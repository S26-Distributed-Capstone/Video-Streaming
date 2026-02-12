package com.distributed26.videostreaming.upload;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
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
        StorageConfig storageConfig = loadStorageConfig();
        ObjectStorageClient storageClient = new S3StorageClient(storageConfig);
        UploadHandler uploadHandler = new UploadHandler(storageClient);

		Javalin app = Javalin.create(config -> {
            config.jetty.multipartConfig.maxFileSize(10, SizeUnit.GB);
        });

		app.get("/health", HealthHandler::health);
        app.post("/upload", uploadHandler::upload);
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
            getEnvOrProp("MINIO_BUCKET_NAME", props, "minio.bucket-name", "videos"),
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
		app.start(port);
		logger.info("Upload service started; health endpoint available at http://localhost:{}/health", app.port());
		return app;
	}

}
