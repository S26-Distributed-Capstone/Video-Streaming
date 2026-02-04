package com.distributed26.videostreaming.upload;

import io.javalin.Javalin;
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
		Javalin app = Javalin.create();
		app.get("/health", HealthHandler::health);
		return app;
	}

	static Javalin startApp(int port) {
		Javalin app = createApp();
		app.start(port);
		logger.info("Upload service started; health endpoint available at http://localhost:{}/health", app.port());
		return app;
	}

}
