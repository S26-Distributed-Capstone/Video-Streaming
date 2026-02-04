package com.distributed26.videostreaming.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HealthEndpointTest {

	private static final Logger logger = LogManager.getLogger(HealthEndpointTest.class);

	@Test
	public void healthEndpointReturnsHealthy() {
		Javalin app = Javalin.create();
		app.get("/health", HealthHandler::health);

		JavalinTest.test(app, (server, client) -> {
			try (var response = client.get("/health")) {
				assertEquals(200, response.code());
				String body = response.body().string();
				assertTrue(body.contains("\"status\":\"UP\""));
				assertTrue(body.contains("\"service\":\"upload-service\""));
				assertTrue(body.contains("\"timestamp\""));

				logger.info("Received this from the endpoint: " + body);
			}
		});
	}

	@Test
	public void startAndStopServerForLogInspection() throws Exception {
		Javalin app = UploadServiceApplication.startApp(0);
		try {
			Thread.sleep(2000);
		} finally {
			app.stop();
		}
	}
}
