package com.distributed26.videostreaming.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

public class AppTest {
	@Test
	public void healthEndpointReturnsHealthy() {
		Javalin app = Javalin.create();
		app.get("/health", HealthHandler::health);

		JavalinTest.test(app, (server, client) -> {
			try (var response = client.get("/health")) {
				assertEquals(200, response.code());
				assertEquals("healthy!", response.body().string());
			}
		});

	}
}
