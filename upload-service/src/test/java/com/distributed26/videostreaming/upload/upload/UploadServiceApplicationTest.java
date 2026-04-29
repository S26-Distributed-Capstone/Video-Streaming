package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UploadServiceApplicationTest {

    @Test
    @DisplayName("Should serve the frontend shell for the root route")
    void shouldServeFrontendShellForRootRoute() {
        Javalin app = Javalin.create();
        UploadServiceApplication.registerFrontendRoutes(app, "<!doctype html><div id=\"uploadTab\"></div>");

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.get("/")) {
                assertEquals(200, response.code());
                String body = response.body().string();
                assertTrue(body.contains("<!doctype html>"));
                assertTrue(body.contains("id=\"uploadTab\""));
            }
        });
    }

    @Test
    @DisplayName("Should serve the frontend shell for processing routes")
    void shouldServeFrontendShellForProcessingRoutes() {
        Javalin app = Javalin.create();
        UploadServiceApplication.registerFrontendRoutes(app, "<!doctype html><div id=\"processingRouteBanner\"></div>");

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.get("/processing/test-video-123")) {
                assertEquals(200, response.code());
                String body = response.body().string();
                assertTrue(body.contains("<!doctype html>"));
                assertTrue(body.contains("id=\"processingRouteBanner\""));
            }
        });
    }

    @Test
    @DisplayName("Should return a controlled error when the frontend shell is unavailable")
    void shouldReturnControlledErrorWhenFrontendShellUnavailable() {
        Javalin app = Javalin.create();
        UploadServiceApplication.registerFrontendRoutes(app, null);

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.get("/processing/test-video-123")) {
                assertEquals(500, response.code());
                assertEquals("Frontend shell unavailable", response.body().string());
            }
        });
    }
}
