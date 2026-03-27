package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UploadServiceApplicationTest {

    @Test
    @DisplayName("Should serve the frontend shell for processing routes")
    void shouldServeFrontendShellForProcessingRoutes() {
        Javalin app = Javalin.create();
        UploadServiceApplication.registerFrontendRoutes(app);

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.get("/processing/test-video-123")) {
                assertEquals(200, response.code());
                String body = response.body().string();
                assertTrue(body.contains("<!doctype html>"));
                assertTrue(body.contains("id=\"processingRouteBanner\""));
            }
        });
    }
}
