package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.UploadProgressEvent;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class StatusServiceIntegrationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String jdbcUrl;
    private String username;
    private String password;

    @AfterEach
    void tearDown() {
        // each test performs explicit cleanup with the generated videoId
    }

    @Test
    void reconnectingToNewStatusServiceInstanceRestoresSnapshotAndReceivesLiveEvents() throws Exception {
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        seedVideoRecord(videoId);
        insertSegment(videoId, 0);
        insertTranscodeState(videoId, "low", 0, "DONE");

        SegmentUploadRepository segmentRepo = new SegmentUploadRepository(jdbcUrl, username, password);
        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        TestStatusEventBus statusEventBus = new TestStatusEventBus();

        Javalin app1 = createStatusTestApp(statusEventBus, segmentRepo, transcodeRepo);
        Javalin app2 = null;
        TestWebSocketClient ws1 = null;
        TestWebSocketClient ws2 = null;

        try {
            app1.start(0);
            ws1 = connectStatusSocket(app1.port(), videoId);
            assertTrue(ws1.awaitMessage(
                    Duration.ofSeconds(5),
                    json -> isProgressEvent(json, 1) || isTranscodeSnapshot(json, "low", 1)
            ), "first status-service instance should send initial snapshot from Postgres");

            app1.stop();
            ws1.awaitClosed(Duration.ofSeconds(5));

            insertSegment(videoId, 1);
            insertTranscodeState(videoId, "medium", 0, "DONE");

            app2 = createStatusTestApp(statusEventBus, segmentRepo, transcodeRepo);
            app2.start(0);
            ws2 = connectStatusSocket(app2.port(), videoId);

            assertTrue(ws2.awaitMessage(Duration.ofSeconds(5), json -> isProgressEvent(json, 2)),
                    "new status-service instance should restore the latest uploaded segment count");
            assertTrue(ws2.awaitMessage(Duration.ofSeconds(5), json -> isTranscodeSnapshot(json, "low", 1)),
                    "new status-service instance should restore low-profile transcode progress");
            assertTrue(ws2.awaitMessage(Duration.ofSeconds(5), json -> isTranscodeSnapshot(json, "medium", 1)),
                    "new status-service instance should restore medium-profile transcode progress");

            statusEventBus.publish(new UploadProgressEvent(videoId, 3));
            assertTrue(ws2.awaitMessage(Duration.ofSeconds(5), json -> isProgressEvent(json, 3)),
                    "reconnected client should receive live progress events after the restored snapshot");

            statusEventBus.publish(new TranscodeProgressEvent(
                    videoId,
                    "high",
                    0,
                    TranscodeSegmentState.DONE,
                    1,
                    0
            ));
            assertTrue(ws2.awaitMessage(Duration.ofSeconds(5), json -> isLiveTranscodeEvent(json, "high", 1)),
                    "reconnected client should resume consuming live transcode events");
        } finally {
            if (ws1 != null) {
                ws1.close();
            }
            if (ws2 != null) {
                ws2.close();
            }
            if (app1 != null) {
                app1.stop();
            }
            if (app2 != null) {
                app2.stop();
            }
            cleanupTranscodeState(videoId);
            cleanupVideoRecords(videoId);
        }
    }

    private Javalin createStatusTestApp(
            TestStatusEventBus statusEventBus,
            SegmentUploadRepository segmentRepo,
            TranscodedSegmentStatusRepository transcodeRepo
    ) {
        UploadStatusWebSocket uploadStatusWebSocket = new UploadStatusWebSocket(statusEventBus, segmentRepo, transcodeRepo);
        Javalin app = Javalin.create();
        app.ws("/upload-status", uploadStatusWebSocket::configure);
        return app;
    }

    private TestWebSocketClient connectStatusSocket(int port, String videoId) throws InterruptedException {
        OkHttpClient client = new OkHttpClient();
        TestWebSocketClient listener = new TestWebSocketClient(client);
        Request request = new Request.Builder()
                .url("ws://localhost:" + port + "/upload-status?jobId=" + videoId)
                .build();
        client.newWebSocket(request, listener);
        assertTrue(listener.opened.await(5, TimeUnit.SECONDS), "websocket should connect");
        return listener;
    }

    private boolean isProgressEvent(JsonNode json, int completedSegments) {
        return "progress".equals(json.path("type").asText())
                && completedSegments == json.path("completedSegments").asInt();
    }

    private boolean isTranscodeSnapshot(JsonNode json, String profile, int doneSegments) {
        return "transcode_progress".equals(json.path("type").asText())
                && profile.equals(json.path("profile").asText())
                && doneSegments == json.path("doneSegments").asInt();
    }

    private boolean isLiveTranscodeEvent(JsonNode json, String profile, int doneSegments) {
        return isTranscodeSnapshot(json, profile, doneSegments)
                && "DONE".equals(json.path("state").asText())
                && 0 == json.path("segmentNumber").asInt();
    }

    private void loadDatabaseConfig() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = normalizeJdbcUrl(firstNonBlank(System.getenv("PG_URL"), dotenv.get("PG_URL")));
        username = firstNonBlank(System.getenv("PG_USER"), dotenv.get("PG_USER"));
        password = firstNonBlank(System.getenv("PG_PASSWORD"), dotenv.get("PG_PASSWORD"));
    }

    private void assumeDatabaseReachable() {
        Assumptions.assumeTrue(jdbcUrl != null && !jdbcUrl.isBlank(), "PG_URL is not set");
        Assumptions.assumeTrue(username != null && !username.isBlank(), "PG_USER is not set");
        try (Connection ignored = DriverManager.getConnection(jdbcUrl, username, password)) {
            // reachable
        } catch (SQLException e) {
            Assumptions.assumeTrue(false, "Skipping integration test because Postgres is not reachable: " + e.getMessage());
        }
    }

    private void seedVideoRecord(String videoId) {
        VideoUploadRepository repo = new VideoUploadRepository(jdbcUrl, username, password);
        repo.create(videoId, "Status Scenario Test", 10, "PROCESSING", "machine-a", "container-a");
    }

    private void insertSegment(String videoId, int segmentNumber) {
        SegmentUploadRepository repo = new SegmentUploadRepository(jdbcUrl, username, password);
        repo.insert(videoId, segmentNumber);
    }

    private void insertTranscodeState(String videoId, String profile, int segmentNumber, String state) {
        String sql = """
                INSERT INTO transcoded_segment_status (video_id, profile, segment_number, state)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (video_id, profile, segment_number) DO UPDATE
                SET state = EXCLUDED.state,
                    updated_at = NOW()
                """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setString(4, state);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert transcoded_segment_status", e);
        }
    }

    private void cleanupVideoRecords(String videoId) {
        if (videoId == null || jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM segment_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM video_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up integration test data", e);
        }
    }

    private void cleanupTranscodeState(String videoId) {
        if (videoId == null || jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement("DELETE FROM transcoded_segment_status WHERE video_id = ?")) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up transcode integration test data", e);
        }
    }

    private String normalizeJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        return url
                .replace("jdbc:postgresql://postgres:", "jdbc:postgresql://localhost:")
                .replace("jdbc:postgresql://host.docker.internal:", "jdbc:postgresql://localhost:");
    }

    private String firstNonBlank(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    private static final class TestWebSocketClient extends WebSocketListener {
        private final OkHttpClient client;
        private final List<JsonNode> messages = new CopyOnWriteArrayList<>();
        private final CountDownLatch opened = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile WebSocket webSocket;
        private volatile Throwable failure;

        private TestWebSocketClient(OkHttpClient client) {
            this.client = client;
        }

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            this.webSocket = webSocket;
            opened.countDown();
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            try {
                messages.add(OBJECT_MAPPER.readTree(text));
            } catch (Exception e) {
                failure = e;
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            failure = t;
            closed.countDown();
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            closed.countDown();
        }

        private boolean awaitMessage(Duration timeout, java.util.function.Predicate<JsonNode> predicate)
                throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                if (failure != null) {
                    throw new RuntimeException("WebSocket listener failed", failure);
                }
                for (JsonNode message : messages) {
                    if (predicate.test(message)) {
                        return true;
                    }
                }
                Thread.sleep(25);
            }
            return false;
        }

        private void awaitClosed(Duration timeout) throws InterruptedException {
            assertTrue(closed.await(timeout.toMillis(), TimeUnit.MILLISECONDS),
                    "websocket should close when the status-service instance stops");
        }

        private void close() {
            if (webSocket != null) {
                webSocket.close(1000, "test complete");
            }
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }
}
