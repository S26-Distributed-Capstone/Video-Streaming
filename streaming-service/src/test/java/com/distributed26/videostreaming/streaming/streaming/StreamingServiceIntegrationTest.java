package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import com.distributed26.videostreaming.shared.storage.S3StorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
class StreamingServiceIntegrationTest {
    private Dotenv dotenv;
    private String jdbcUrl;
    private String username;
    private String password;
    private StorageConfig storageConfig;
    private S3StorageClient storageClient;
    private Javalin app;
    private int port;
    private HttpClient httpClient;
    private String videoId;

    @BeforeEach
    void setUp() {
        dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = normalizeJdbcUrl(dotenv.get("PG_URL"));
        username = dotenv.get("PG_USER");
        password = dotenv.get("PG_PASSWORD");

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }

        storageConfig = new StorageConfig(
            normalizeMinioEndpoint(getenvOrDefault("MINIO_ENDPOINT", "http://localhost:9000")),
            getenvOrDefault("MINIO_ACCESS_KEY", "minioadmin"),
            getenvOrDefault("MINIO_SECRET_KEY", "minioadmin"),
            getenvOrDefault("MINIO_BUCKET_NAME", "uploads"),
            getenvOrDefault("MINIO_REGION", "us-east-1")
        );
        storageClient = new S3StorageClient(storageConfig);
        storageClient.ensureBucketExists();

        videoId = UUID.randomUUID().toString();
        insertVideoStatus(videoId, "COMPLETED");

        byte[] manifest = "#EXTM3U\n#EXTINF:10,\n000.ts\n".getBytes(StandardCharsets.UTF_8);
        storageClient.uploadFile(videoId + "/chunks/output.m3u8",
            new ByteArrayInputStream(manifest), manifest.length);
        byte[] segment = "segment-000".getBytes(StandardCharsets.UTF_8);
        storageClient.uploadFile(videoId + "/chunks/000.ts",
            new ByteArrayInputStream(segment), segment.length);

        VideoStatusRepository repo = new VideoStatusRepository(jdbcUrl, username, password);
        app = StreamingServiceApplication.createStreamingApp(storageClient, repo);
        app.start(0);
        port = app.port();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (app != null) {
            app.stop();
        }
        if (videoId != null) {
            deleteVideoStatus(videoId);
            storageClient.deleteFile(videoId + "/chunks/output.m3u8");
            storageClient.deleteFile(videoId + "/chunks/000.ts");
        }
    }

    @Test
    void servesManifestFromMinioWhenCompleted() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/manifest"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        String body = new String(response.body(), StandardCharsets.UTF_8);
        assertEquals("#EXTM3U\n#EXTINF:10,\nsegment/000.ts\n", body);
    }

    @Test
    void servesSegmentFromMinioWhenCompleted() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/segment/000.ts"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        String body = new String(response.body(), StandardCharsets.UTF_8);
        assertEquals("segment-000", body);
    }

    private String getenvOrDefault(String key, String defaultValue) {
        String val = dotenv.get(key);
        if (val == null || val.isBlank()) {
            val = System.getenv(key);
        }
        return val == null || val.isBlank() ? defaultValue : val;
    }

    private String normalizeJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        return url.replace("jdbc:postgresql://postgres:", "jdbc:postgresql://localhost:");
    }

    private String normalizeMinioEndpoint(String endpoint) {
        if (endpoint == null) {
            return null;
        }
        return endpoint.replace("http://minio:", "http://localhost:");
    }

    private void insertVideoStatus(String videoId, String status) {
        String sql = """
            INSERT INTO video_upload (video_id, total_segments, status, machine_id, container_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (video_id) DO UPDATE
            SET status = EXCLUDED.status
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setInt(2, 1);
            ps.setString(3, status);
            ps.setString(4, "it-machine");
            ps.setString(5, "it-container");
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert video_upload", e);
        }
    }

    private void deleteVideoStatus(String videoId) {
        String sql = "DELETE FROM video_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete video_upload", e);
        }
    }
}
