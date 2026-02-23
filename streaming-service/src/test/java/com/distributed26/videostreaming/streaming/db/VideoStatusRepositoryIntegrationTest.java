package com.distributed26.videostreaming.streaming.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class VideoStatusRepositoryIntegrationTest {
    private String jdbcUrl;
    private String username;
    private String password;
    private VideoStatusRepository repo;
    private String videoId;

    @BeforeEach
    void setUp() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = normalizeJdbcUrl(dotenv.get("PG_URL"));
        username = dotenv.get("PG_USER");
        password = dotenv.get("PG_PASSWORD");

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }

        repo = new VideoStatusRepository(jdbcUrl, username, password);
        videoId = UUID.randomUUID().toString();
    }

    @AfterEach
    void tearDown() {
        if (jdbcUrl == null || username == null) {
            return;
        }
        String sql = "DELETE FROM video_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to clean up test data", e);
        }
    }

    @Test
    void findsStatusByVideoId() {
        insertVideo(videoId, "COMPLETED");

        Optional<String> status = repo.findStatusByVideoId(videoId);
        assertTrue(status.isPresent(), "Expected status to exist");
        assertEquals("COMPLETED", status.get());
    }

    private void insertVideo(String id, String status) {
        String sql = """
            INSERT INTO video_upload (video_id, total_segments, status, machine_id, container_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (video_id) DO UPDATE
            SET status = EXCLUDED.status
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(id));
            ps.setInt(2, 1);
            ps.setString(3, status);
            ps.setString(4, "it-machine");
            ps.setString(5, "it-container");
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert video_upload", e);
        }
    }

    private String normalizeJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        return url.replace("jdbc:postgresql://postgres:", "jdbc:postgresql://localhost:");
    }
}
