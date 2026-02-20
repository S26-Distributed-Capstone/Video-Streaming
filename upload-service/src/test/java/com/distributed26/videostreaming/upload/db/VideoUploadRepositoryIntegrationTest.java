package com.distributed26.videostreaming.upload.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VideoUploadRepositoryIntegrationTest {
    private String jdbcUrl;
    private String username;
    private String password;

    private VideoUploadRepository repo;
    private String videoId;

    @BeforeEach
    void setUp() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = dotenv.get("PG_URL");
        username = dotenv.get("PG_USER");
        password = dotenv.get("PG_PASSWORD");

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }

        repo = new VideoUploadRepository(jdbcUrl, username, password);
        videoId = UUID.randomUUID().toString();
    }

    @AfterEach
    void tearDown() {
        if (jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM video_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up test data", e);
        }
    }

    @Test
    @Tag("integration")
    void createAndFindByVideoId() {
        repo.create(videoId, 3, "UPLOADING", "machine-a", "container-a");

        Optional<VideoUploadRecord> record = repo.findByVideoId(videoId);
        assertTrue(record.isPresent(), "Expected record to exist");

        VideoUploadRecord r = record.get();
        assertEquals(videoId, r.getVideoId());
        assertEquals(3, r.getTotalSegments());
        assertEquals("UPLOADING", r.getStatus());
        assertEquals("machine-a", r.getMachineId());
    }

    @Test
    @Tag("integration")
    void updateStatusAndTotalSegments() {
        repo.create(videoId, 2, "UPLOADING", "machine-b", "container-b");

        repo.updateStatus(videoId, "PROCESSING");
        repo.updateTotalSegments(videoId, 5);

        Optional<VideoUploadRecord> record = repo.findByVideoId(videoId);
        assertTrue(record.isPresent(), "Expected record to exist");

        VideoUploadRecord r = record.get();
        assertEquals("PROCESSING", r.getStatus());
        assertEquals(5, r.getTotalSegments());
        assertEquals("machine-b", r.getMachineId());
    }
}
