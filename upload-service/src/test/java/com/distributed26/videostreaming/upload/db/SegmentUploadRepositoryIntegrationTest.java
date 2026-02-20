package com.distributed26.videostreaming.upload.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SegmentUploadRepositoryIntegrationTest {
    private String jdbcUrl;
    private String username;
    private String password;

    private SegmentUploadRepository segmentRepo;
    private VideoUploadRepository videoRepo;
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

        segmentRepo = new SegmentUploadRepository(jdbcUrl, username, password);
        videoRepo = new VideoUploadRepository(jdbcUrl, username, password);

        videoId = UUID.randomUUID().toString();
        videoRepo.create(videoId, 3, "UPLOADING", "test-machine", "test-container");
    }

    @AfterEach
    void tearDown() {
        if (jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM segment_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
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
    void insertAndCountByVideoId() {
        assertEquals(0, segmentRepo.countByVideoId(videoId));

        segmentRepo.insert(videoId, 1);
        segmentRepo.insert(videoId, 2);

        assertEquals(2, segmentRepo.countByVideoId(videoId));
    }

    @Test
    @Tag("integration")
    void insertIsIdempotentForSameSegment() {
        segmentRepo.insert(videoId, 1);
        segmentRepo.insert(videoId, 1);

        assertEquals(1, segmentRepo.countByVideoId(videoId));
    }
}
