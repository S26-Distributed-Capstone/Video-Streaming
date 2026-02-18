package com.distributed26.videostreaming.upload.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class SegmentUploadRepository {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public SegmentUploadRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static SegmentUploadRepository fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        String url = dotenv.get("PG_URL");
        String user = dotenv.get("PG_USER");
        String pass = dotenv.get("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new SegmentUploadRepository(url, user, pass);
    }

    public void insert(String videoId, int segmentNumber) {
        String sql = """
            INSERT INTO segment_upload (video_id, segment_number)
            VALUES (?, ?)
            ON CONFLICT (video_id, segment_number) DO NOTHING
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setInt(2, segmentNumber);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert segment_upload", e);
        }
    }

    public int countByVideoId(String videoId) {
        String sql = "SELECT COUNT(*) FROM segment_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count segment_upload", e);
        }
    }
}
