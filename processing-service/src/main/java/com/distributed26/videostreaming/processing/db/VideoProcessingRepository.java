package com.distributed26.videostreaming.processing.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalInt;
import java.util.UUID;

public class VideoProcessingRepository {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public VideoProcessingRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static VideoProcessingRepository fromEnv() {
        String url = System.getenv("PG_URL");
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new VideoProcessingRepository(url, user, pass);
    }

    public OptionalInt findTotalSegments(String videoId) {
        String sql = "SELECT total_segments FROM video_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return OptionalInt.empty();
                }
                return OptionalInt.of(rs.getInt("total_segments"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query video_upload total_segments", e);
        }
    }
}
