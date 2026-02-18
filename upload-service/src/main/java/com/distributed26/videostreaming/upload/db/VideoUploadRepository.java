package com.distributed26.videostreaming.upload.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.Optional;

public class VideoUploadRepository {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public VideoUploadRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static VideoUploadRepository fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        String url = dotenv.get("PG_URL");
        String user = dotenv.get("PG_USER");
        String pass = dotenv.get("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new VideoUploadRepository(url, user, pass);
    }

    public void create(String videoId, int totalSegments, String status, String machineId) {
        String sql = """
            INSERT INTO video_upload (video_id, total_segments, status, machine_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (video_id) DO UPDATE
            SET total_segments = EXCLUDED.total_segments,
                status = EXCLUDED.status,
                machine_id = EXCLUDED.machine_id
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setInt(2, totalSegments);
            ps.setString(3, status);
            ps.setString(4, machineId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert video_upload", e);
        }
    }

    public void updateStatus(String videoId, String status) {
        String sql = "UPDATE video_upload SET status = ? WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setObject(2, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update video_upload status", e);
        }
    }

    public void updateTotalSegments(String videoId, int totalSegments) {
        String sql = "UPDATE video_upload SET total_segments = ? WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, totalSegments);
            ps.setObject(2, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update video_upload total_segments", e);
        }
    }

    public Optional<VideoUploadRecord> findByVideoId(String videoId) {
        String sql = "SELECT video_id, total_segments, status, machine_id FROM video_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String id = rs.getString("video_id");
                int totalSegments = rs.getInt("total_segments");
                String status = rs.getString("status");
                String machineId = rs.getString("machine_id");
                return Optional.of(new VideoUploadRecord(id, totalSegments, status, machineId));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query video_upload", e);
        }
    }
}
