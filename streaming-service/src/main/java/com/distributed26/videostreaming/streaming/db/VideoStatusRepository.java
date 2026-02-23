package com.distributed26.videostreaming.streaming.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VideoStatusRepository {
    private static final Logger logger = LogManager.getLogger(VideoStatusRepository.class);
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public VideoStatusRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static VideoStatusRepository fromEnv() {
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
        return new VideoStatusRepository(url, user, pass);
    }

    public Optional<String> findStatusByVideoId(String videoId) {
        String sql = "SELECT status FROM video_upload WHERE video_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.ofNullable(rs.getString("status"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query video_upload status", e);
        }
    }

    public List<String> findCompletedVideoIds(int limit) {
        String sql = "SELECT video_id FROM video_upload WHERE status = 'COMPLETED' ORDER BY id DESC LIMIT ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(rs.getString("video_id"));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query completed video IDs", e);
        }
    }
}
