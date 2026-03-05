package com.distributed26.videostreaming.upload.db;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class TranscodedSegmentStatusRepository {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public TranscodedSegmentStatusRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static TranscodedSegmentStatusRepository fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        String url = getenvOrDotenv(dotenv, "PG_URL");
        String user = getenvOrDotenv(dotenv, "PG_USER");
        String pass = getenvOrDotenv(dotenv, "PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new TranscodedSegmentStatusRepository(url, user, pass);
    }

    private static String getenvOrDotenv(Dotenv dotenv, String key) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        return dotenv.get(key);
    }

    public int countByState(String videoId, String profile, String state) {
        String sql = """
            SELECT COUNT(*) FROM transcoded_segment_status
            WHERE video_id = ? AND profile = ? AND state = ?
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setString(3, state);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count transcoded_segment_status", e);
        }
    }
}
