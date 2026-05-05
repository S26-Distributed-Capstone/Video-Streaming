package com.distributed26.videostreaming.processing.db;

import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
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
        String url = System.getenv("PG_URL");
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new TranscodedSegmentStatusRepository(url, user, pass);
    }

    public void upsertState(String videoId, String profile, int segmentNumber, TranscodeSegmentState state) {
        String sql = """
            INSERT INTO transcoded_segment_status (video_id, profile, segment_number, state)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (video_id, profile, segment_number) DO UPDATE
            SET state = CASE
                WHEN transcoded_segment_status.state = 'DONE' AND EXCLUDED.state <> 'DONE'
                    THEN transcoded_segment_status.state
                ELSE EXCLUDED.state
            END,
            updated_at = NOW()
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setString(4, state.name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to upsert transcoded_segment_status", e);
        }
    }

    public int countByState(String videoId, String profile, TranscodeSegmentState state) {
        String sql = """
            SELECT COUNT(*) FROM transcoded_segment_status
            WHERE video_id = ? AND profile = ? AND state = ?
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setString(3, state.name());
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

    public boolean hasState(String videoId, String profile, int segmentNumber, TranscodeSegmentState state) {
        String sql = """
            SELECT 1 FROM transcoded_segment_status
            WHERE video_id = ? AND profile = ? AND segment_number = ? AND state = ?
            LIMIT 1
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setString(4, state.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query transcoded_segment_status", e);
        }
    }

    public Set<Integer> findSegmentNumbersByState(String videoId, String profile, TranscodeSegmentState state) {
        String sql = """
            SELECT segment_number FROM transcoded_segment_status
            WHERE video_id = ? AND profile = ? AND state = ?
            """;
        Set<Integer> segmentNumbers = new HashSet<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setString(3, state.name());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    segmentNumbers.add(rs.getInt("segment_number"));
                }
            }
            return segmentNumbers;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list transcoded_segment_status", e);
        }
    }

    public Set<Integer> findSegmentNumbersByStateUpdatedSince(
            String videoId,
            String profile,
            TranscodeSegmentState state,
            long freshnessMillis
    ) {
        String sql = """
            SELECT segment_number FROM transcoded_segment_status
            WHERE video_id = ? AND profile = ? AND state = ?
              AND updated_at >= NOW() - (? * INTERVAL '1 millisecond')
            """;
        Set<Integer> segmentNumbers = new HashSet<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setString(3, state.name());
            ps.setLong(4, Math.max(0L, freshnessMillis));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    segmentNumbers.add(rs.getInt("segment_number"));
                }
            }
            return segmentNumbers;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list fresh transcoded_segment_status", e);
        }
    }
}
