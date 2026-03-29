package com.distributed26.videostreaming.processing.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProcessingTaskClaimRepository {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingTaskClaimRepository.class);

    public enum ClaimResult {
        ACQUIRED,
        HELD_BY_OTHER,
        ERROR
    }

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public ProcessingTaskClaimRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static ProcessingTaskClaimRepository fromEnv() {
        String url = System.getenv("PG_URL");
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }
        return new ProcessingTaskClaimRepository(url, user, pass);
    }

    public ClaimResult claim(String videoId, String profile, int segmentNumber, String stage, String claimedBy, long staleMillis) {
        String sql = """
            INSERT INTO processing_task_claim (
                video_id,
                profile,
                segment_number,
                stage,
                claimed_by,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, NOW())
            ON CONFLICT (video_id, profile, segment_number) DO UPDATE
            SET stage = EXCLUDED.stage,
                claimed_by = EXCLUDED.claimed_by,
                updated_at = NOW()
            WHERE processing_task_claim.claimed_by = EXCLUDED.claimed_by
               OR processing_task_claim.updated_at < NOW() - (? * INTERVAL '1 millisecond')
            RETURNING id
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setString(4, stage);
            ps.setString(5, claimedBy);
            ps.setLong(6, Math.max(0L, staleMillis));
            try (var rs = ps.executeQuery()) {
                return rs.next() ? ClaimResult.ACQUIRED : ClaimResult.HELD_BY_OTHER;
            }
        } catch (SQLException | RuntimeException e) {
            LOGGER.warn("Failed to claim processing_task_claim videoId={} profile={} segment={} stage={} claimedBy={}",
                    videoId, profile, segmentNumber, stage, claimedBy, e);
            return ClaimResult.ERROR;
        }
    }

    public boolean hasActiveClaim(String videoId, String profile, int segmentNumber, long staleMillis) {
        String sql = """
            SELECT 1
            FROM processing_task_claim
            WHERE video_id = ?
              AND profile = ?
              AND segment_number = ?
              AND updated_at >= NOW() - (? * INTERVAL '1 millisecond')
            LIMIT 1
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setLong(4, Math.max(0L, staleMillis));
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException | RuntimeException e) {
            LOGGER.warn("Failed to query active processing_task_claim videoId={} profile={} segment={}",
                    videoId, profile, segmentNumber, e);
            return false;
        }
    }

    public java.util.Set<Integer> findClaimedSegmentNumbers(String videoId, String profile, long staleMillis) {
        String sql = """
            SELECT segment_number
            FROM processing_task_claim
            WHERE video_id = ?
              AND profile = ?
              AND updated_at >= NOW() - (? * INTERVAL '1 millisecond')
            """;
        java.util.Set<Integer> segmentNumbers = new java.util.HashSet<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setLong(3, Math.max(0L, staleMillis));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    segmentNumbers.add(rs.getInt("segment_number"));
                }
            }
            return segmentNumbers;
        } catch (SQLException | RuntimeException e) {
            throw new RuntimeException("Failed to list processing_task_claim segments", e);
        }
    }

    public boolean release(String videoId, String profile, int segmentNumber) {
        String sql = """
            DELETE FROM processing_task_claim
            WHERE video_id = ? AND profile = ? AND segment_number = ?
            """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.executeUpdate();
            return true;
        } catch (SQLException | RuntimeException e) {
            LOGGER.warn("Failed to release processing_task_claim videoId={} profile={} segment={}",
                    videoId, profile, segmentNumber, e);
            return false;
        }
    }
}
