package com.distributed26.videostreaming.processing.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProcessingTaskClaimRepository {
    private static final Logger LOGGER = LogManager.getLogger(ProcessingTaskClaimRepository.class);

    public enum ClaimResult {
        ACQUIRED,
        HELD_BY_OTHER,
        ERROR
    }

    private final DataSource dataSource;

    public ProcessingTaskClaimRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static ProcessingTaskClaimRepository fromEnv() {
        return new ProcessingTaskClaimRepository(ProcessingDataSource.create());
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
        try (Connection conn = dataSource.getConnection();
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
        try (Connection conn = dataSource.getConnection();
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

    public boolean heartbeat(String videoId, String profile, int segmentNumber, String stage, String claimedBy) {
        String sql = """
            UPDATE processing_task_claim
            SET stage = ?,
                updated_at = NOW()
            WHERE video_id = ?
              AND profile = ?
              AND segment_number = ?
              AND claimed_by = ?
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, stage);
            ps.setObject(2, UUID.fromString(videoId));
            ps.setString(3, profile);
            ps.setInt(4, segmentNumber);
            ps.setString(5, claimedBy);
            return ps.executeUpdate() > 0;
        } catch (SQLException | RuntimeException e) {
            LOGGER.warn("Failed to heartbeat processing_task_claim videoId={} profile={} segment={} stage={} claimedBy={}",
                    videoId, profile, segmentNumber, stage, claimedBy, e);
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
        try (Connection conn = dataSource.getConnection();
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
            LOGGER.warn("Failed to list processing_task_claim segments videoId={} profile={}",
                    videoId, profile, e);
            return java.util.Set.of();
        }
    }

    public boolean release(String videoId, String profile, int segmentNumber) {
        String sql = """
            DELETE FROM processing_task_claim
            WHERE video_id = ? AND profile = ? AND segment_number = ?
            """;
        try (Connection conn = dataSource.getConnection();
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
