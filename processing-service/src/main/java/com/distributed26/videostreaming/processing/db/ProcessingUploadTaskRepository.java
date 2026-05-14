package com.distributed26.videostreaming.processing.db;

import com.distributed26.videostreaming.processing.LocalSpoolUploadTask;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

public class ProcessingUploadTaskRepository {
    private final DataSource dataSource;

    public record UploadTaskMetadata(
            String spoolOwner,
            String spoolPath,
            String state,
            String claimedBy
    ) {
    }

    public ProcessingUploadTaskRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        ensureSpoolOwnerColumn();
    }

    public static ProcessingUploadTaskRepository fromEnv() {
        return new ProcessingUploadTaskRepository(ProcessingDataSource.create());
    }

    public void upsertPending(
            String videoId,
            String spoolOwner,
            String profile,
            int segmentNumber,
            String chunkKey,
            String outputKey,
            String spoolPath,
            long sizeBytes,
            double outputTsOffsetSeconds
    ) {
        String sql = """
            INSERT INTO processing_upload_task (
                video_id,
                spool_owner,
                profile,
                segment_number,
                chunk_key,
                output_key,
                spool_path,
                size_bytes,
                output_ts_offset_seconds,
                state,
                attempt_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, NOW())
            ON CONFLICT (video_id, profile, segment_number) DO UPDATE
            SET spool_owner = EXCLUDED.spool_owner,
                chunk_key = EXCLUDED.chunk_key,
                output_key = EXCLUDED.output_key,
                spool_path = EXCLUDED.spool_path,
                size_bytes = EXCLUDED.size_bytes,
                output_ts_offset_seconds = EXCLUDED.output_ts_offset_seconds,
                state = 'PENDING',
                claimed_by = NULL,
                updated_at = NOW()
            """;
        try (Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, spoolOwner);
            ps.setString(3, profile);
            ps.setInt(4, segmentNumber);
            ps.setString(5, chunkKey);
            ps.setString(6, outputKey);
            ps.setString(7, spoolPath);
            ps.setLong(8, sizeBytes);
            ps.setDouble(9, outputTsOffsetSeconds);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to upsert processing_upload_task", e);
        }
    }

    public Optional<LocalSpoolUploadTask> claimNextReady(String claimedBy, long staleMillis) {
        String sql = """
            WITH next_task AS (
                SELECT id
                FROM processing_upload_task
                WHERE spool_owner = ?
                  AND (
                        state = 'PENDING'
                     OR (state = 'UPLOADING' AND updated_at < NOW() - (? * INTERVAL '1 millisecond'))
                  )
                ORDER BY updated_at ASC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE processing_upload_task t
            SET state = 'UPLOADING',
                claimed_by = ?,
                attempt_count = t.attempt_count + 1,
                updated_at = NOW()
            FROM next_task
            WHERE t.id = next_task.id
            RETURNING t.id,
                      t.video_id,
                      t.spool_owner,
                      t.profile,
                      t.segment_number,
                      t.chunk_key,
                      t.output_key,
                      t.spool_path,
                      t.size_bytes,
                      t.output_ts_offset_seconds,
                      t.attempt_count
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, claimedBy);
            ps.setLong(2, staleMillis);
            ps.setString(3, claimedBy);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim processing_upload_task", e);
        }
    }

    public void markPending(long id) {
        updateState(id, "PENDING");
    }

    public int resetUploadingTasks() {
        String sql = """
            UPDATE processing_upload_task
            SET state = 'PENDING', claimed_by = NULL, updated_at = NOW()
            WHERE state = 'UPLOADING'
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to reset processing_upload_task state", e);
        }
    }

    public void deleteById(long id) {
        String sql = "DELETE FROM processing_upload_task WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete processing_upload_task", e);
        }
    }

    public boolean hasOpenTask(String videoId, String profile, int segmentNumber) {
        String sql = """
            SELECT 1
            FROM processing_upload_task
            WHERE video_id = ? AND profile = ? AND segment_number = ?
            LIMIT 1
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query processing_upload_task", e);
        }
    }

    public Optional<UploadTaskMetadata> findTask(String videoId, String profile, int segmentNumber) {
        String sql = """
            SELECT spool_owner, spool_path, state, claimed_by
            FROM processing_upload_task
            WHERE video_id = ? AND profile = ? AND segment_number = ?
            LIMIT 1
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new UploadTaskMetadata(
                        rs.getString("spool_owner"),
                        rs.getString("spool_path"),
                        rs.getString("state"),
                        rs.getString("claimed_by")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query processing_upload_task metadata", e);
        }
    }

    public Set<Integer> findOpenSegmentNumbers(String videoId, String profile) {
        String sql = """
            SELECT segment_number
            FROM processing_upload_task
            WHERE video_id = ? AND profile = ?
            """;
        Set<Integer> segmentNumbers = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    segmentNumbers.add(rs.getInt("segment_number"));
                }
            }
            return segmentNumbers;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list processing_upload_task segments", e);
        }
    }

    public List<String> findVideoIdsWithOpenTasks() {
        String sql = "SELECT DISTINCT video_id FROM processing_upload_task";
        List<String> videoIds = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                videoIds.add(rs.getString("video_id"));
            }
            return videoIds;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list processing_upload_task video ids", e);
        }
    }

    public int countByState(String state) {
        String sql = "SELECT COUNT(*) FROM processing_upload_task WHERE state = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count processing_upload_task by state", e);
        }
    }

    private void updateState(long id, String state) {
        String sql = """
            UPDATE processing_upload_task
            SET state = ?, claimed_by = NULL, updated_at = NOW()
            WHERE id = ?
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state);
            ps.setLong(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update processing_upload_task state", e);
        }
    }

    private LocalSpoolUploadTask mapRow(ResultSet rs) throws SQLException {
        return new LocalSpoolUploadTask(
                rs.getLong("id"),
                rs.getString("video_id"),
                rs.getString("spool_owner"),
                rs.getString("profile"),
                rs.getInt("segment_number"),
                rs.getString("chunk_key"),
                rs.getString("output_key"),
                rs.getString("spool_path"),
                rs.getLong("size_bytes"),
                rs.getDouble("output_ts_offset_seconds"),
                rs.getInt("attempt_count")
        );
    }

    private void ensureSpoolOwnerColumn() {
        String sql = """
            ALTER TABLE processing_upload_task
            ADD COLUMN IF NOT EXISTS spool_owner VARCHAR(128)
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to ensure processing_upload_task.spool_owner", e);
        }
    }
}
