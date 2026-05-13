package com.distributed26.videostreaming.processing.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import javax.sql.DataSource;

public class VideoProcessingRepository {
    private final DataSource dataSource;

    public VideoProcessingRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static VideoProcessingRepository fromEnv() {
        return new VideoProcessingRepository(ProcessingDataSource.create());
    }

    public OptionalInt findTotalSegments(String videoId) {
        String sql = "SELECT total_segments FROM video_upload WHERE video_id = ?";
        try (Connection conn = dataSource.getConnection();
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

    public Optional<String> findStatusByVideoId(String videoId) {
        String sql = "SELECT status FROM video_upload WHERE video_id = ?";
        try (Connection conn = dataSource.getConnection();
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

    public List<String> findVideoIdsByStatus(String status) {
        String sql = "SELECT video_id FROM video_upload WHERE status = ?";
        List<String> videoIds = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    videoIds.add(rs.getString("video_id"));
                }
            }
            return videoIds;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query video_upload by status", e);
        }
    }

    public void updateStatus(String videoId, String status) {
        String sql = "UPDATE video_upload SET status = ? WHERE video_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setObject(2, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update video_upload status", e);
        }
    }

    public boolean markProcessingIfPending(String videoId) {
        String sql = """
                UPDATE video_upload
                SET status = 'PROCESSING'
                WHERE video_id = ?
                  AND status IN ('UPLOADED', 'WAITING_FOR_STORAGE')
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to conditionally update video_upload status to PROCESSING", e);
        }
    }

    public boolean isFailed(String videoId) {
        String sql = "SELECT 1 FROM video_upload WHERE video_id = ? AND status = 'FAILED' LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query video_upload failed status", e);
        }
    }
}
