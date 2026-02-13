package com.distributed26.videostreaming.upload.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import io.github.cdimascio.dotenv.Dotenv;

public class JobTaskRepository {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public JobTaskRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static JobTaskRepository fromEnv() {
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
        return new JobTaskRepository(url, user, pass);
    }

    public Optional<JobTaskRecord> findByJobId(String jobId) {
        String sql = "SELECT job_id, num_tasks FROM job_tasks WHERE job_id = ?";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String id = rs.getString("job_id");
                int numTasks = rs.getInt("num_tasks");
                return Optional.of(new JobTaskRecord(id, numTasks));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query job_tasks", e);
        }
    }
}
