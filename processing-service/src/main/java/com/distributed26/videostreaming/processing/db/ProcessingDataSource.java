package com.distributed26.videostreaming.processing.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

/**
 * Factory for the shared HikariCP connection pool used by all processing-service
 * repositories.
 *
 * <p>A single {@link DataSource} instance is created once and shared across all
 * repositories so the total number of physical Postgres connections is bounded
 * regardless of how many worker threads are running concurrently.
 *
 * <p>Pool size is controlled by the {@code PG_POOL_SIZE} environment variable
 * (default 10). This should be tuned to:
 *   <pre>pool_size ≤ (postgres max_connections) / (number of processing pods)</pre>
 * For example with Postgres max_connections=100 and up to 10 pods: set PG_POOL_SIZE=8.
 */
public final class ProcessingDataSource {

    private ProcessingDataSource() {}

    /**
     * Creates a new {@link HikariDataSource} from environment variables.
     * <ul>
     *   <li>{@code PG_URL}      — JDBC URL (required)</li>
     *   <li>{@code PG_USER}     — Postgres user (required)</li>
     *   <li>{@code PG_PASSWORD} — Postgres password</li>
     *   <li>{@code PG_POOL_SIZE} — max pool size (default 10)</li>
     * </ul>
     *
     * <p>Call this <b>once</b> at application startup and share the returned
     * {@link DataSource} with all repositories. Close it in your shutdown hook.
     */
    public static HikariDataSource create() {
        String url  = System.getenv("PG_URL");
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("PG_URL is not set");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("PG_USER is not set");
        }

        int poolSize = 10;
        String poolSizeEnv = System.getenv("PG_POOL_SIZE");
        if (poolSizeEnv != null && !poolSizeEnv.isBlank()) {
            try {
                poolSize = Integer.parseInt(poolSizeEnv.trim());
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);
        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10_000);   // 10 s — fail fast rather than pile up
        config.setIdleTimeout(300_000);         // 5 min idle before connection is retired
        config.setMaxLifetime(1_800_000);       // 30 min max lifetime (avoids stale connections)
        config.setPoolName("processing-pool");
        config.addDataSourceProperty("ApplicationName", "processing-service");

        return new HikariDataSource(config);
    }
}

