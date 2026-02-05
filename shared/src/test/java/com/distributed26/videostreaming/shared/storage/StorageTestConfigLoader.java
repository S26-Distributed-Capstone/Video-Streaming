package com.distributed26.videostreaming.shared.storage;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class StorageTestConfigLoader {
    private StorageTestConfigLoader() {
    }

    static StorageConfig load() {
        Properties properties = new Properties();
        try (InputStream input = StorageTestConfigLoader.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load application.properties", ex);
        }

        return new StorageConfig(
                require(properties, "minio.endpoint"),
                require(properties, "minio.access-key"),
                require(properties, "minio.secret-key"),
                require(properties, "minio.bucket-name"),
                require(properties, "minio.region")
        );
    }

    private static String require(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }
        return value.trim();
    }
}
