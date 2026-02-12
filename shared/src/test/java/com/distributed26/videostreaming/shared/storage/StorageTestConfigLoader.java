package com.distributed26.videostreaming.shared.storage;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import io.github.cdimascio.dotenv.Dotenv;

final class StorageTestConfigLoader {
    private StorageTestConfigLoader() {
    }

    static StorageConfig load() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();

        return new StorageConfig(
                require(dotenv, "MINIO_ENDPOINT"),
                require(dotenv, "MINIO_ACCESS_KEY"),
                require(dotenv, "MINIO_SECRET_KEY"),
                require(dotenv, "MINIO_BUCKET_NAME"),
                require(dotenv, "MINIO_REGION")
        );
    }

    private static String require(Dotenv dotenv, String key) {
        String value = dotenv.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value.trim();
    }
}
