package com.distributed26.videostreaming.shared.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import java.net.URI;
import org.junit.jupiter.api.Test;

class S3StorageClientTest {

    @Test
    void generatePresignedUrlUsesConfiguredPublicEndpoint() {
        StorageConfig config = new StorageConfig(
                "http://minio:9000",
                "http://playback.example.com:9000",
                "minioadmin",
                "minioadmin",
                "uploads",
                "us-east-1"
        );

        String presignedUrl;
        try (S3StorageClient client = new S3StorageClient(config)) {
            presignedUrl = client.generatePresignedUrl("video-1/processed/low/output0.ts", 60);
        }

        URI uri = URI.create(presignedUrl);
        assertEquals("playback.example.com", uri.getHost(),
                "presigned URL should use the browser-reachable public endpoint host");
        assertEquals(9000, uri.getPort(),
                "presigned URL should preserve the configured public endpoint port");
        assertTrue(uri.getPath().startsWith("/uploads/video-1/processed/low/output0.ts"),
                "presigned URL should use path-style bucket addressing on the public endpoint");
    }
}
