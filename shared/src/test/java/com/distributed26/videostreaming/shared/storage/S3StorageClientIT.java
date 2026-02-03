package com.distributed26.videostreaming.shared.storage;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Tag("integration")
class S3StorageClientIT {
    private static StorageConfig config;
    private static S3StorageClient storageClient;
    private static S3Client adminClient;
    private static String bucketName;

    @BeforeAll
    static void setUp() {
        config = StorageTestConfigLoader.load();
        bucketName = config.getDefaultBucketName();
        adminClient = buildS3Client(config);
        storageClient = new S3StorageClient(config);

        assertMinioAvailable();
        ensureBucketExists(bucketName);
    }

    @AfterAll
    static void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Test
    void createsBucketIfMissing() {
        String tempBucket = bucketName + "-it-" + UUID.randomUUID().toString().substring(0, 8);

        ensureBucketExists(tempBucket);
        Assertions.assertTrue(bucketExists(tempBucket), "Expected temp bucket to exist after creation");

        deleteBucket(tempBucket);
    }

    @Test
    void uploadDownloadDeleteRoundTrip() throws Exception {
        String keyPrefix = "it/" + UUID.randomUUID() + "/";
        String key = keyPrefix + "hello.txt";
        byte[] payload = "hello-minio".getBytes(StandardCharsets.UTF_8);

        storageClient.uploadFile(key, toStream(payload), payload.length);
        Assertions.assertTrue(storageClient.fileExists(key), "Expected uploaded object to exist");

        List<String> keys = storageClient.listFiles(keyPrefix);
        Assertions.assertTrue(keys.contains(key), "Expected list to contain uploaded object");

        try (InputStream downloaded = storageClient.downloadFile(key)) {
            byte[] received = downloaded.readAllBytes();
            Assertions.assertArrayEquals(payload, received, "Downloaded content should match uploaded bytes");
        }

        storageClient.deleteFile(key);
        Assertions.assertFalse(storageClient.fileExists(key), "Expected object to be deleted");
    }

    private static S3Client buildS3Client(StorageConfig config) {
        return S3Client.builder()
                .endpointOverride(URI.create(config.getEndpointUrl()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey())
                ))
                .region(Region.of(config.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    private static void ensureBucketExists(String bucket) {
        if (bucketExists(bucket)) {
            return;
        }

        CreateBucketRequest.Builder request = CreateBucketRequest.builder().bucket(bucket);
        if (!"us-east-1".equals(config.getRegion())) {
            request.createBucketConfiguration(CreateBucketConfiguration.builder()
                    .locationConstraint(config.getRegion())
                    .build());
        }
        adminClient.createBucket(request.build());
    }

    private static boolean bucketExists(String bucket) {
        try {
            adminClient.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            return true;
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            throw ex;
        } catch (SdkException ex) {
            throw ex;
        }
    }

    private static void deleteBucket(String bucket) {
        ListObjectsV2Response objects = adminClient.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .build());
        objects.contents().forEach(object -> adminClient.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(object.key())
                .build()));
        adminClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
    }

    private static InputStream toStream(byte[] payload) {
        return new java.io.ByteArrayInputStream(payload);
    }

    private static void assertMinioAvailable() {
        try {
            adminClient.listBuckets();
        } catch (SdkException ex) {
            Assertions.fail("MinIO is not reachable at " + config.getEndpointUrl(), ex);
        }
    }
}
