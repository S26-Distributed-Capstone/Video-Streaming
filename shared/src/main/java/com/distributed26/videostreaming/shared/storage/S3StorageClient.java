package com.distributed26.videostreaming.shared.storage;

import com.distributed26.videostreaming.shared.config.StorageConfig;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3StorageClient implements ObjectStorageClient {
    private static final Logger LOGGER = LogManager.getLogger(S3StorageClient.class);

    private final S3Client s3Client;
    private final String bucketName;

    public S3StorageClient(StorageConfig config) {
        this.bucketName = config.getDefaultBucketName();
        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(config.getEndpointUrl()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey())
                ))
                .region(Region.of(config.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
        LOGGER.info("Initialized S3StorageClient for bucket '{}' at '{}'", bucketName, config.getEndpointUrl());
    }

    @Override
    public void uploadFile(String key, InputStream data, long size) {
        LOGGER.info("Uploading object '{}' ({} bytes) to bucket '{}'", key, size, bucketName);
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.putObject(request, RequestBody.fromInputStream(data, size));
            LOGGER.info("Uploaded object '{}' to bucket '{}'", key, bucketName);
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to upload object '{}' to bucket '{}'", key, bucketName, ex);
            throw new IllegalStateException("Failed to upload object: " + key, ex);
        }
    }

    @Override
    public InputStream downloadFile(String key) {
        LOGGER.info("Downloading object '{}' from bucket '{}'", key, bucketName);
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            return s3Client.getObject(request);
        } catch (NoSuchKeyException ex) {
            LOGGER.warn("Object '{}' not found in bucket '{}'", key, bucketName);
            throw ex;
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to download object '{}' from bucket '{}'", key, bucketName, ex);
            throw new IllegalStateException("Failed to download object: " + key, ex);
        }
    }

    @Override
    public void deleteFile(String key) {
        LOGGER.info("Deleting object '{}' from bucket '{}'", key, bucketName);
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.deleteObject(request);
            LOGGER.info("Deleted object '{}' from bucket '{}'", key, bucketName);
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to delete object '{}' from bucket '{}'", key, bucketName, ex);
            throw new IllegalStateException("Failed to delete object: " + key, ex);
        }
    }

    @Override
    public boolean fileExists(String key) {
        LOGGER.info("Checking existence of object '{}' in bucket '{}'", key, bucketName);
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            LOGGER.error("Failed to check existence of object '{}' in bucket '{}'", key, bucketName, ex);
            throw new IllegalStateException("Failed to check object existence: " + key, ex);
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to check existence of object '{}' in bucket '{}'", key, bucketName, ex);
            throw new IllegalStateException("Failed to check object existence: " + key, ex);
        }
    }

    @Override
    public List<String> listFiles(String prefix) {
        LOGGER.info("Listing objects in bucket '{}' with prefix '{}'", bucketName, prefix);
        try {
            List<String> keys = new ArrayList<>();
            String continuationToken = null;
            do {
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(prefix)
                        .continuationToken(continuationToken)
                        .build();
                ListObjectsV2Response response = s3Client.listObjectsV2(request);
                for (S3Object object : response.contents()) {
                    keys.add(object.key());
                }
                continuationToken = response.nextContinuationToken();
            } while (continuationToken != null && !continuationToken.isEmpty());
            LOGGER.info("Listed {} object(s) in bucket '{}' with prefix '{}'", keys.size(), bucketName, prefix);
            return keys;
        } catch (RuntimeException ex) {
            LOGGER.error("Failed to list objects in bucket '{}' with prefix '{}'", bucketName, prefix, ex);
            throw new IllegalStateException("Failed to list objects with prefix: " + prefix, ex);
        }
    }
}
