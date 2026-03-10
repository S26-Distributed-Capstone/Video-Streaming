package com.distributed26.videostreaming.shared.config;

import java.util.Objects;

public class StorageConfig {
    private final String endpointUrl;
    private final String publicEndpointUrl;
    private final String accessKey;
    private final String secretKey;
    private final String defaultBucketName;
    private final String region;

    public StorageConfig(
            String endpointUrl,
            String accessKey,
            String secretKey,
            String defaultBucketName,
            String region
    ) {
        this(endpointUrl, null, accessKey, secretKey, defaultBucketName, region);
    }

    public StorageConfig(
            String endpointUrl,
            String publicEndpointUrl,
            String accessKey,
            String secretKey,
            String defaultBucketName,
            String region
    ) {
        this.endpointUrl = Objects.requireNonNull(endpointUrl, "endpointUrl");
        this.publicEndpointUrl = publicEndpointUrl != null && !publicEndpointUrl.isBlank()
                ? publicEndpointUrl : endpointUrl;
        this.accessKey = Objects.requireNonNull(accessKey, "accessKey");
        this.secretKey = Objects.requireNonNull(secretKey, "secretKey");
        this.defaultBucketName = Objects.requireNonNull(defaultBucketName, "defaultBucketName");
        this.region = Objects.requireNonNull(region, "region");
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    /** Endpoint reachable by external clients (browsers). Used for presigned URLs. */
    public String getPublicEndpointUrl() {
        return publicEndpointUrl;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return "StorageConfig{" +
                "endpointUrl='" + endpointUrl + '\'' +
                ", publicEndpointUrl='" + publicEndpointUrl + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='***'" +
                ", defaultBucketName='" + defaultBucketName + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
