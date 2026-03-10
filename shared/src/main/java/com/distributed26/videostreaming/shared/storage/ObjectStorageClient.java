package com.distributed26.videostreaming.shared.storage;

import java.io.InputStream;
import java.util.List;

public interface ObjectStorageClient {
    void uploadFile(String key, InputStream data, long size);

    InputStream downloadFile(String key);

    void deleteFile(String key);

    boolean fileExists(String key);

    List<String> listFiles(String prefix);

    void ensureBucketExists();

    /**
     * Generate a presigned GET URL for the given object key.
     *
     * @param key             the S3 object key
     * @param durationSeconds how long the URL should be valid
     * @return a fully-qualified URL that grants temporary read access
     */
    String generatePresignedUrl(String key, long durationSeconds);
}
