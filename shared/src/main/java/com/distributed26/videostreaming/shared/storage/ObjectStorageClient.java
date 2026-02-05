package com.distributed26.videostreaming.shared.storage;

import java.io.InputStream;
import java.util.List;

public interface ObjectStorageClient {
    void uploadFile(String key, InputStream data, long size);

    InputStream downloadFile(String key);

    void deleteFile(String key);

    boolean fileExists(String key);

    List<String> listFiles(String prefix);
}
