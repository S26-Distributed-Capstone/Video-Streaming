package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TerminalFailureStorageCleanupTest {

    private static final class StubStorageClient implements ObjectStorageClient {
        private final List<String> listedPrefixes = new ArrayList<>();
        private final List<String> deletedKeys = new ArrayList<>();
        private CountDownLatch deleteLatch = new CountDownLatch(0);

        @Override
        public void uploadFile(String key, InputStream data, long size) {
        }

        @Override
        public InputStream downloadFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void deleteFile(String key) {
            deletedKeys.add(key);
            deleteLatch.countDown();
        }

        @Override
        public boolean fileExists(String key) {
            return false;
        }

        @Override
        public synchronized List<String> listFiles(String prefix) {
            listedPrefixes.add(prefix);
            return List.of(prefix + "metadata.json", prefix + "chunks/output0.ts");
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class StubVideoUploadRepository extends VideoUploadRepository {
        private boolean failed;

        StubVideoUploadRepository() {
            super("jdbc:test", "user", "pass");
        }

        @Override
        public boolean isFailed(String videoId) {
            return failed;
        }
    }

    @Test
    void deletesAllObjectsForFailedVideo() throws Exception {
        StubStorageClient storageClient = new StubStorageClient();
        storageClient.deleteLatch = new CountDownLatch(2);
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        repository.failed = true;
        TerminalFailureStorageCleanup cleanup = new TerminalFailureStorageCleanup(storageClient, repository);
        String videoId = "6ca8f040-b643-4b6b-ab2e-dc1c92913e8d";

        try {
            cleanup.onEvent(new UploadFailedEvent(videoId, "container_died", "machine", "container"));

            assertTrue(storageClient.deleteLatch.await(3, TimeUnit.SECONDS), "cleanup should delete all objects");
            assertEquals(List.of(videoId + "/"), storageClient.listedPrefixes);
            assertEquals(
                    List.of(videoId + "/metadata.json", videoId + "/chunks/output0.ts"),
                    storageClient.deletedKeys
            );
        } finally {
            cleanup.close();
        }
    }

    @Test
    void skipsCleanupWhenFailureIsNotTerminal() throws Exception {
        StubStorageClient storageClient = new StubStorageClient();
        storageClient.deleteLatch = new CountDownLatch(1);
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        TerminalFailureStorageCleanup cleanup = new TerminalFailureStorageCleanup(storageClient, repository);
        String videoId = "6ca8f040-b643-4b6b-ab2e-dc1c92913e8d";

        try {
            cleanup.onEvent(new UploadFailedEvent(videoId, "container_died", "machine", "container"));

            assertTrue(!storageClient.deleteLatch.await(300, TimeUnit.MILLISECONDS), "cleanup should not run");
            assertEquals(List.of(), storageClient.listedPrefixes);
            assertEquals(List.of(), storageClient.deletedKeys);
        } finally {
            cleanup.close();
        }
    }

    @Test
    void deletesObjectsForUserCancelledUpload() throws Exception {
        StubStorageClient storageClient = new StubStorageClient();
        storageClient.deleteLatch = new CountDownLatch(2);
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        TerminalFailureStorageCleanup cleanup = new TerminalFailureStorageCleanup(storageClient, repository);
        String videoId = "6ca8f040-b643-4b6b-ab2e-dc1c92913e8d";

        try {
            cleanup.onEvent(new UploadFailedEvent(videoId, "user_cancelled", "machine", "container"));

            assertTrue(storageClient.deleteLatch.await(3, TimeUnit.SECONDS), "cleanup should delete cancelled upload objects");
            assertEquals(List.of(videoId + "/"), storageClient.listedPrefixes);
            assertEquals(
                    List.of(videoId + "/metadata.json", videoId + "/chunks/output0.ts"),
                    storageClient.deletedKeys
            );
        } finally {
            cleanup.close();
        }
    }
}
