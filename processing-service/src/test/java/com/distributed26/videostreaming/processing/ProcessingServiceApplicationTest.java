package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.processing.runtime.ProcessingRuntime;
import com.distributed26.videostreaming.processing.runtime.StartupRecoveryService;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ProcessingServiceApplicationTest {

    @AfterEach
    void tearDown() throws Exception {
        Field bucketThreadField = ProcessingServiceApplication.class.getDeclaredField("bucketEnsureThread");
        bucketThreadField.setAccessible(true);
        Thread bucketThread = (Thread) bucketThreadField.get(null);
        if (bucketThread != null) {
            bucketThread.interrupt();
            bucketThread.join(5_000);
            bucketThreadField.set(null, null);
        }
        ProcessingServiceApplication.resetState();
    }

    @Test
    void startupBucketEnsureThreadRetriesUntilStorageRecovers() throws Exception {
        ProcessingRuntime runtime = new ProcessingRuntime(
                null, null, null, null, null, null, null, null, null, null
        );
        StartupRecoveryService startupRecovery = new StartupRecoveryService(
                ProcessingServiceApplication.PROFILES,
                runtime
        );
        RecoveringEnsureBucketStorageClient storageClient = new RecoveringEnsureBucketStorageClient(2);

        Method startBucketEnsureThread = ProcessingServiceApplication.class.getDeclaredMethod(
                "startBucketEnsureThread",
                ObjectStorageClient.class,
                StartupRecoveryService.class,
                Path.class,
                com.distributed26.videostreaming.shared.upload.RabbitMQDevLogPublisher.class,
                long.class
        );
        startBucketEnsureThread.setAccessible(true);
        startBucketEnsureThread.invoke(null, storageClient, startupRecovery, null, null, 1_000L);

        Field bucketThreadField = ProcessingServiceApplication.class.getDeclaredField("bucketEnsureThread");
        bucketThreadField.setAccessible(true);
        Thread bucketThread = (Thread) bucketThreadField.get(null);
        assertNotNull(bucketThread, "startup should spawn the bucket ensure thread");

        assertTrue(storageClient.awaitRecovered(5, TimeUnit.SECONDS),
                "bucket ensure thread should retry until storage becomes reachable");
        assertEquals(3, storageClient.ensureAttempts(),
                "bucket ensure should retry after two failures and then succeed");

        bucketThread.interrupt();
        bucketThread.join(5_000);
        assertNull(bucketThreadField.get(null), "bucket ensure thread should clear itself on shutdown");
    }

    private static final class RecoveringEnsureBucketStorageClient implements ObjectStorageClient {
        private final int failuresBeforeSuccess;
        private final AtomicInteger ensureAttempts = new AtomicInteger();
        private final CountDownLatch recovered = new CountDownLatch(1);

        private RecoveringEnsureBucketStorageClient(int failuresBeforeSuccess) {
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream downloadFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean fileExists(String key) {
            return false;
        }

        @Override
        public List<String> listFiles(String prefix) {
            return List.of();
        }

        @Override
        public void ensureBucketExists() {
            int attempt = ensureAttempts.incrementAndGet();
            if (attempt <= failuresBeforeSuccess) {
                throw new RuntimeException("simulated MinIO outage during ensureBucketExists attempt " + attempt);
            }
            recovered.countDown();
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private boolean awaitRecovered(long timeout, TimeUnit unit) throws InterruptedException {
            return recovered.await(timeout, unit);
        }

        private int ensureAttempts() {
            return ensureAttempts.get();
        }
    }
}
