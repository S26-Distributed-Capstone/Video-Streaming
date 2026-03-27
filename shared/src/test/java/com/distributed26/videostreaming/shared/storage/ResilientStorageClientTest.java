package com.distributed26.videostreaming.shared.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class ResilientStorageClientTest {

    @Mock
    private ObjectStorageClient delegate;

    // --- Happy path: operations succeed on first try ---

    @Test
    void fileExists_delegatesOnFirstSuccess() {
        when(delegate.fileExists("key")).thenReturn(true);
        ResilientStorageClient client = new ResilientStorageClient(delegate, 10, 100, 3);

        assertTrue(client.fileExists("key"));
        verify(delegate, times(1)).fileExists("key");
    }

    @Test
    void downloadFile_delegatesOnFirstSuccess() {
        InputStream expected = new ByteArrayInputStream(new byte[0]);
        when(delegate.downloadFile("key")).thenReturn(expected);
        ResilientStorageClient client = new ResilientStorageClient(delegate, 10, 100, 3);

        assertSame(expected, client.downloadFile("key"));
        verify(delegate, times(1)).downloadFile("key");
    }

    @Test
    void uploadFile_delegatesOnFirstSuccess() {
        ResilientStorageClient client = new ResilientStorageClient(delegate, 10, 100, 3);
        InputStream data = new ByteArrayInputStream(new byte[]{1, 2, 3});

        assertDoesNotThrow(() -> client.uploadFile("key", data, 3));
        verify(delegate, times(1)).uploadFile("key", data, 3);
    }

    @Test
    void listFiles_delegatesOnFirstSuccess() {
        when(delegate.listFiles("prefix/")).thenReturn(List.of("a", "b"));
        ResilientStorageClient client = new ResilientStorageClient(delegate, 10, 100, 3);

        assertEquals(List.of("a", "b"), client.listFiles("prefix/"));
        verify(delegate, times(1)).listFiles("prefix/");
    }

    // --- Pass-through behavior (no retry) ---

    @Test
    void fileExists_doesNotRetry_passesThrough() {
        when(delegate.fileExists("key")).thenThrow(new RuntimeException("Connection refused"));

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertThrows(RuntimeException.class, () -> client.fileExists("key"));
        verify(delegate, times(1)).fileExists("key");
    }

    @Test
    void downloadFile_doesNotRetry_passesThrough() {
        when(delegate.downloadFile("key")).thenThrow(new RuntimeException("Connection refused"));

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertThrows(RuntimeException.class, () -> client.downloadFile("key"));
        // Called exactly once — no retry; caller (TranscodingTask) owns retry logic
        verify(delegate, times(1)).downloadFile("key");
    }

    // --- Retry behavior ---

    @Test
    void uploadFile_doesNotRetry_passesThrough() {
        InputStream data = new ByteArrayInputStream(new byte[]{1});
        doThrow(new RuntimeException("Connection refused"))
                .when(delegate).uploadFile("key", data, 1);

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertThrows(RuntimeException.class, () -> client.uploadFile("key", data, 1));
        // Called exactly once — no retry; InputStream is consumed on first attempt
        verify(delegate, times(1)).uploadFile("key", data, 1);
    }

    @Test
    void listFiles_retriesOnFailureThenSucceeds() {
        AtomicInteger callCount = new AtomicInteger();
        when(delegate.listFiles("prefix/")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() < 2) {
                throw new RuntimeException("Connection refused");
            }
            return List.of("a");
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertEquals(List.of("a"), client.listFiles("prefix/"));
        assertEquals(2, callCount.get());
    }

    // --- Exhausted retries ---

    @Test
    void listFiles_throwsAfterMaxAttempts() {
        when(delegate.listFiles("prefix/")).thenThrow(new RuntimeException("Connection refused"));

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 2);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> client.listFiles("prefix/"));
        assertTrue(ex.getMessage().contains("failed after 2 attempt(s)"));
        verify(delegate, times(2)).listFiles("prefix/");
    }

    // --- Non-transient error detection ---

    @Test
    void isNonTransient_trueForNoSuchKey() {
        NoSuchKeyException nsk = (NoSuchKeyException) NoSuchKeyException.builder()
                .statusCode(404).message("key not found").build();
        assertTrue(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", nsk)));
    }

    @Test
    void isNonTransient_trueForNoSuchBucket() {
        NoSuchBucketException nsb = (NoSuchBucketException) NoSuchBucketException.builder()
                .statusCode(404).message("bucket not found").build();
        assertTrue(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", nsb)));
    }

    @Test
    void isNonTransient_trueFor403() {
        S3Exception forbidden = (S3Exception) S3Exception.builder()
                .statusCode(403).message("Access Denied").build();
        assertTrue(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", forbidden)));
    }

    @Test
    void isNonTransient_falseFor408RequestTimeout() {
        S3Exception timeout = (S3Exception) S3Exception.builder()
                .statusCode(408).message("Request Timeout").build();
        assertFalse(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", timeout)));
    }

    @Test
    void isNonTransient_falseFor429TooManyRequests() {
        S3Exception throttle = (S3Exception) S3Exception.builder()
                .statusCode(429).message("Too Many Requests").build();
        assertFalse(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", throttle)));
    }

    @Test
    void isNonTransient_falseFor500() {
        S3Exception serverError = (S3Exception) S3Exception.builder()
                .statusCode(500).message("Internal Server Error").build();
        assertFalse(ResilientStorageClient.isNonTransient(
                new IllegalStateException("wrapped", serverError)));
    }

    @Test
    void isNonTransient_falseForConnectionError() {
        assertFalse(ResilientStorageClient.isNonTransient(
                new RuntimeException("Connection refused")));
    }

    @Test
    void deleteFile_throwsImmediatelyOnNonTransientError() {
        NoSuchBucketException nsb = (NoSuchBucketException) NoSuchBucketException.builder()
                .statusCode(404).message("bucket not found").build();
        doThrow(new IllegalStateException("wrapped", nsb))
                .when(delegate).deleteFile("key");

        // Unlimited retries configured — but should still fail immediately
        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 0);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> client.deleteFile("key"));
        assertTrue(ex.getMessage().contains("non-transient"));
        // Called only once — no retry for non-transient errors
        verify(delegate, times(1)).deleteFile("key");
    }

    // --- Interrupt handling ---

    @Test
    void retry_respectsInterruptFlag() {
        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Connection refused");
        }).when(delegate).deleteFile("key");

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 0);

        assertThrows(RuntimeException.class, () -> client.deleteFile("key"));
        assertTrue(Thread.interrupted(), "Interrupt flag should be preserved");
    }

    // --- Close delegates ---

    @Test
    void close_delegatesToWrappedClient() {
        ResilientStorageClient client = new ResilientStorageClient(delegate, 10, 100, 3);
        client.close();
        verify(delegate).close();
    }

    // --- Unlimited retries (maxAttempts=0) recover eventually ---

    @Test
    void unlimitedRetries_eventuallySucceeds() {
        AtomicInteger callCount = new AtomicInteger();
        when(delegate.listFiles("prefix/")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() < 5) {
                throw new RuntimeException("Connection refused");
            }
            return List.of("a");
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 5, 0);

        assertEquals(List.of("a"), client.listFiles("prefix/"));
        assertEquals(5, callCount.get());
    }

    // --- ensureBucketExists retries ---

    @Test
    void ensureBucketExists_retriesOnFailure() {
        AtomicInteger callCount = new AtomicInteger();
        doAnswer(invocation -> {
            if (callCount.incrementAndGet() < 3) {
                throw new RuntimeException("Connection refused");
            }
            return null;
        }).when(delegate).ensureBucketExists();

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertDoesNotThrow(client::ensureBucketExists);
        assertEquals(3, callCount.get());
    }
}

