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

    // --- Retry behavior ---

    @Test
    void fileExists_retriesOnFailureThenSucceeds() {
        AtomicInteger callCount = new AtomicInteger();
        when(delegate.fileExists("key")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() < 3) {
                throw new RuntimeException("Connection refused");
            }
            return true;
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertTrue(client.fileExists("key"));
        assertEquals(3, callCount.get());
    }

    @Test
    void downloadFile_retriesOnFailureThenSucceeds() {
        InputStream expected = new ByteArrayInputStream(new byte[]{42});
        AtomicInteger callCount = new AtomicInteger();
        when(delegate.downloadFile("key")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() < 2) {
                throw new RuntimeException("Connection refused");
            }
            return expected;
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertSame(expected, client.downloadFile("key"));
        assertEquals(2, callCount.get());
    }

    @Test
    void uploadFile_retriesOnFailureThenSucceeds() {
        AtomicInteger callCount = new AtomicInteger();
        InputStream data = new ByteArrayInputStream(new byte[]{1});
        doAnswer(invocation -> {
            if (callCount.incrementAndGet() < 2) {
                throw new RuntimeException("Connection refused");
            }
            return null;
        }).when(delegate).uploadFile("key", data, 1);

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 5);

        assertDoesNotThrow(() -> client.uploadFile("key", data, 1));
        assertEquals(2, callCount.get());
    }

    // --- Exhausted retries ---

    @Test
    void fileExists_throwsAfterMaxAttempts() {
        when(delegate.fileExists("key")).thenThrow(new RuntimeException("Connection refused"));

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 3);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> client.fileExists("key"));
        assertTrue(ex.getMessage().contains("failed after 3 attempt(s)"));
        verify(delegate, times(3)).fileExists("key");
    }

    @Test
    void downloadFile_throwsAfterMaxAttempts() {
        when(delegate.downloadFile("key")).thenThrow(new RuntimeException("Connection refused"));

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 2);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> client.downloadFile("key"));
        assertTrue(ex.getMessage().contains("failed after 2 attempt(s)"));
        verify(delegate, times(2)).downloadFile("key");
    }

    // --- Interrupt handling ---

    @Test
    void retry_respectsInterruptFlag() {
        when(delegate.fileExists("key")).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Connection refused");
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 10, 0);

        assertThrows(RuntimeException.class, () -> client.fileExists("key"));
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
        when(delegate.fileExists("key")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() < 5) {
                throw new RuntimeException("Connection refused");
            }
            return false;
        });

        ResilientStorageClient client = new ResilientStorageClient(delegate, 1, 5, 0);

        assertFalse(client.fileExists("key"));
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

