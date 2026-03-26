package com.distributed26.videostreaming.upload.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StorageRetryExecutorTest {

    @Test
    void retriesUntilOperationSucceeds() {
        StorageRetryExecutor executor = new StorageRetryExecutor(1L, 2L);
        AtomicInteger attempts = new AtomicInteger(0);

        executor.run("test", () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("temporary");
            }
        });

        assertEquals(3, attempts.get());
    }

    @Test
    void doesNotRetryCancellation() {
        StorageRetryExecutor executor = new StorageRetryExecutor(1L, 2L);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(CancellationException.class, () -> executor.run("test", () -> {
            attempts.incrementAndGet();
            throw new CancellationException("cancelled");
        }));

        assertEquals(1, attempts.get());
    }
}
