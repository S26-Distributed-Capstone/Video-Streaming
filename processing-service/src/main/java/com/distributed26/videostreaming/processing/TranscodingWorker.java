package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.jobs.WorkerStatus;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Polls a shared {@link BlockingQueue} for {@link TranscodingTask}s and executes them.
 */
public class TranscodingWorker {
    private static final Logger LOGGER = LogManager.getLogger(TranscodingWorker.class);

    private final Worker worker;
    private final ObjectStorageClient storageClient;
    private final BlockingQueue<TranscodingTask> queue;
    private volatile boolean running = false;
    private Thread thread;

    public TranscodingWorker(String id, ObjectStorageClient storageClient,
                             BlockingQueue<TranscodingTask> queue) {
        this.worker = new Worker(id, Instant.now());
        this.storageClient = storageClient;
        this.queue = queue;
    }

    public String getId() { return worker.getId(); }
    public WorkerStatus getStatus() { return worker.getStatus(); }

    public void start() {
        running = true;
        thread = new Thread(this::runLoop, "TranscodingWorker-" + worker.getId());
        thread.setDaemon(true);
        thread.start();
        LOGGER.info("Worker started: {}", worker.getId());
    }

    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
        worker.setStatus(WorkerStatus.OFFLINE);
        LOGGER.info("Worker stopped: {}", worker.getId());
    }

    private void runLoop() {
        while (running) {
            try {
                TranscodingTask task = queue.poll(1, TimeUnit.SECONDS);
                if (task == null) {
                    worker.heartbeat();
                    continue;
                }

                worker.setStatus(WorkerStatus.BUSY);
                task.setStatus(Status.RUNNING);
                LOGGER.info("Worker {} picked up task {} (chunk={} profile={})",
                        worker.getId(), task.getId(), task.getChunkKey(), task.getProfile().getName());

                try {
                    task.execute(storageClient);
                    task.setStatus(Status.SUCCEEDED);
                    LOGGER.info("Task {} succeeded", task.getId());
                } catch (Exception e) {
                    task.setStatus(Status.FAILED);
                    LOGGER.error("Task {} failed: {}", task.getId(), e.getMessage(), e);
                }

                worker.setStatus(WorkerStatus.IDLE);
                worker.heartbeat();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
