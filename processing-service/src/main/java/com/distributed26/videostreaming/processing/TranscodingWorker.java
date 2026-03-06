package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.processing.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.shared.jobs.Status;
import com.distributed26.videostreaming.shared.jobs.Worker;
import com.distributed26.videostreaming.shared.jobs.WorkerStatus;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.RabbitMQJobTaskBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
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
    private final RabbitMQJobTaskBus bus;
    private final TranscodedSegmentStatusRepository transcodeStatusRepository;
    private volatile boolean running = false;
    private Thread thread;

    public TranscodingWorker(String id, ObjectStorageClient storageClient,
                             BlockingQueue<TranscodingTask> queue,
                             RabbitMQJobTaskBus bus,
                             TranscodedSegmentStatusRepository transcodeStatusRepository) {
        this.worker = new Worker(id, Instant.now());
        this.storageClient = storageClient;
        this.queue = queue;
        this.bus = bus;
        this.transcodeStatusRepository = transcodeStatusRepository;
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
                emitState(task, TranscodeSegmentState.TRANSCODING);

                try {
                    task.execute(storageClient, () -> emitState(task, TranscodeSegmentState.UPLOADING));
                    task.setStatus(Status.SUCCEEDED);
                    emitState(task, TranscodeSegmentState.DONE);
                    LOGGER.info("Task {} succeeded", task.getId());
                } catch (Exception e) {
                    task.setStatus(Status.FAILED);
                    emitState(task, TranscodeSegmentState.FAILED);
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

    private void emitState(TranscodingTask task, TranscodeSegmentState state) {
        if (bus == null || transcodeStatusRepository == null) {
            return;
        }
        int segmentNumber = extractSegmentNumber(task.getChunkKey());
        if (segmentNumber < 0) {
            return;
        }
        ProcessingServiceApplication.publishTranscodeState(
                task.getJobId(),
                task.getProfile().getName(),
                segmentNumber,
                state
        );
    }

    private static int extractSegmentNumber(String chunkKey) {
        if (chunkKey == null || chunkKey.isBlank()) {
            return -1;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(\\d+)").matcher(chunkKey);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last;
    }
}
