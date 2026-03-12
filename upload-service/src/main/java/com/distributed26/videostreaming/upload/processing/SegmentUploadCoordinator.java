package com.distributed26.videostreaming.upload.processing;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class SegmentUploadCoordinator {
    private static final Logger logger = LogManager.getLogger(SegmentUploadCoordinator.class);
    private static final Pattern EXTINF_PATTERN = Pattern.compile("^#EXTINF:([^,]+),?");
    private static final Pattern SEGMENT_NUMBER_PATTERN = Pattern.compile("(\\d+)");
    private static final String[] TRANSCODE_PROFILES = {"low", "medium", "high"};

    private final ObjectStorageClient storageClient;
    private final StatusEventBus statusEventBus;
    private final TranscodeTaskBus transcodeTaskBus;
    private final SegmentUploadRepository segmentUploadRepository;
    private final FailedVideoRegistry failedVideoRegistry;
    private final ExecutorService segmentUploadExecutor;
    private final int maxInFlightSegmentUploads;
    private final int segmentDuration;

    public SegmentUploadCoordinator(
            ObjectStorageClient storageClient,
            StatusEventBus statusEventBus,
            TranscodeTaskBus transcodeTaskBus,
            SegmentUploadRepository segmentUploadRepository,
            FailedVideoRegistry failedVideoRegistry,
            ExecutorService segmentUploadExecutor,
            int maxInFlightSegmentUploads,
            int segmentDuration
    ) {
        this.storageClient = storageClient;
        this.statusEventBus = statusEventBus;
        this.transcodeTaskBus = transcodeTaskBus;
        this.segmentUploadRepository = segmentUploadRepository;
        this.failedVideoRegistry = failedVideoRegistry;
        this.segmentUploadExecutor = segmentUploadExecutor;
        this.maxInFlightSegmentUploads = maxInFlightSegmentUploads;
        this.segmentDuration = segmentDuration;
    }

    public int uploadReadySegments(
            Path tempOutput,
            String videoId,
            Set<Path> uploadedFiles,
            Set<Integer> uploadedSegmentNumbers,
            boolean isFinalSweep
    ) {
        return uploadReadySegments(
                tempOutput,
                videoId,
                uploadedFiles,
                uploadedSegmentNumbers,
                new ConcurrentHashMap<>(),
                isFinalSweep
        );
    }

    public void preloadUploadedSegmentNumbers(String videoId, Set<Integer> uploadedSegmentNumbers) {
        if (segmentUploadRepository == null) {
            return;
        }
        try {
            uploadedSegmentNumbers.addAll(segmentUploadRepository.findSegmentNumbers(videoId));
        } catch (RuntimeException e) {
            logger.warn("Failed to preload uploaded segment numbers for videoId={}", videoId, e);
        }
    }

    public void uploadSegment(Path path, String videoId, double outputTsOffsetSeconds) {
        ensureVideoActive(videoId);
        try {
            String fileName = path.getFileName().toString();
            String objectKey = videoId + "/chunks/" + fileName;
            long size = Files.size(path);
            logger.info("Uploading segment: {} ({} bytes)", objectKey, size);

            try (InputStream is = new FileInputStream(path.toFile())) {
                storageClient.uploadFile(objectKey, is, size);
                if (fileName.endsWith(".ts")) {
                    OptionalInt segmentNumber = extractSegmentNumber(fileName);
                    publishTranscodeTasks(videoId, objectKey, segmentNumber, outputTsOffsetSeconds);
                    statusEventBus.publish(new JobEvent(videoId, objectKey));
                    recordUploadedSegment(videoId, fileName, segmentNumber);
                }
                logger.info("Finished uploading segment: {}", objectKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload segment: " + path, e);
        }
    }

    int uploadReadySegments(
            Path tempOutput,
            String videoId,
            Set<Path> uploadedFiles,
            Set<Integer> uploadedSegmentNumbers,
            Map<Path, PendingUpload> inFlightUploads,
            boolean isFinalSweep
    ) {
        ensureVideoActive(videoId);
        List<Path> files;
        try (Stream<Path> stream = Files.list(tempOutput)) {
            files = stream.toList();
        } catch (IOException e) {
            logger.error("Failed to list segments in directory: {}", tempOutput, e);
            return 0;
        }

        int uploadedCount = collectCompletedUploads(inFlightUploads, uploadedFiles, uploadedSegmentNumbers, false);
        Map<String, SegmentTiming> timingsByFileName = readSegmentTimings(tempOutput.resolve("output.m3u8"));

        List<Path> tsFiles = files.stream()
                .filter(p -> p.getFileName().toString().endsWith(".ts"))
                .sorted(java.util.Comparator.comparingLong(p -> p.toFile().lastModified()))
                .toList();

        int tsLimit = isFinalSweep ? tsFiles.size() : Math.max(0, tsFiles.size() - 1);

        for (int i = 0; i < tsLimit; i++) {
            ensureVideoActive(videoId);
            Path path = tsFiles.get(i);
            if (uploadedFiles.contains(path) || inFlightUploads.containsKey(path)) {
                continue;
            }
            OptionalInt segmentNumber = extractSegmentNumber(path.getFileName().toString());
            if (segmentNumber.isPresent() && uploadedSegmentNumbers.contains(segmentNumber.getAsInt())) {
                logger.info("Skipping already uploaded segment {} for videoId={}", segmentNumber.getAsInt(), videoId);
                uploadedFiles.add(path);
                continue;
            }
            SegmentTiming timing = timingsByFileName.get(path.getFileName().toString());
            if (timing == null && !isFinalSweep) {
                continue;
            }
            double outputTsOffsetSeconds = timing != null
                    ? timing.startOffsetSeconds()
                    : fallbackOffsetForSegment(segmentNumber);
            waitForUploadCapacity(videoId, inFlightUploads, uploadedFiles, uploadedSegmentNumbers);
            inFlightUploads.put(
                    path,
                    new PendingUpload(
                            segmentNumber,
                            CompletableFuture.runAsync(
                                    () -> uploadSegment(path, videoId, outputTsOffsetSeconds),
                                    segmentUploadExecutor
                            )
                    )
            );
            logger.info("Queued segment upload: {}", path.getFileName());
            uploadedCount++;
        }

        uploadedCount += collectCompletedUploads(inFlightUploads, uploadedFiles, uploadedSegmentNumbers, isFinalSweep);

        if (isFinalSweep) {
            files.stream()
                    .filter(p -> p.getFileName().toString().endsWith(".m3u8"))
                    .forEach(path -> {
                        ensureVideoActive(videoId);
                        if (!uploadedFiles.contains(path)) {
                            uploadSegment(path, videoId, 0d);
                            uploadedFiles.add(path);
                        }
                    });
        }

        return uploadedCount;
    }

    void waitForOrCancelInFlightUploads(Map<Path, PendingUpload> inFlightUploads, boolean cancelPending) {
        for (var entry : List.copyOf(inFlightUploads.entrySet())) {
            Path path = entry.getKey();
            CompletableFuture<Void> future = entry.getValue().future();
            try {
                if (cancelPending && !future.isDone()) {
                    future.cancel(true);
                }
                future.join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                logger.warn("In-flight segment upload finished with error during cleanup: {}", path, cause);
            } catch (java.util.concurrent.CancellationException e) {
                logger.info("Cancelled in-flight segment upload during cleanup: {}", path);
            } catch (RuntimeException e) {
                future.cancel(true);
                logger.warn("Failed while waiting for in-flight upload during cleanup: {}", path, e);
            } finally {
                inFlightUploads.remove(path);
            }
        }
    }

    record PendingUpload(OptionalInt segmentNumber, CompletableFuture<Void> future) {
    }

    private void waitForUploadCapacity(
            String videoId,
            Map<Path, PendingUpload> inFlightUploads,
            Set<Path> uploadedFiles,
            Set<Integer> uploadedSegmentNumbers
    ) {
        while (inFlightUploads.size() >= Math.max(1, maxInFlightSegmentUploads)) {
            ensureVideoActive(videoId);
            int drained = collectCompletedUploads(inFlightUploads, uploadedFiles, uploadedSegmentNumbers, false);
            if (drained > 0) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for upload capacity", e);
            }
        }
    }

    private int collectCompletedUploads(
            Map<Path, PendingUpload> inFlightUploads,
            Set<Path> uploadedFiles,
            Set<Integer> uploadedSegmentNumbers,
            boolean waitForAll
    ) {
        int completedCount = 0;
        boolean keepDraining = true;

        while (keepDraining) {
            keepDraining = false;
            for (var entry : List.copyOf(inFlightUploads.entrySet())) {
                Path path = entry.getKey();
                PendingUpload pending = entry.getValue();
                CompletableFuture<Void> future = pending.future();
                if (!waitForAll && !future.isDone()) {
                    continue;
                }

                try {
                    future.join();
                    uploadedFiles.add(path);
                    if (pending.segmentNumber().isPresent()) {
                        uploadedSegmentNumbers.add(pending.segmentNumber().getAsInt());
                    }
                    completedCount++;
                } catch (CompletionException e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    logger.error("Failed to upload segment asynchronously: {}", path, cause);
                    throw new RuntimeException("Failed to upload segment: " + path, cause);
                } finally {
                    inFlightUploads.remove(path);
                }
                keepDraining = waitForAll;
            }
        }

        return completedCount;
    }

    private void recordUploadedSegment(String videoId, String fileName, OptionalInt segmentNumber) {
        if (segmentUploadRepository == null) {
            logger.warn("SegmentUploadRepository is null; skipping segment_upload insert");
            return;
        }
        if (segmentNumber.isEmpty()) {
            logger.warn("Could not parse segment number from {}", fileName);
            return;
        }
        segmentUploadRepository.insert(videoId, segmentNumber.getAsInt());
        logger.info("Recorded segment_upload videoId={} segmentNumber={}", videoId, segmentNumber.getAsInt());
    }

    private void publishTranscodeTasks(
            String videoId,
            String objectKey,
            OptionalInt segmentNumber,
            double outputTsOffsetSeconds
    ) {
        if (segmentNumber.isEmpty()) {
            logger.warn("Skipping transcode task publish because segment number could not be parsed for {}", objectKey);
            return;
        }
        for (String profile : TRANSCODE_PROFILES) {
            transcodeTaskBus.publish(
                    new TranscodeTaskEvent(videoId, objectKey, profile, segmentNumber.getAsInt(), outputTsOffsetSeconds)
            );
        }
    }

    private OptionalInt extractSegmentNumber(String fileName) {
        Matcher matcher = SEGMENT_NUMBER_PATTERN.matcher(fileName);
        int last = -1;
        while (matcher.find()) {
            last = Integer.parseInt(matcher.group(1));
        }
        return last >= 0 ? OptionalInt.of(last) : OptionalInt.empty();
    }

    private Map<String, SegmentTiming> readSegmentTimings(Path playlistPath) {
        Map<String, SegmentTiming> timingsByFileName = new HashMap<>();
        if (!Files.exists(playlistPath)) {
            return timingsByFileName;
        }

        try {
            String content = Files.readString(playlistPath, StandardCharsets.UTF_8);
            double pendingDuration = 0d;
            double runningOffset = 0d;
            for (String line : content.split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.startsWith("#")) {
                    if (trimmed.startsWith("#EXTINF:")) {
                        pendingDuration = parseExtinfDuration(trimmed);
                    }
                    continue;
                }
                if (!trimmed.endsWith(".ts")) {
                    continue;
                }
                timingsByFileName.put(trimmed, new SegmentTiming(runningOffset, pendingDuration));
                runningOffset += pendingDuration;
            }
        } catch (IOException e) {
            logger.warn("Failed to read local HLS playlist for segment timings: {}", playlistPath, e);
        }
        return timingsByFileName;
    }

    private double parseExtinfDuration(String line) {
        Matcher matcher = EXTINF_PATTERN.matcher(line);
        if (!matcher.find()) {
            return 0d;
        }
        try {
            return Double.parseDouble(matcher.group(1).trim());
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse EXTINF duration '{}'", line);
            return 0d;
        }
    }

    private double fallbackOffsetForSegment(OptionalInt segmentNumber) {
        return segmentNumber.isPresent()
                ? (double) segmentNumber.getAsInt() * Math.max(1, segmentDuration)
                : 0d;
    }

    private void ensureVideoActive(String videoId) {
        if (failedVideoRegistry != null && failedVideoRegistry.isFailed(videoId)) {
            throw new RuntimeException("Upload already marked FAILED for videoId=" + videoId);
        }
    }

    private record SegmentTiming(double startOffsetSeconds, double durationSeconds) {
    }
}
