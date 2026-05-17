package com.distributed26.videostreaming.processing.runtime;

import com.distributed26.videostreaming.processing.db.ProcessingUploadTaskRepository.UploadTaskMetadata;
import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StartupRecoveryService {
    private static final Logger LOGGER = LogManager.getLogger(StartupRecoveryService.class);
    private static final java.util.regex.Pattern EXTINF_PATTERN = java.util.regex.Pattern.compile("^#EXTINF:([^,]+),?");
    private static final long DEFAULT_QUEUED_RECOVERY_STALE_MILLIS = 300_000L;
    private static final long ACTIVE_PROGRESS_QUEUED_RECOVERY_STALE_MILLIS = 180_000L;
    private static final long MAX_BACKLOG_QUEUED_RECOVERY_STALE_MILLIS = 600_000L;
    private static final int BACKLOG_QUEUED_SEGMENT_THRESHOLD = 24;
    private static final long BACKLOG_QUEUED_RECOVERY_EXTENSION_PER_SEGMENT_MILLIS = 500L;

    private final TranscodingProfile[] profiles;
    private final ProcessingRuntime runtime;
    private final boolean reconcileCompletedVideos;
    private final long queuedRecoveryStaleMillis;

    public StartupRecoveryService(TranscodingProfile[] profiles, ProcessingRuntime runtime) {
        this(profiles, runtime, false, DEFAULT_QUEUED_RECOVERY_STALE_MILLIS);
    }

    public StartupRecoveryService(
            TranscodingProfile[] profiles,
            ProcessingRuntime runtime,
            boolean reconcileCompletedVideos
    ) {
        this(profiles, runtime, reconcileCompletedVideos, DEFAULT_QUEUED_RECOVERY_STALE_MILLIS);
    }

    public StartupRecoveryService(
            TranscodingProfile[] profiles,
            ProcessingRuntime runtime,
            boolean reconcileCompletedVideos,
            long queuedRecoveryStaleMillis
    ) {
        this.profiles = profiles;
        this.runtime = runtime;
        this.reconcileCompletedVideos = reconcileCompletedVideos;
        this.queuedRecoveryStaleMillis = Math.max(1_000L, queuedRecoveryStaleMillis);
    }

    /**
     * Scans the local spool directory for transcoded segment files that were never
     * registered as upload tasks in the database. This covers the crash window
     * between {@code transcodeToSpool()} completing and {@code upsertPending()} being called.
     *
     * <p>For each orphaned spool file found:
     * <ul>
     *   <li>If the segment is already in object storage → delete the spool file and mark DONE</li>
     *   <li>If an upload task already exists → skip (upload workers will handle it)</li>
     *   <li>Otherwise → create a PENDING upload task so the upload workers pick it up</li>
     * </ul>
     *
     * <p>Also cleans up stale {@code .part} files from incomplete writes.
     *
     * @param storageClient object storage client for checking existing uploads
     * @param spoolRoot     root directory of the local spool (e.g. {@code /app/processing-spool})
     */
    public void recoverOrphanedSpoolFiles(ObjectStorageClient storageClient, Path spoolRoot) {
        if (spoolRoot == null || !Files.isDirectory(spoolRoot)) {
            LOGGER.info("Spool recovery skipped: spool root is not available (path={})", spoolRoot);
            return;
        }
        if (runtime.processingUploadTaskRepository() == null) {
            LOGGER.info("Spool recovery skipped: upload task repository is not configured");
            return;
        }

        int recovered = 0;
        int reassignedExisting = 0;
        int skippedDone = 0;
        int cleanedPartFiles = 0;

        try (Stream<Path> videoIdDirs = Files.list(spoolRoot)) {
            for (Path videoIdDir : videoIdDirs.toList()) {
                if (!Files.isDirectory(videoIdDir)) {
                    continue;
                }
                String videoId = videoIdDir.getFileName().toString();

                // Validate UUID format
                try {
                    UUID.fromString(videoId);
                } catch (IllegalArgumentException e) {
                    LOGGER.debug("Spool recovery: skipping non-UUID directory {}", videoIdDir);
                    continue;
                }

                if (runtime.isVideoFailed(videoId)) {
                    LOGGER.info("Spool recovery: cleaning up spool for failed videoId={}", videoId);
                    deleteDirectoryRecursive(videoIdDir);
                    continue;
                }

                try (Stream<Path> profileDirs = Files.list(videoIdDir)) {
                    for (Path profileDir : profileDirs.toList()) {
                        if (!Files.isDirectory(profileDir)) {
                            continue;
                        }
                        String profileName = profileDir.getFileName().toString();
                        TranscodingProfile profile = findProfile(profileName);
                        if (profile == null) {
                            LOGGER.warn("Spool recovery: unknown profile directory {}", profileDir);
                            continue;
                        }

                        try (Stream<Path> files = Files.list(profileDir)) {
                            for (Path file : files.toList()) {
                                // Clean up incomplete .part files from interrupted writes
                                if (file.toString().endsWith(".part")) {
                                    try {
                                        Files.deleteIfExists(file);
                                        cleanedPartFiles++;
                                    } catch (IOException e) {
                                        LOGGER.warn("Spool recovery: failed to delete .part file {}", file, e);
                                    }
                                    continue;
                                }

                                if (!Files.isRegularFile(file) || !file.toString().endsWith(".ts")) {
                                    continue;
                                }

                                String fileName = file.getFileName().toString();
                                int segmentNumber = ProcessingRuntime.parseSegmentNumber(fileName);
                                if (segmentNumber < 0) {
                                    LOGGER.warn("Spool recovery: cannot parse segment number from {}", file);
                                    continue;
                                }

                                // If already in object storage, clean up and mark DONE
                                String outputKey = videoId + "/processed/" + profileName + "/" + fileName;
                                if (safeFileExists(storageClient, outputKey)) {
                                    try {
                                        Files.deleteIfExists(file);
                                    } catch (IOException e) {
                                        LOGGER.warn("Spool recovery: failed to delete already-uploaded spool file {}", file, e);
                                    }
                                    runtime.publishTranscodeState(videoId, profileName, segmentNumber,
                                            TranscodeSegmentState.DONE, profiles);
                                    skippedDone++;
                                    continue;
                                }

                                UploadTaskMetadata existingTask = runtime.processingUploadTaskRepository()
                                        .findTask(videoId, profileName, segmentNumber)
                                        .orElse(null);
                                if (existingTask != null) {
                                    if (hasActiveProcessingClaim(videoId, profileName, segmentNumber)) {
                                        LOGGER.debug("Spool recovery: upload task still has an active processing claim videoId={} profile={} segment={} state={} owner={} claimedBy={}",
                                                videoId, profileName, segmentNumber, existingTask.state(),
                                                existingTask.spoolOwner(), existingTask.claimedBy());
                                        continue;
                                    }
                                    if (isRegisteredForCurrentSpoolFile(existingTask, file)) {
                                        LOGGER.debug("Spool recovery: upload task already registered for local spool file videoId={} profile={} segment={} path={} state={}",
                                                videoId, profileName, segmentNumber, file, existingTask.state());
                                        continue;
                                    }
                                }

                                // Create or reassign a PENDING upload task for this visible spool file.
                                String chunkKey = videoId + "/chunks/" + fileName;
                                long sizeBytes = Files.size(file);
                                double offsetSeconds = ProcessingRuntime.fallbackOffsetForSegment(segmentNumber);

                                runtime.processingUploadTaskRepository().upsertPending(
                                        videoId,
                                        runtime.processorInstanceId(),
                                        profileName,
                                        segmentNumber,
                                        chunkKey,
                                        outputKey,
                                        file.toAbsolutePath().toString(),
                                        sizeBytes,
                                        offsetSeconds
                                );
                                if (existingTask != null) {
                                    reassignedExisting++;
                                    LOGGER.info("Spool recovery: reassigned local upload task ownership videoId={} profile={} segment={} path={} owner={}",
                                            videoId, profileName, segmentNumber, file, runtime.processorInstanceId());
                                } else {
                                    recovered++;
                                    LOGGER.info("Spool recovery: created upload task for orphaned file videoId={} profile={} segment={} path={}",
                                            videoId, profileName, segmentNumber, file);
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Spool recovery: error scanning spool directory", e);
        }

        if (recovered > 0 || reassignedExisting > 0 || skippedDone > 0 || cleanedPartFiles > 0) {
            LOGGER.info("Spool recovery complete: recovered={} reassigned={} alreadyUploaded={} partFilesCleaned={}",
                    recovered, reassignedExisting, skippedDone, cleanedPartFiles);
        } else {
            LOGGER.info("Spool recovery complete: no orphaned files found");
        }
    }

    public void recoverIncompleteVideos(ObjectStorageClient storageClient) {
        if (runtime.videoProcessingRepository() == null) {
            LOGGER.info("Startup recovery skipped because video metadata repository is not configured");
            return;
        }
        List<String> videoIds;
        try {
            Set<String> recoverableVideoIds = new HashSet<>(
                    runtime.videoProcessingRepository().findVideoIdsByStatus("PROCESSING")
            );
            recoverableVideoIds.addAll(runtime.videoProcessingRepository().findVideoIdsByStatus("UPLOADED"));
            if (reconcileCompletedVideos) {
                recoverableVideoIds.addAll(runtime.videoProcessingRepository().findVideoIdsByStatus("COMPLETED"));
            }
            if (runtime.processingUploadTaskRepository() != null) {
                addRecoverableVideoIds(recoverableVideoIds, runtime.processingUploadTaskRepository().findVideoIdsWithOpenTasks());
            }
            videoIds = new ArrayList<>(recoverableVideoIds);
        } catch (Exception e) {
            LOGGER.warn("Startup recovery failed to load recoverable videos", e);
            return;
        }
        if (videoIds.isEmpty()) {
            LOGGER.info("Startup recovery found no recoverable videos");
            return;
        }
        LOGGER.info("Startup recovery inspecting {} recoverable video(s)", videoIds.size());
        for (String videoId : videoIds) {
            recoverVideo(videoId, storageClient);
        }
    }

    private void addRecoverableVideoIds(Set<String> recoverableVideoIds, List<String> videoIds) {
        if (videoIds == null || videoIds.isEmpty()) {
            return;
        }
        recoverableVideoIds.addAll(videoIds);
    }

    private void recoverVideo(String videoId, ObjectStorageClient storageClient) {
        try {
            if (runtime.isVideoFailed(videoId)) {
                LOGGER.info("Startup recovery skipping failed videoId={}", videoId);
                return;
            }
            String currentStatus = runtime.findVideoStatus(videoId).orElse(null);
            if ("COMPLETED".equalsIgnoreCase(currentStatus)) {
                if (runtime.hasRequiredManifests(videoId)) {
                    LOGGER.debug("Startup recovery found video already reconciled videoId={}", videoId);
                    return;
                }
            }
            List<String> chunkKeys = listSourceChunkKeys(videoId, storageClient);
            if (chunkKeys.isEmpty()) {
                LOGGER.info("Startup recovery: no source chunks found for videoId={}", videoId);
                return;
            }

            int totalSegments = Math.max(runtime.findTotalSegments(videoId), chunkKeys.size());
            Map<String, Set<Integer>> doneSegmentsByProfile = loadDoneSegmentsByProfile(videoId);
            Map<String, Set<Integer>> queuedSegmentsByProfile = loadQueuedSegmentsByProfile(videoId);
            Map<String, Set<Integer>> inFlightSegmentsByProfile = loadInFlightSegmentsByProfile(videoId, queuedSegmentsByProfile);
            Map<String, Set<Integer>> recoverableQueuedSegmentsByProfile =
                    loadRecoverableQueuedSegmentsByProfile(videoId, queuedSegmentsByProfile, inFlightSegmentsByProfile);
            int republished = 0;
            Set<String> touchedProfiles = new HashSet<>();
            int deferredQueuedSegments = 0;

            Map<Integer, Double> offsetsBySegment = loadSourceSegmentOffsets(videoId, storageClient);
            for (String chunkKey : chunkKeys) {
                int segmentNumber = ProcessingRuntime.parseSegmentNumber(chunkKey);
                if (segmentNumber < 0) {
                    LOGGER.warn("Startup recovery: skipping chunk with unparseable segment number videoId={} key={}",
                            videoId, chunkKey);
                    continue;
                }
                for (TranscodingProfile profile : profiles) {
                    String profileName = profile.getName();
                    Set<Integer> doneSegments = doneSegmentsByProfile.getOrDefault(profile.getName(), Set.of());
                    Set<Integer> inFlightSegments = inFlightSegmentsByProfile.getOrDefault(profile.getName(), Set.of());
                    Set<Integer> queuedSegments = queuedSegmentsByProfile.getOrDefault(profileName, Set.of());
                    Set<Integer> recoverableQueuedSegments = recoverableQueuedSegmentsByProfile.getOrDefault(profileName, Set.of());
                    if (doneSegments.contains(segmentNumber) || inFlightSegments.contains(segmentNumber)) {
                        continue;
                    }
                    if (queuedSegments.contains(segmentNumber) && !recoverableQueuedSegments.contains(segmentNumber)) {
                        deferredQueuedSegments += 1;
                        continue;
                    }
                    runtime.publishTranscodeState(videoId, profile.getName(), segmentNumber,
                            TranscodeSegmentState.QUEUED, profiles);
                    runtime.transcodeTaskBusRef().publish(new TranscodeTaskEvent(
                            videoId,
                            chunkKey,
                            profile.getName(),
                            segmentNumber,
                            offsetsBySegment.getOrDefault(segmentNumber, ProcessingRuntime.fallbackOffsetForSegment(segmentNumber))
                    ));
                    touchedProfiles.add(profile.getName());
                    republished += 1;
                }
            }

            if (republished > 0) {
                LOGGER.info("Startup recovery requeued {} missing transcode task(s) for videoId={} profiles={}",
                        republished, videoId, touchedProfiles);
            } else if (deferredQueuedSegments > 0) {
                LOGGER.info("Startup recovery deferred {} stale queued transcode task(s) for videoId={} because active progress or backlog suggests the queue is still healthy",
                        deferredQueuedSegments, videoId);
            } else {
                LOGGER.info("Startup recovery found no missing transcode tasks for videoId={}", videoId);
            }

            if (totalSegments > 0 && runtime.areAllProfilesDone(videoId, totalSegments, profiles)) {
                runtime.scheduleManifestGeneration(videoId, totalSegments);
            }
        } catch (Exception e) {
            LOGGER.warn("Startup recovery failed for videoId={}", videoId, e);
        }
    }

    private List<String> listSourceChunkKeys(String videoId, ObjectStorageClient storageClient) {
        String prefix = videoId + "/chunks/";
        List<String> chunkKeys = new ArrayList<>(storageClient.listFiles(prefix).stream()
                .filter(key -> key.endsWith(".ts"))
                .toList());
        chunkKeys.sort(Comparator.comparingInt(ProcessingRuntime::parseSegmentNumber));
        return chunkKeys;
    }

    private Map<String, Set<Integer>> loadDoneSegmentsByProfile(String videoId) {
        Map<String, Set<Integer>> doneSegmentsByProfile = new HashMap<>();
        if (runtime.transcodeStatusRepository() == null) {
            return doneSegmentsByProfile;
        }
        for (TranscodingProfile profile : profiles) {
            try {
                doneSegmentsByProfile.put(
                        profile.getName(),
                        runtime.transcodeStatusRepository()
                                .findSegmentNumbersByState(videoId, profile.getName(), TranscodeSegmentState.DONE)
                );
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load DONE segments videoId={} profile={}",
                        videoId, profile.getName(), e);
            }
        }
        return doneSegmentsByProfile;
    }

    private Map<String, Set<Integer>> loadQueuedSegmentsByProfile(String videoId) {
        Map<String, Set<Integer>> queuedSegmentsByProfile = new HashMap<>();
        if (runtime.transcodeStatusRepository() == null) {
            return queuedSegmentsByProfile;
        }
        for (TranscodingProfile profile : profiles) {
            try {
                queuedSegmentsByProfile.put(
                        profile.getName(),
                        runtime.transcodeStatusRepository()
                                .findSegmentNumbersByState(videoId, profile.getName(), TranscodeSegmentState.QUEUED)
                );
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load queued segments videoId={} profile={}",
                        videoId, profile.getName(), e);
            }
        }
        return queuedSegmentsByProfile;
    }

    private Map<String, Set<Integer>> loadInFlightSegmentsByProfile(
            String videoId,
            Map<String, Set<Integer>> queuedSegmentsByProfile
    ) {
        Map<String, Set<Integer>> inFlightSegmentsByProfile = new HashMap<>();
        if (runtime.processingUploadTaskRepository() == null
                && runtime.processingTaskClaimRepository() == null
                && runtime.transcodeStatusRepository() == null) {
            return inFlightSegmentsByProfile;
        }
        for (TranscodingProfile profile : profiles) {
            Set<Integer> inFlightSegments = new HashSet<>();
            try {
                if (runtime.transcodeStatusRepository() != null) {
                    // Treat QUEUED as in-flight only while it is fresh enough for the current
                    // workload level. When a profile is backlogged but still making progress,
                    // we intentionally widen the freshness window to avoid duplicate requeues.
                    inFlightSegments.addAll(runtime.transcodeStatusRepository()
                            .findSegmentNumbersByStateUpdatedSince(
                                    videoId,
                                    profile.getName(),
                                    TranscodeSegmentState.QUEUED,
                                    computeQueuedRecoveryStaleMillis(
                                            queuedSegmentsByProfile.getOrDefault(profile.getName(), Set.of()).size(),
                                            hasRecentNonQueuedProgress(videoId, profile.getName())
                                    )
                            ));
                    inFlightSegments.addAll(runtime.transcodeStatusRepository()
                            .findSegmentNumbersByStateUpdatedSince(
                                    videoId,
                                    profile.getName(),
                                    TranscodeSegmentState.TRANSCODING,
                                    runtime.claimStaleMillis()
                            ));
                    inFlightSegments.addAll(runtime.transcodeStatusRepository()
                            .findSegmentNumbersByStateUpdatedSince(
                                    videoId,
                                    profile.getName(),
                                    TranscodeSegmentState.TRANSCODED,
                                    runtime.claimStaleMillis()
                            ));
                    inFlightSegments.addAll(runtime.transcodeStatusRepository()
                            .findSegmentNumbersByStateUpdatedSince(
                                    videoId,
                                    profile.getName(),
                                    TranscodeSegmentState.UPLOADING,
                                    runtime.claimStaleMillis()
                            ));
                }
                if (runtime.processingUploadTaskRepository() != null) {
                    inFlightSegments.addAll(
                            runtime.processingUploadTaskRepository().findOpenSegmentNumbers(videoId, profile.getName())
                    );
                }
                if (runtime.processingTaskClaimRepository() != null) {
                    inFlightSegments.addAll(
                            runtime.processingTaskClaimRepository().findClaimedSegmentNumbers(
                                    videoId,
                                    profile.getName(),
                                    runtime.claimStaleMillis()
                            )
                    );
                }
                inFlightSegmentsByProfile.put(profile.getName(), inFlightSegments);
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load in-flight processing work videoId={} profile={}",
                        videoId, profile.getName(), e);
            }
        }
        return inFlightSegmentsByProfile;
    }

    private Map<String, Set<Integer>> loadRecoverableQueuedSegmentsByProfile(
            String videoId,
            Map<String, Set<Integer>> queuedSegmentsByProfile,
            Map<String, Set<Integer>> inFlightSegmentsByProfile
    ) {
        Map<String, Set<Integer>> recoverableQueuedSegmentsByProfile = new HashMap<>();
        if (runtime.transcodeStatusRepository() == null) {
            return recoverableQueuedSegmentsByProfile;
        }
        for (TranscodingProfile profile : profiles) {
            String profileName = profile.getName();
            try {
                Set<Integer> queuedSegments = queuedSegmentsByProfile.getOrDefault(profileName, Set.of());
                if (queuedSegments.isEmpty()) {
                    recoverableQueuedSegmentsByProfile.put(profileName, Set.of());
                    continue;
                }
                Set<Integer> nonQueuedInFlightSegments = new HashSet<>(
                        inFlightSegmentsByProfile.getOrDefault(profileName, Set.of())
                );
                nonQueuedInFlightSegments.removeAll(queuedSegments);
                Set<Integer> freshQueuedSegments = runtime.transcodeStatusRepository().findSegmentNumbersByStateUpdatedSince(
                        videoId,
                        profileName,
                        TranscodeSegmentState.QUEUED,
                        computeQueuedRecoveryStaleMillis(queuedSegments.size(), !nonQueuedInFlightSegments.isEmpty())
                );
                Set<Integer> recoverableQueuedSegments = new HashSet<>(queuedSegments);
                recoverableQueuedSegments.removeAll(freshQueuedSegments);
                recoverableQueuedSegmentsByProfile.put(
                        profileName,
                        recoverableQueuedSegments
                );
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load recoverable queued segments videoId={} profile={}",
                        videoId, profileName, e);
            }
        }
        return recoverableQueuedSegmentsByProfile;
    }

    private boolean hasRecentNonQueuedProgress(String videoId, String profile) {
        if (runtime.processingUploadTaskRepository() != null) {
            try {
                if (!runtime.processingUploadTaskRepository().findOpenSegmentNumbers(videoId, profile).isEmpty()) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load open upload-task segments videoId={} profile={}",
                        videoId, profile, e);
            }
        }
        if (runtime.processingTaskClaimRepository() != null) {
            try {
                if (!runtime.processingTaskClaimRepository().findClaimedSegmentNumbers(
                        videoId,
                        profile,
                        runtime.claimStaleMillis()
                ).isEmpty()) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load claimed processing segments videoId={} profile={}",
                        videoId, profile, e);
            }
        }
        if (runtime.transcodeStatusRepository() == null) {
            return false;
        }
        try {
            return !runtime.transcodeStatusRepository()
                    .findSegmentNumbersByStateUpdatedSince(
                            videoId,
                            profile,
                            TranscodeSegmentState.TRANSCODING,
                            runtime.claimStaleMillis()
                    ).isEmpty()
                    || !runtime.transcodeStatusRepository()
                    .findSegmentNumbersByStateUpdatedSince(
                            videoId,
                            profile,
                            TranscodeSegmentState.TRANSCODED,
                            runtime.claimStaleMillis()
                    ).isEmpty()
                    || !runtime.transcodeStatusRepository()
                    .findSegmentNumbersByStateUpdatedSince(
                            videoId,
                            profile,
                            TranscodeSegmentState.UPLOADING,
                            runtime.claimStaleMillis()
                    ).isEmpty();
        } catch (Exception e) {
            LOGGER.warn("Startup recovery failed to inspect recent non-queued progress videoId={} profile={}",
                    videoId, profile, e);
            return false;
        }
    }

    private long computeQueuedRecoveryStaleMillis(int queuedSegments, boolean hasRecentProgress) {
        long adaptiveStaleMillis = queuedRecoveryStaleMillis;
        if (hasRecentProgress) {
            adaptiveStaleMillis = Math.max(adaptiveStaleMillis, ACTIVE_PROGRESS_QUEUED_RECOVERY_STALE_MILLIS);
        }
        if (queuedSegments >= BACKLOG_QUEUED_SEGMENT_THRESHOLD) {
            long backlogExtension = (long) (queuedSegments - BACKLOG_QUEUED_SEGMENT_THRESHOLD + 1)
                    * BACKLOG_QUEUED_RECOVERY_EXTENSION_PER_SEGMENT_MILLIS;
            adaptiveStaleMillis = Math.max(
                    adaptiveStaleMillis,
                    Math.min(
                            MAX_BACKLOG_QUEUED_RECOVERY_STALE_MILLIS,
                            queuedRecoveryStaleMillis + backlogExtension
                    )
            );
        }
        return adaptiveStaleMillis;
    }

    private Map<Integer, Double> loadSourceSegmentOffsets(String videoId, ObjectStorageClient storageClient) {
        Map<Integer, Double> offsetsBySegment = new HashMap<>();
        String manifestKey = videoId + "/chunks/output.m3u8";
        try {
            if (!storageClient.fileExists(manifestKey)) {
                return offsetsBySegment;
            }
            try (InputStream is = storageClient.downloadFile(manifestKey)) {
                String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
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
                    int segmentNumber = ProcessingRuntime.parseSegmentNumber(trimmed);
                    if (segmentNumber >= 0) {
                        offsetsBySegment.put(segmentNumber, runningOffset);
                    }
                    runningOffset += pendingDuration;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to load source segment offsets for videoId={}", videoId, e);
        }
        return offsetsBySegment;
    }

    private double parseExtinfDuration(String line) {
        java.util.regex.Matcher matcher = EXTINF_PATTERN.matcher(line);
        if (!matcher.find()) {
            return 0d;
        }
        try {
            return Double.parseDouble(matcher.group(1).trim());
        } catch (NumberFormatException e) {
            return 0d;
        }
    }

    /**
     * Best-effort existence check for use during startup recovery.
     * Returns {@code false} when MinIO is unreachable so the spool file
     * is re-registered as a PENDING upload task (safe default — the upload
     * workers will handle the actual upload once MinIO returns).
     */
    private boolean safeFileExists(ObjectStorageClient storageClient, String key) {
        try {
            return storageClient.fileExists(key);
        } catch (Exception e) {
            LOGGER.warn("Spool recovery: unable to check object existence (key={}), treating as not uploaded: {}",
                    key, e.toString());
            return false;
        }
    }

    private TranscodingProfile findProfile(String profileName) {
        if (profileName == null || profileName.isBlank()) {
            return null;
        }
        for (TranscodingProfile profile : profiles) {
            if (profile.getName().equalsIgnoreCase(profileName)) {
                return profile;
            }
        }
        return null;
    }

    private boolean hasActiveProcessingClaim(String videoId, String profileName, int segmentNumber) {
        if (runtime.processingTaskClaimRepository() == null || segmentNumber < 0) {
            return false;
        }
        try {
            return runtime.processingTaskClaimRepository().hasActiveClaim(
                    videoId,
                    profileName,
                    segmentNumber,
                    runtime.claimStaleMillis()
            );
        } catch (Exception e) {
            LOGGER.warn("Spool recovery: failed to check active processing claim videoId={} profile={} segment={}",
                    videoId, profileName, segmentNumber, e);
            return false;
        }
    }

    private boolean isRegisteredForCurrentSpoolFile(UploadTaskMetadata existingTask, Path file) {
        String existingOwner = existingTask.spoolOwner();
        String existingPath = existingTask.spoolPath();
        if (existingOwner == null || existingPath == null) {
            return false;
        }
        return runtime.processorInstanceId().equals(existingOwner)
                && file.toAbsolutePath().toString().equals(existingPath);
    }

    private void deleteDirectoryRecursive(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            LOGGER.warn("Spool recovery: failed to delete {}", path, e);
                        }
                    });
        } catch (IOException e) {
            LOGGER.warn("Spool recovery: failed to recursively delete directory {}", dir, e);
        }
    }
}
