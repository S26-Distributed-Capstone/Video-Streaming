package com.distributed26.videostreaming.processing.runtime;

import com.distributed26.videostreaming.processing.AbrManifestService;
import com.distributed26.videostreaming.processing.TranscodingProfile;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StartupRecoveryService {
    private static final Logger LOGGER = LogManager.getLogger(StartupRecoveryService.class);
    private static final java.util.regex.Pattern EXTINF_PATTERN = java.util.regex.Pattern.compile("^#EXTINF:([^,]+),?");

    private final TranscodingProfile[] profiles;
    private final ProcessingRuntime runtime;

    public StartupRecoveryService(TranscodingProfile[] profiles, ProcessingRuntime runtime) {
        this.profiles = profiles;
        this.runtime = runtime;
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
            if (runtime.processingUploadTaskRepository() != null) {
                recoverableVideoIds.addAll(runtime.processingUploadTaskRepository().findVideoIdsWithOpenTasks());
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

    private void recoverVideo(String videoId, ObjectStorageClient storageClient) {
        try {
            List<String> chunkKeys = listSourceChunkKeys(videoId, storageClient);
            if (chunkKeys.isEmpty()) {
                LOGGER.info("Startup recovery: no source chunks found for videoId={}", videoId);
                return;
            }

            int totalSegments = Math.max(runtime.findTotalSegments(videoId), chunkKeys.size());
            Map<String, Set<Integer>> doneSegmentsByProfile = loadDoneSegmentsByProfile(videoId);
            Map<String, Set<Integer>> openUploadSegmentsByProfile = loadOpenUploadSegmentsByProfile(videoId);
            int republished = 0;
            Set<String> touchedProfiles = new HashSet<>();

            Map<Integer, Double> offsetsBySegment = loadSourceSegmentOffsets(videoId, storageClient);
            for (String chunkKey : chunkKeys) {
                int segmentNumber = ProcessingRuntime.parseSegmentNumber(chunkKey);
                if (segmentNumber < 0) {
                    LOGGER.warn("Startup recovery: skipping chunk with unparseable segment number videoId={} key={}",
                            videoId, chunkKey);
                    continue;
                }
                for (TranscodingProfile profile : profiles) {
                    Set<Integer> doneSegments = doneSegmentsByProfile.getOrDefault(profile.getName(), Set.of());
                    Set<Integer> openSegments = openUploadSegmentsByProfile.getOrDefault(profile.getName(), Set.of());
                    if (doneSegments.contains(segmentNumber) || openSegments.contains(segmentNumber)) {
                        continue;
                    }
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

    private Map<String, Set<Integer>> loadOpenUploadSegmentsByProfile(String videoId) {
        Map<String, Set<Integer>> openSegmentsByProfile = new HashMap<>();
        if (runtime.processingUploadTaskRepository() == null) {
            return openSegmentsByProfile;
        }
        for (TranscodingProfile profile : profiles) {
            try {
                openSegmentsByProfile.put(
                        profile.getName(),
                        runtime.processingUploadTaskRepository().findOpenSegmentNumbers(videoId, profile.getName())
                );
            } catch (Exception e) {
                LOGGER.warn("Startup recovery failed to load local upload tasks videoId={} profile={}",
                        videoId, profile.getName(), e);
            }
        }
        return openSegmentsByProfile;
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
}
