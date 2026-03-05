package com.distributed26.videostreaming.processing;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builds HLS variant + master manifests for processed profiles in Option A:
 * source-chunking remains in chunks/, while per-profile manifests and a master are generated in manifest/.
 */
class AbrManifestService {
    private static final Logger LOGGER = LogManager.getLogger(AbrManifestService.class);
    private static final Pattern EXTINF_PATTERN = Pattern.compile("^#EXTINF:([^,]+),?");
    private static final Pattern LAST_DIGIT_SEQUENCE_PATTERN = Pattern.compile("(\\d+)(?!.*\\d)");
    private static final String SOURCE_MANIFEST_KEY_SUFFIX = "/chunks/output.m3u8";
    private static final String MANIFEST_ROOT = "/manifest/";
    private static final String VARIANT_MANIFEST_SUFFIX = ".m3u8";
    private static final String MASTER_MANIFEST_KEY = "master.m3u8";
    private static final int DEFAULT_TARGET_SEGMENT_DURATION_SECONDS = 10;
    private static final long WAIT_INTERVAL_MILLIS = 1_000L;
    private static final long INITIAL_POLL_INTERVAL_MILLIS = 500L;
    private static final long MAX_POLL_INTERVAL_MILLIS = 10_000L;
    private static final double BACKOFF_MULTIPLIER = 1.5;

    private final ObjectStorageClient storageClient;
    private final int maxWaitSeconds;

    AbrManifestService(ObjectStorageClient storageClient, int maxWaitSeconds) {
        this.storageClient = Objects.requireNonNull(storageClient, "storageClient");
        this.maxWaitSeconds = maxWaitSeconds;
    }

    void generateIfNeeded(String videoId, int totalSegments) throws IOException {
        Objects.requireNonNull(videoId, "videoId");
        String masterManifestKey = videoId + MANIFEST_ROOT + MASTER_MANIFEST_KEY;
        if (storageClient.fileExists(masterManifestKey)) {
            LOGGER.info("Master manifest already exists, skipping generation: {}", masterManifestKey);
            return;
        }

        List<SourceSegment> sourceSegments = parseSourceSegments(videoId, totalSegments);
        LOGGER.info("videoId={} source segment count from manifest={}", videoId, sourceSegments.size());

        for (TranscodingProfile profile : ProcessingServiceApplication.PROFILES) {
            waitForAllTranscodedSegments(videoId, profile, sourceSegments);
            String variantManifestKey = buildVariantManifestKey(videoId, profile.getName());
            String variantPlaylist = buildVariantPlaylist(sourceSegments);
            uploadString(variantManifestKey, variantPlaylist);
            LOGGER.info("Wrote variant manifest: {} ({} segments)", variantManifestKey, sourceSegments.size());
        }

        String masterManifest = buildMasterManifest(videoId);
        uploadString(masterManifestKey, masterManifest);
        LOGGER.info("Wrote master manifest: {} for videoId={}", masterManifestKey, videoId);
    }

    private String buildMasterManifest(String videoId) {
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:6\n");

        for (TranscodingProfile profile : ProcessingServiceApplication.PROFILES) {
            sb.append("#EXT-X-STREAM-INF:BANDWIDTH=").append(profile.getBitrate()).append('\n');
            sb.append(profile.getName())
              .append("/playlist")
              .append(VARIANT_MANIFEST_SUFFIX)
              .append('\n');
        }
        return sb.toString();
    }

    private String buildVariantPlaylist(List<SourceSegment> sourceSegments) {
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:6\n");
        sb.append("#EXT-X-PLAYLIST-TYPE:VOD\n");

        double target = 1.0d;
        for (SourceSegment segment : sourceSegments) {
            target = Math.max(target, Math.ceil(segment.durationSeconds()));
        }
        sb.append("#EXT-X-TARGETDURATION:").append((long) target).append('\n');
        sb.append("#EXT-X-MEDIA-SEQUENCE:0\n");

        for (SourceSegment segment : sourceSegments) {
            sb.append("#EXTINF:")
              .append(formatDuration(segment.durationSeconds()))
              .append(",\n");
            sb.append(segment.uri()).append('\n');
        }
        sb.append("#EXT-X-ENDLIST\n");
        return sb.toString();
    }

    private void waitForAllTranscodedSegments(String videoId, TranscodingProfile profile,
                                             List<SourceSegment> segments) throws IOException {
        String prefix = videoId + "/processed/" + profile.getName() + "/";
        // Build the set of keys still outstanding; it shrinks as segments appear.
        Set<String> remaining = new LinkedHashSet<>();
        for (SourceSegment segment : segments) {
            remaining.add(prefix + segment.uri());
        }

        long deadline = System.currentTimeMillis() + (maxWaitSeconds * 1_000L);
        long pollInterval = INITIAL_POLL_INTERVAL_MILLIS;

        while (!remaining.isEmpty() && System.currentTimeMillis() < deadline) {
            // One listFiles() call per interval instead of one fileExists() per segment.
            Set<String> existing = new HashSet<>(storageClient.listFiles(prefix));
            remaining.removeIf(existing::contains);

            if (remaining.isEmpty()) {
                return;
            }

            LOGGER.debug("Profile {}: waiting for {}/{} transcoded segments (videoId={})",
                    profile.getName(), remaining.size(), segments.size(), videoId);

            sleepQuietly(pollInterval);
            // Exponential backoff: slow down polling for long-running encodes.
            pollInterval = Math.min((long) (pollInterval * BACKOFF_MULTIPLIER), MAX_POLL_INTERVAL_MILLIS);
        }

        throw new IOException("Timed out generating ABR manifests: missing segments for profile " +
                profile.getName() + " on videoId=" + videoId);
    }

    private List<SourceSegment> parseSourceSegments(String videoId, int expectedTotalSegments) throws IOException {
        String sourceManifestKey = videoId + SOURCE_MANIFEST_KEY_SUFFIX;
        if (!waitForSourceManifest(sourceManifestKey)) {
            LOGGER.info("Source manifest never appeared, using chunk fallback for videoId={}", videoId);
            return fallbackSourceSegmentsFromStorage(videoId, expectedTotalSegments);
        }

        List<SourceSegment> parsed = new ArrayList<>();
        double pendingDuration = DEFAULT_TARGET_SEGMENT_DURATION_SECONDS;
        try (InputStream is = storageClient.downloadFile(sourceManifestKey)) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            for (String line : content.split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.startsWith("#")) {
                    if (trimmed.startsWith("#EXTINF:")) {
                        pendingDuration = parseDuration(trimmed).orElse((double) DEFAULT_TARGET_SEGMENT_DURATION_SECONDS);
                    }
                    continue;
                }

                if (trimmed.endsWith(".ts")) {
                    parsed.add(new SourceSegment(trimmed, pendingDuration));
                }
            }
        }

        if (!parsed.isEmpty() && expectedTotalSegments > 0 && parsed.size() > expectedTotalSegments) {
            return parsed.subList(0, expectedTotalSegments);
        }

        if (!parsed.isEmpty()) {
            return parsed;
        }

        LOGGER.info("Source manifest parsing returned no segments, falling back to sorted object listing: {}",
                sourceManifestKey);
        return fallbackSourceSegmentsFromStorage(videoId, expectedTotalSegments);
    }

    private boolean waitForSourceManifest(String sourceManifestKey) throws IOException {
        long deadline = System.currentTimeMillis() + (maxWaitSeconds * 1_000L);
        while (System.currentTimeMillis() < deadline) {
            if (storageClient.fileExists(sourceManifestKey)) {
                return true;
            }
            sleepQuietly(WAIT_INTERVAL_MILLIS);
        }
        return false;
    }

    private List<SourceSegment> fallbackSourceSegmentsFromStorage(String videoId, int expectedTotalSegments) {
        List<String> chunkKeys = storageClient.listFiles(videoId + "/chunks/");
        List<String> segmentNames = chunkKeys.stream()
                .filter(path -> path.endsWith(".ts"))
                .map(path -> {
                    int idx = path.lastIndexOf('/');
                    return idx < 0 ? path : path.substring(idx + 1);
                })
                .sorted(this::compareSegmentNames)
                .toList();

        if (expectedTotalSegments > 0 && segmentNames.size() > expectedTotalSegments) {
            segmentNames = segmentNames.subList(0, expectedTotalSegments);
        }

        List<SourceSegment> fallback = new ArrayList<>(segmentNames.size());
        for (String name : segmentNames) {
            fallback.add(new SourceSegment(name, DEFAULT_TARGET_SEGMENT_DURATION_SECONDS));
        }
        return fallback;
    }

    private int compareSegmentNames(String left, String right) {
        int leftIndex = lastInt(left);
        int rightIndex = lastInt(right);
        if (leftIndex >= 0 && rightIndex >= 0) {
            return Integer.compare(leftIndex, rightIndex);
        }
        return left.compareTo(right);
    }

    private int lastInt(String value) {
        Matcher matcher = LAST_DIGIT_SEQUENCE_PATTERN.matcher(value);
        if (!matcher.find()) {
            return -1;
        }
        return Integer.parseInt(matcher.group(1));
    }

    private Optional<Double> parseDuration(String line) {
        Matcher matcher = EXTINF_PATTERN.matcher(line);
        if (!matcher.find()) {
            return java.util.Optional.empty();
        }
        String raw = matcher.group(1).trim();
        try {
            return java.util.Optional.of(Double.parseDouble(raw));
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse EXTINF duration '{}'", raw);
            return java.util.Optional.empty();
        }
    }

    private String formatDuration(double durationSeconds) {
        return String.format(Locale.US, "%.3f", durationSeconds)
                .replaceFirst("\\.?0+$", "");
    }

    private String buildVariantManifestKey(String videoId, String profileName) {
        return videoId + MANIFEST_ROOT + profileName + VARIANT_MANIFEST_SUFFIX;
    }

    private void uploadString(String key, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        try (InputStream is = new ByteArrayInputStream(bytes)) {
            storageClient.uploadFile(key, is, bytes.length);
        }
    }

    private void sleepQuietly(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Manifest generation interrupted", e);
        }
    }

    private record SourceSegment(String uri, double durationSeconds) {
        SourceSegment {
            if (uri == null || uri.isBlank()) throw new IllegalArgumentException("uri is blank");
            if (durationSeconds <= 0d) {
                throw new IllegalArgumentException("durationSeconds must be > 0");
            }
        }
    }
}
