package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.shared.upload.FailedVideoRegistry;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.TranscodedSegmentStatusRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRecord;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import com.distributed26.videostreaming.upload.processing.StorageRetryExecutor;
import com.distributed26.videostreaming.upload.processing.StorageStateTracker;
import com.distributed26.videostreaming.upload.processing.UploadProcessingConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import io.javalin.testtools.HttpClient;
import io.javalin.testtools.JavalinTest;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

@Tag("integration")
class UploadHandlerIntegrationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final org.apache.logging.log4j.Logger logger =
        org.apache.logging.log4j.LogManager.getLogger(UploadHandlerIntegrationTest.class);


    private String jdbcUrl;
    private String username;
    private String password;

    @AfterEach
    void tearDown() {
        // per-test cleanup happens explicitly once videoId is known
    }

    @Test
    void retryWithSameVideoIdSkipsPreviouslyUploadedSegments() throws Exception {
        assumeFfmpegAvailable();
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        Path testVideo = createTestVideo();
        byte[] videoBytes = Files.readAllBytes(testVideo);
        Files.deleteIfExists(testVideo);

        VideoUploadRepository videoRepo = new VideoUploadRepository(jdbcUrl, username, password);
        SegmentUploadRepository segmentRepo = new SegmentUploadRepository(jdbcUrl, username, password);
        FlakyStorageClient storageClient = new FlakyStorageClient(videoId + "/chunks/output8.ts");
        TestStatusEventBus statusEventBus = new TestStatusEventBus();
        TestTranscodeTaskBus transcodeTaskBus = new TestTranscodeTaskBus();
        UploadProcessingConfig config = new UploadProcessingConfig(
                200,
                5,
                60_000,
                100,
                1,
                1,
                1,
                1,
                1
        );
        UploadHandler handler = new UploadHandler(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                videoRepo,
                segmentRepo,
                "it-machine",
                "it-container",
                new FailedVideoRegistry(),
                new StorageStateTracker(videoRepo, statusEventBus),
                config
        );

        Javalin app = Javalin.create();
        app.post("/upload", handler::upload);

        try {
            JavalinTest.test(app, (server, client) -> {
                JsonNode firstResponse = sendUpload(client, videoBytes, videoId, "resume-test-first.mp4");
                assertEquals(videoId, firstResponse.get("videoId").asText());
                assertTrue(storageClient.firstFailure.await(10, TimeUnit.SECONDS),
                        "first upload should fail after a partial chunk upload");
                awaitCondition(Duration.ofSeconds(20), () -> statusOf(videoRepo, videoId).equals("FAILED"));

                assertEquals(expectedUploadedBeforeFailure(), segmentRepo.findSegmentNumbers(videoId),
                        "first attempt should record the first eight uploaded segments");
                for (int i = 0; i < 8; i++) {
                    assertEquals(1, storageClient.uploadAttempts(videoId + "/chunks/output" + i + ".ts"));
                }
                assertEquals(0, storageClient.successfulUploads(videoId + "/chunks/output8.ts"));

                JsonNode secondResponse = sendUpload(client, videoBytes, videoId, "resume-test-second.mp4");
                assertEquals(videoId, secondResponse.get("videoId").asText());

                awaitCondition(Duration.ofSeconds(20), () -> statusOf(videoRepo, videoId).equals("UPLOADED"));

                Set<Integer> uploadedSegments = segmentRepo.findSegmentNumbers(videoId);
                assertTrue(uploadedSegments.size() >= 9,
                        "retry should upload the remaining segments after the first eight");
                for (int i = 0; i < 8; i++) {
                    assertEquals(1, storageClient.uploadAttempts(videoId + "/chunks/output" + i + ".ts"),
                            "retry should skip segment " + i + " because it was already uploaded");
                }
                assertEquals(1, storageClient.successfulUploads(videoId + "/chunks/output8.ts"),
                        "retry should complete the previously failed ninth segment");

                Optional<VideoUploadRecord> record = videoRepo.findByVideoId(videoId);
                assertTrue(record.isPresent(), "video record should exist");
                assertEquals("UPLOADED", record.get().getStatus());
            });
        } finally {
            handler.close();
            cleanupVideoRecords(videoId);
        }
    }

    @Test
    @DisplayName("client can keep monitoring status after upload-service work is done without restarting upload")
    void statusRemainsVisibleAfterUploadServiceStops() throws Exception {
        assumeFfmpegAvailable();
        loadDatabaseConfig();
        assumeDatabaseReachable();

        String videoId = UUID.randomUUID().toString();
        Path testVideo = createTestVideo();
        byte[] videoBytes = Files.readAllBytes(testVideo);
        Files.deleteIfExists(testVideo);

        VideoUploadRepository videoRepo = new VideoUploadRepository(jdbcUrl, username, password);
        SegmentUploadRepository segmentRepo = new SegmentUploadRepository(jdbcUrl, username, password);
        TranscodedSegmentStatusRepository transcodeRepo =
                new TranscodedSegmentStatusRepository(jdbcUrl, username, password);
        FlakyStorageClient storageClient = new FlakyStorageClient(null);
        TestStatusEventBus statusEventBus = new TestStatusEventBus();
        TestTranscodeTaskBus transcodeTaskBus = new TestTranscodeTaskBus();
        UploadProcessingConfig config = new UploadProcessingConfig(
                200,
                5,
                60_000,
                100,
                1,
                1,
                1,
                1,
                1
        );
        UploadHandler uploadHandler = new UploadHandler(
                storageClient,
                statusEventBus,
                transcodeTaskBus,
                videoRepo,
                segmentRepo,
                "it-machine",
                "it-container",
                new FailedVideoRegistry(),
                new StorageStateTracker(videoRepo, statusEventBus),
                config
        );

        Javalin uploadApp = Javalin.create();
        uploadApp.post("/upload", uploadHandler::upload);

        try {
            JavalinTest.test(uploadApp, (server, client) -> {
                JsonNode response = sendUpload(client, videoBytes, videoId, "status-monitoring.mp4");
                assertEquals(videoId, response.get("videoId").asText());
                awaitCondition(Duration.ofSeconds(20), () -> statusOf(videoRepo, videoId).equals("UPLOADED"));
            });
            int uploadedSegments = segmentRepo.countByVideoId(videoId);
            assertTrue(uploadedSegments >= 2, "expected multiple uploaded source segments");
            int uploadAttemptsBeforeShutdown = storageClient.totalUploadAttempts();

            uploadHandler.close();
            if (uploadApp != null) {
                uploadApp.stop();
            }

            insertTranscodeState(videoId, "low", 0, "DONE");
            insertTranscodeState(videoId, "low", 1, "DONE");
            insertTranscodeState(videoId, "medium", 0, "DONE");

            UploadInfoHandler infoHandler = new UploadInfoHandler(videoRepo, segmentRepo, transcodeRepo);
            Javalin statusApp = Javalin.create();
            statusApp.get("/upload-info/{videoId}", infoHandler::getInfo);

            try {
                JavalinTest.test(statusApp, (server, client) -> {
                    try (var response = client.get("/upload-info/" + videoId)) {
                        assertEquals(200, response.code());
                        JsonNode body = OBJECT_MAPPER.readTree(response.body().string());
                        assertEquals(videoId, body.get("videoId").asText());
                        assertEquals("UPLOADED", body.get("status").asText());
                        assertEquals("Upload completed. Waiting for processing to finish.",
                                body.get("statusMessage").asText());
                        assertEquals(uploadedSegments, body.get("uploadedSegments").asInt());
                        assertEquals(2, body.get("transcode").get("lowDone").asInt());
                        assertEquals(1, body.get("transcode").get("mediumDone").asInt());
                        assertEquals(0, body.get("transcode").get("highDone").asInt());
                    }
                });
            } finally {
                statusApp.stop();
            }

            assertEquals(uploadAttemptsBeforeShutdown, storageClient.totalUploadAttempts(),
                    "status monitoring should not trigger any additional upload attempts");
        } finally {
            uploadHandler.close();
            cleanupTranscodeState(videoId);
            cleanupVideoRecords(videoId);
        }
    }

    private JsonNode sendUpload(HttpClient client, byte[] videoBytes, String videoId, String fileName)
            throws IOException {
        RequestBody fileBody = RequestBody.create(videoBytes, MediaType.parse("video/mp4"));
        MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", fileName, fileBody)
                .addFormDataPart("name", "Resume Scenario Test")
                .addFormDataPart("videoId", videoId)
                .build();

        try (var response = client.request("/upload", builder -> builder.post(requestBody))) {
            assertEquals(202, response.code());
            return OBJECT_MAPPER.readTree(response.body().string());
        }
    }

    private void loadDatabaseConfig() {
        Dotenv dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
        jdbcUrl = normalizeJdbcUrl(firstNonBlank(System.getenv("PG_URL"), dotenv.get("PG_URL")));
        username = firstNonBlank(System.getenv("PG_USER"), dotenv.get("PG_USER"));
        password = firstNonBlank(System.getenv("PG_PASSWORD"), dotenv.get("PG_PASSWORD"));
    }

    private void assumeDatabaseReachable() {
        Assumptions.assumeTrue(jdbcUrl != null && !jdbcUrl.isBlank(), "PG_URL is not set");
        Assumptions.assumeTrue(username != null && !username.isBlank(), "PG_USER is not set");
        try (Connection ignored = DriverManager.getConnection(jdbcUrl, username, password)) {
            // reachable
        } catch (SQLException e) {
            Assumptions.assumeTrue(false, "Skipping integration test because Postgres is not reachable: " + e.getMessage());
        }
    }

    private void cleanupVideoRecords(String videoId) {
        if (videoId == null || jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM segment_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM video_upload WHERE video_id = ?")) {
                ps.setObject(1, UUID.fromString(videoId));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up integration test data", e);
        }
    }

    private void cleanupTranscodeState(String videoId) {
        if (videoId == null || jdbcUrl == null || username == null) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement("DELETE FROM transcoded_segment_status WHERE video_id = ?")) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean up transcode integration test data", e);
        }
    }

    private void insertTranscodeState(String videoId, String profile, int segmentNumber, String state) {
        String sql = """
                INSERT INTO transcoded_segment_status (video_id, profile, segment_number, state)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (video_id, profile, segment_number) DO UPDATE
                SET state = EXCLUDED.state,
                    updated_at = NOW()
                """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(videoId));
            ps.setString(2, profile);
            ps.setInt(3, segmentNumber);
            ps.setString(4, state);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert transcoded_segment_status", e);
        }
    }

    private String statusOf(VideoUploadRepository repo, String videoId) {
        return repo.findByVideoId(videoId).map(VideoUploadRecord::getStatus).orElse("");
    }

    private void awaitCondition(Duration timeout, CheckedBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not satisfied within " + timeout);
    }

    private void assumeFfmpegAvailable() {
        try {
            Process process = new ProcessBuilder("ffmpeg", "-version")
                    .redirectErrorStream(true)
                    .start();
            int exitCode = process.waitFor();
            Assumptions.assumeTrue(exitCode == 0, "Skipping integration test because ffmpeg is not available");
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Skipping integration test because ffmpeg is not available: " + e.getMessage());
        }
    }

    private Path createTestVideo() throws Exception {
        Path testVideo = Files.createTempFile("upload-resume-it-", ".mp4");
        ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg",
                "-f", "lavfi",
                "-i", "testsrc=duration=52:size=320x240:rate=25",
                "-f", "lavfi",
                "-i", "sine=frequency=1000:duration=52",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-g", "25",
                "-keyint_min", "25",
                "-sc_threshold", "0",
                "-c:a", "aac",
                "-shortest",
                "-y",
                testVideo.toString()
        );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            while (reader.readLine() != null) {
                // drain process output
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            Files.deleteIfExists(testVideo);
            throw new RuntimeException("Failed to create test video, exit code: " + exitCode);
        }
        return testVideo;
    }

    private String normalizeJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        return url
                .replace("jdbc:postgresql://postgres:", "jdbc:postgresql://localhost:")
                .replace("jdbc:postgresql://host.docker.internal:", "jdbc:postgresql://localhost:");
    }

    private String firstNonBlank(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    private Set<Integer> expectedUploadedBeforeFailure() {
        Set<Integer> segments = new LinkedHashSet<>();
        for (int i = 0; i < 8; i++) {
            segments.add(i);
        }
        return segments;
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private static final class FlakyStorageClient implements ObjectStorageClient {
        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
        private final Map<String, Integer> successes = new ConcurrentHashMap<>();
        private final String failOnceKey;
        private volatile boolean failedOnce;
        private final CountDownLatch firstFailure = new CountDownLatch(1);

        private FlakyStorageClient(String failOnceKey) {
            this.failOnceKey = failOnceKey;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            attempts.merge(key, 1, Integer::sum);
            if (!failedOnce && key.equals(failOnceKey)) {
                failedOnce = true;
                firstFailure.countDown();
                throw new StorageRetryExecutor.NonRetriableException("simulated upload-service crash during segment upload");
            }
            try {
                objects.put(key, data.readAllBytes());
                successes.merge(key, 1, Integer::sum);
            } catch (IOException e) {
                throw new RuntimeException("Failed to store object in test storage", e);
            }
        }

        @Override
        public InputStream downloadFile(String key) {
            byte[] data = objects.get(key);
            if (data == null) {
                throw new RuntimeException("Object not found: " + key);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public void deleteFile(String key) {
            objects.remove(key);
        }

        @Override
        public boolean fileExists(String key) {
            return objects.containsKey(key);
        }

        @Override
        public List<String> listFiles(String prefix) {
            return objects.keySet().stream().filter(key -> key.startsWith(prefix)).sorted().toList();
        }

        @Override
        public void ensureBucketExists() {
        }

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key;
        }

        private int uploadAttempts(String key) {
            return attempts.getOrDefault(key, 0);
        }

        private int successfulUploads(String key) {
            return successes.getOrDefault(key, 0);
        }

        private int totalUploadAttempts() {
            return attempts.values().stream().mapToInt(Integer::intValue).sum();
        }
    }
}
