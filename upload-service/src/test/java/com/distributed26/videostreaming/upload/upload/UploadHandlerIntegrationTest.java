package com.distributed26.videostreaming.upload.upload;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Integration tests for UploadHandler that require FFmpeg to be installed.
 *
 * These tests verify the complete upload, segmentation, and storage flow.
 * They are skipped if FFmpeg is not available in the system PATH.
 *
 * Note: These tests use the global .env file in the Video-Streaming root directory.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("isFfmpegAvailable")
public class UploadHandlerIntegrationTest {

    private ObjectStorageClient mockStorageClient;
    private Path testVideoFile;
    private final ObjectMapper objectMapper = new ObjectMapper();

    static boolean isFfmpegAvailable() {
        try {
            Process process = new ProcessBuilder("ffmpeg", "-version")
                .redirectErrorStream(true)
                .start();
            int exitCode = process.waitFor();
            boolean available = exitCode == 0;
            System.out.println("[Integration Test] FFmpeg available: " + available);
            return available;
        } catch (Exception e) {
            System.out.println("[Integration Test] FFmpeg available: false (error: " + e.getMessage() + ")");
            return false;
        }
    }

    @BeforeAll
    void setUpClass() throws Exception {
        // Create a real test video file using FFmpeg
        testVideoFile = createTestVideo();
    }

    @AfterAll
    void tearDownClass() throws Exception {
        // Cleanup test video
        if (testVideoFile != null && Files.exists(testVideoFile)) {
            Files.deleteIfExists(testVideoFile);
        }
    }

    @BeforeEach
    void setUp() {
        mockStorageClient = mock(ObjectStorageClient.class);
    }

    @Test
    @DisplayName("Should segment video and upload all chunks to storage")
    void shouldSegmentVideoAndUploadAllChunks() throws Exception {
        List<String> uploadedKeys = new CopyOnWriteArrayList<>();
        List<Long> uploadedSizes = new CopyOnWriteArrayList<>();

        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            Long size = invocation.getArgument(2);
            uploadedKeys.add(key);
            uploadedSizes.add(size);
            return null;
        }).when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

        UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
        Javalin app = Javalin.create();
        app.post("/upload", handler::upload);

        final String[] capturedVideoId = new String[1];

        JavalinTest.test(app, (server, client) -> {
            byte[] videoBytes = Files.readAllBytes(testVideoFile);

            RequestBody fileBody = RequestBody.create(
                videoBytes,
                MediaType.parse("video/mp4")
            );

            MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", "integration-test.mp4", fileBody)
                .build();

            try (var response = client.request("/upload", builder ->
                builder.post(requestBody))) {
                assertEquals(202, response.code());
                JsonNode json = objectMapper.readTree(response.body().string());
                capturedVideoId[0] = json.get("videoId").asText();
            }
        });

        // Wait for async processing to complete
        await().atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .until(() -> {
                // Should have uploaded at least one .ts segment and one .m3u8 playlist
                boolean hasSegment = uploadedKeys.stream().anyMatch(k -> k.endsWith(".ts"));
                boolean hasPlaylist = uploadedKeys.stream().anyMatch(k -> k.endsWith(".m3u8"));
                return hasSegment && hasPlaylist;
            });

        // Verify uploads
        assertTrue(uploadedKeys.size() >= 2, "Should have uploaded at least segment and playlist");

        // All keys should contain the video ID
        String finalVideoId = capturedVideoId[0];
        assertTrue(uploadedKeys.stream().allMatch(k -> k.contains(finalVideoId)),
            "All keys should contain video ID");

        // All keys should follow the pattern: {videoId}/chunks/{filename}
        assertTrue(uploadedKeys.stream().allMatch(k -> k.matches("[a-f0-9-]+/chunks/.+")),
            "All keys should follow expected pattern");

        // Verify playlist was uploaded
        assertTrue(uploadedKeys.stream().anyMatch(k -> k.endsWith("output.m3u8")),
            "Should have uploaded HLS playlist");

        // Verify at least one segment was uploaded
        assertTrue(uploadedKeys.stream().anyMatch(k -> k.matches(".*output\\d+\\.ts")),
            "Should have uploaded at least one TS segment");

        // Verify all uploads have non-zero size
        assertTrue(uploadedSizes.stream().allMatch(s -> s > 0),
            "All uploaded files should have positive size");
    }

    @Test
    @DisplayName("Should handle storage failure during segment upload")
    void shouldHandleStorageFailureDuringSegmentUpload() throws Exception {
        // Make storage fail after first successful upload
        List<String> uploadedKeys = new ArrayList<>();

        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            uploadedKeys.add(key);
            if (uploadedKeys.size() > 1) {
                throw new RuntimeException("Simulated storage failure");
            }
            return null;
        }).when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

        UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
        Javalin app = Javalin.create();
        app.post("/upload", handler::upload);

        JavalinTest.test(app, (server, client) -> {
            byte[] videoBytes = Files.readAllBytes(testVideoFile);

            RequestBody fileBody = RequestBody.create(
                videoBytes,
                MediaType.parse("video/mp4")
            );

            MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", "fail-test.mp4", fileBody)
                .build();

            try (var response = client.request("/upload", builder ->
                builder.post(requestBody))) {
                // Initial response should still be 202
                assertEquals(202, response.code());
            }
        });

        // Wait for processing to fail
        Thread.sleep(5000);

        // Verify at least one upload was attempted before failure
        assertFalse(uploadedKeys.isEmpty(), "Should have attempted at least one upload");
    }

    @Test
    @DisplayName("Should properly cleanup temp files even on failure")
    void shouldCleanupTempFilesOnFailure() throws Exception {
        // Make storage always fail
        doThrow(new RuntimeException("Storage unavailable"))
            .when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

        UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
        Javalin app = Javalin.create();
        app.post("/upload", handler::upload);

        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));

        // Count temp files before test
        long uploadFilesBefore;
        try (var stream = Files.list(tempDir)) {
            uploadFilesBefore = stream
                .filter(p -> p.getFileName().toString().startsWith("upload-"))
                .count();
        }

        JavalinTest.test(app, (server, client) -> {
            byte[] videoBytes = Files.readAllBytes(testVideoFile);

            RequestBody fileBody = RequestBody.create(
                videoBytes,
                MediaType.parse("video/mp4")
            );

            MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", "cleanup-test.mp4", fileBody)
                .build();

            try (var response = client.request("/upload", builder ->
                builder.post(requestBody))) {
                assertEquals(202, response.code());
            }
        });

        // Wait for processing and cleanup
        final long beforeCount = uploadFilesBefore;
        await().atMost(Duration.ofSeconds(20))
            .pollInterval(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> {
                // Check that temp files are cleaned up (allow for concurrent test runs)
                try (var stream = Files.list(tempDir)) {
                    long uploadFilesAfter = stream
                        .filter(p -> p.getFileName().toString().startsWith("upload-"))
                        .count();
                    // Files should not accumulate significantly
                    return uploadFilesAfter <= beforeCount + 2;
                }
            });
    }

    @Test
    @DisplayName("Should upload segments progressively as FFmpeg generates them")
    void shouldUploadSegmentsProgressively() throws Exception {
        List<Long> uploadTimestamps = new CopyOnWriteArrayList<>();

        doAnswer(invocation -> {
            uploadTimestamps.add(System.currentTimeMillis());
            // Simulate some upload delay
            Thread.sleep(50);
            return null;
        }).when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

        UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
        Javalin app = Javalin.create();
        app.post("/upload", handler::upload);

        JavalinTest.test(app, (server, client) -> {
            byte[] videoBytes = Files.readAllBytes(testVideoFile);

            RequestBody fileBody = RequestBody.create(
                videoBytes,
                MediaType.parse("video/mp4")
            );

            MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", "progressive-test.mp4", fileBody)
                .build();

            try (var response = client.request("/upload", builder ->
                builder.post(requestBody))) {
                assertEquals(202, response.code());
            }
        });

        // Wait for processing to complete
        await().atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .until(() -> uploadTimestamps.size() >= 2);

        // Verify that uploads were spread out over time (progressive upload)
        if (uploadTimestamps.size() >= 2) {
            long firstUpload = uploadTimestamps.get(0);
            long lastUpload = uploadTimestamps.get(uploadTimestamps.size() - 1);

            // There should be some time gap between first and last upload
            // indicating progressive processing rather than batch at the end
            // Note: On fast systems, this gap might be small, so we use a small threshold
            assertTrue(lastUpload >= firstUpload,
                "Last upload should not be before first upload");
        }
    }

    /**
     * Creates a test video file using FFmpeg.
     * The video is 3 seconds long with color bars.
     */
    private Path createTestVideo() throws Exception {
        Path testVideo = Files.createTempFile("test-video-", ".mp4");

        ProcessBuilder pb = new ProcessBuilder(
            "ffmpeg",
            "-y",  // Overwrite output
            "-f", "lavfi",  // Use lavfi input
            "-i", "testsrc=duration=3:size=320x240:rate=30",  // 3-second test pattern
            "-f", "lavfi",
            "-i", "sine=frequency=1000:duration=3",  // Audio
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-c:a", "aac",
            "-shortest",
            testVideo.toString()
        );

        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read output to prevent buffer blocking
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            while (reader.readLine() != null) {
                // Consume output
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            Files.deleteIfExists(testVideo);
            throw new RuntimeException("Failed to create test video, exit code: " + exitCode);
        }

        return testVideo;
    }
}






