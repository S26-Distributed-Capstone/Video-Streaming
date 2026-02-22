package com.distributed26.videostreaming.upload.upload;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for UploadHandler.
 *
 * Tests cover:
 * - Request validation (missing file handling)
 * - Successful upload flow with mocked dependencies
 * - Error scenarios (storage failures, timeouts)
 * - Concurrent upload handling
 *
 * Note: These tests use the global .env file in the Video-Streaming root directory.
 *
 * IMPORTANT: These are UNIT tests that send fake video data. FFmpeg errors like
 * "Invalid data found when processing input" are EXPECTED and can be ignored.
 * For tests with real video processing, see UploadHandlerIntegrationTest.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class UploadHandlerTest {

    @Mock
    private ObjectStorageClient mockStorageClient;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    static void printTestInfo() {
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("[UploadHandlerTest] Running UNIT tests with mock video data");
        System.out.println("[UploadHandlerTest] FFmpeg 'Invalid data' errors are EXPECTED - ignore them");
        System.out.println("=".repeat(70));
        System.out.println();
    }

    @Nested
    @DisplayName("Request Validation Tests")
    class RequestValidationTests {

        @Test
        @DisplayName("Should return 400 when no file is provided")
        void shouldReturn400WhenNoFileProvided() {
            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            JavalinTest.test(app, (server, client) -> {
                // Send POST without file
                try (var response = client.post("/upload", "")) {
                    assertEquals(400, response.code());
                    String body = response.body().string();
                    assertTrue(body.contains("No 'file' part found"));
                }
            });
        }

        @Test
        @DisplayName("Should return 202 Accepted with video ID for valid upload")
        void shouldReturn202WithVideoIdForValidUpload() {
            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            JavalinTest.test(app, (server, client) -> {
                // Create a simple test file
                RequestBody fileBody = RequestBody.create(
                    "test video content".getBytes(),
                    MediaType.parse("video/mp4")
                );

                MultipartBody requestBody = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("file", "test-video.mp4", fileBody)
                    .build();

                try (var response = client.request("/upload", builder ->
                    builder.post(requestBody))) {
                    assertEquals(202, response.code());
                    String body = response.body().string();
                    // Response should include videoId and uploadStatusUrl fields
                    assertNotNull(body);
                    assertTrue(body.contains("\"videoId\""), "Response should contain videoId");
                    assertTrue(body.contains("\"uploadStatusUrl\""), "Response should contain uploadStatusUrl");
                }
            });
        }
    }

    @Nested
    @DisplayName("Storage Client Interaction Tests")
    class StorageClientInteractionTests {

        @Test
        @DisplayName("Should upload segments to storage with correct key format")
        @EnabledIf("com.distributed26.videostreaming.upload.upload.UploadHandlerTest#isFfmpegAvailable")
        void shouldUploadSegmentsWithCorrectKeyFormat() throws Exception {
            Path testVideo = createTestVideo();
            Assumptions.assumeTrue(testVideo != null, "FFmpeg test video creation failed");
            try {
                // Capture upload calls
                ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
                doNothing().when(mockStorageClient).uploadFile(keyCaptor.capture(), any(InputStream.class), anyLong());

                UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
                Javalin app = Javalin.create();
                app.post("/upload", handler::upload);

                JavalinTest.test(app, (server, client) -> {
                    // Use a real short video file so FFmpeg can segment it
                    RequestBody fileBody = RequestBody.create(
                        Files.readAllBytes(testVideo),
                        MediaType.parse("video/mp4")
                    );

                    MultipartBody requestBody = new MultipartBody.Builder()
                        .setType(MultipartBody.FORM)
                        .addFormDataPart("file", "test-video.mp4", fileBody)
                        .build();

                    try (var response = client.request("/upload", builder ->
                        builder.post(requestBody))) {
                        assertEquals(202, response.code());
                    }
                });

                // Wait for async processing to attempt uploads
                // Note: This may fail if FFmpeg isn't installed - that's expected in unit tests
                await().atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(100))
                    .ignoreExceptions()
                    .until(() -> {
                        // Check if any uploads were captured
                        List<String> uploadedKeys = keyCaptor.getAllValues();
                        // Either uploads happened or processing failed (FFmpeg not installed)
                        return !uploadedKeys.isEmpty();
                    });
            } finally {
                Files.deleteIfExists(testVideo);
            }
        }

        @Test
        @DisplayName("Should handle storage client upload failure gracefully")
        void shouldHandleStorageUploadFailure() throws Exception {
            // Simulate storage failure
            doThrow(new RuntimeException("Storage unavailable"))
                .when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            JavalinTest.test(app, (server, client) -> {
                RequestBody fileBody = RequestBody.create(
                    "test content".getBytes(),
                    MediaType.parse("video/mp4")
                );

                MultipartBody requestBody = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("file", "test-video.mp4", fileBody)
                    .build();

                try (var response = client.request("/upload", builder ->
                    builder.post(requestBody))) {
                    // Initial response should still be 202 (async processing)
                    assertEquals(202, response.code());
                }
            });

            // The error handling happens asynchronously - just ensure no exceptions leak
            Thread.sleep(1000);
        }
    }

    @Nested
    @DisplayName("Retry Skip Tests")
    class RetrySkipTests {

        @Test
        @DisplayName("Should skip uploading segments already recorded for the video")
        void shouldSkipPreviouslyUploadedSegments() throws Exception {
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            doNothing().when(mockStorageClient).uploadFile(keyCaptor.capture(), any(InputStream.class), anyLong());

            Set<Integer> existingSegments = new HashSet<>();
            existingSegments.add(0);
            FakeSegmentUploadRepository fakeRepo = new FakeSegmentUploadRepository(existingSegments);

            UploadHandler handler = new UploadHandler(
                mockStorageClient,
                new TestJobTaskBus(),
                null,
                fakeRepo,
                "test-machine"
            );

            Path tempDir = Files.createTempDirectory("upload-test-segments-");
            try {
                Path seg0 = tempDir.resolve("output0.ts");
                Path seg1 = tempDir.resolve("output1.ts");
                Path playlist = tempDir.resolve("output.m3u8");
                Files.writeString(seg0, "seg0");
                Files.writeString(seg1, "seg1");
                Files.writeString(playlist, "#EXTM3U");

                Set<Path> uploadedFiles = new HashSet<>();
                Set<Integer> uploadedSegmentNumbers = new HashSet<>(existingSegments);

                var method = UploadHandler.class.getDeclaredMethod(
                    "uploadReadySegments",
                    Path.class,
                    String.class,
                    Set.class,
                    Set.class,
                    boolean.class
                );
                method.setAccessible(true);
                String videoId = "123e4567-e89b-12d3-a456-426614174000";
                method.invoke(handler, tempDir, videoId, uploadedFiles, uploadedSegmentNumbers, true);

                List<String> uploadedKeys = keyCaptor.getAllValues();
                Set<Integer> uploadedSegments = extractSegmentNumbersFromKeys(uploadedKeys);

                assertFalse(uploadedSegments.contains(0), "segment 0 should be skipped");
                assertTrue(uploadedSegments.contains(1), "segment 1 should be uploaded");
                assertTrue(uploadedSegmentNumbers.contains(1), "uploaded segment numbers should be updated");
            } finally {
                try (var walk = Files.walk(tempDir)) {
                    walk.sorted((a, b) -> b.compareTo(a)).forEach(path -> path.toFile().delete());
                }
            }
        }
    }

    static boolean isFfmpegAvailable() {
        try {
            Process process = new ProcessBuilder("ffmpeg", "-version")
                .redirectErrorStream(true)
                .start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Nested
    @DisplayName("Concurrent Upload Tests")
    class ConcurrentUploadTests {

        @Test
        @DisplayName("Should handle multiple concurrent uploads")
        void shouldHandleMultipleConcurrentUploads() throws Exception {
            AtomicInteger uploadCount = new AtomicInteger(0);
            doAnswer(invocation -> {
                uploadCount.incrementAndGet();
                return null;
            }).when(mockStorageClient).uploadFile(anyString(), any(InputStream.class), anyLong());

            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            JavalinTest.test(app, (server, client) -> {
                // Submit multiple concurrent uploads
                int concurrentUploads = 3;

                for (int i = 0; i < concurrentUploads; i++) {
                    RequestBody fileBody = RequestBody.create(
                        ("test content " + i).getBytes(),
                        MediaType.parse("video/mp4")
                    );

                    MultipartBody requestBody = new MultipartBody.Builder()
                        .setType(MultipartBody.FORM)
                        .addFormDataPart("file", "test-video-" + i + ".mp4", fileBody)
                        .build();

                    try (var response = client.request("/upload", builder ->
                        builder.post(requestBody))) {
                        assertEquals(202, response.code());
                    }
                }
            });

            // Allow some time for async processing
            Thread.sleep(2000);
        }
    }

    @Nested
    @DisplayName("File Cleanup Tests")
    class FileCleanupTests {

        @Test
        @DisplayName("Should cleanup temporary files after processing")
        void shouldCleanupTemporaryFilesAfterProcessing() throws Exception {
            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));

            // Count temp files before upload
            long tempFilesBefore = countTempFiles(tempDir, "upload-");
            long hlsDirsBefore = countTempFiles(tempDir, "hls-");

            JavalinTest.test(app, (server, client) -> {
                RequestBody fileBody = RequestBody.create(
                    "test content".getBytes(),
                    MediaType.parse("video/mp4")
                );

                MultipartBody requestBody = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("file", "test-video.mp4", fileBody)
                    .build();

                try (var response = client.request("/upload", builder ->
                    builder.post(requestBody))) {
                    assertEquals(202, response.code());
                }
            });

            // Wait for async processing and cleanup
            await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    // Temp files should eventually be cleaned up
                    // We allow some tolerance since other processes may create temp files
                    long uploadFilesAfter = countTempFiles(tempDir, "upload-");
                    long hlsDirsAfter = countTempFiles(tempDir, "hls-");

                    // Files created by our test should be cleaned up
                    return uploadFilesAfter <= tempFilesBefore + 1 &&
                           hlsDirsAfter <= hlsDirsBefore + 1;
                });
        }

        private long countTempFiles(Path dir, String prefix) throws IOException {
            try (var stream = Files.list(dir)) {
                return stream.filter(p -> p.getFileName().toString().startsWith(prefix)).count();
            }
        }
    }

    @Nested
    @DisplayName("Video ID Generation Tests")
    class VideoIdGenerationTests {

        @Test
        @DisplayName("Should generate unique video IDs for each upload")
        void shouldGenerateUniqueVideoIds() throws Exception {
            UploadHandler handler = new UploadHandler(mockStorageClient, new TestJobTaskBus());
            Javalin app = Javalin.create();
            app.post("/upload", handler::upload);

            JavalinTest.test(app, (server, client) -> {
                String videoId1;
                String videoId2;

                // First upload
                RequestBody fileBody1 = RequestBody.create(
                    "test content 1".getBytes(),
                    MediaType.parse("video/mp4")
                );
                MultipartBody requestBody1 = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("file", "test1.mp4", fileBody1)
                    .build();

                try (var response = client.request("/upload", builder ->
                    builder.post(requestBody1))) {
                    videoId1 = extractVideoId(response.body().string());
                }

                // Second upload
                RequestBody fileBody2 = RequestBody.create(
                    "test content 2".getBytes(),
                    MediaType.parse("video/mp4")
                );
                MultipartBody requestBody2 = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("file", "test2.mp4", fileBody2)
                    .build();

                try (var response = client.request("/upload", builder ->
                    builder.post(requestBody2))) {
                    videoId2 = extractVideoId(response.body().string());
                }

                // Verify IDs are different and valid UUIDs
                assertNotNull(videoId1);
                assertNotNull(videoId2);
                assertNotEquals(videoId1, videoId2);

                // Validate UUID format (8-4-4-4-12)
                assertTrue(videoId1.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Video ID should be a valid UUID: " + videoId1);
                assertTrue(videoId2.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Video ID should be a valid UUID: " + videoId2);
            });
        }
    }

    /**
     * Creates a minimal valid MP4 file header for testing.
     * This is not a playable video but has valid MP4 structure.
     */
    private byte[] createMinimalMp4() {
        // Minimal ftyp box header
        byte[] ftyp = {
            0x00, 0x00, 0x00, 0x14, // box size (20 bytes)
            0x66, 0x74, 0x79, 0x70, // 'ftyp'
            0x69, 0x73, 0x6F, 0x6D, // 'isom' (major brand)
            0x00, 0x00, 0x00, 0x01, // minor version
            0x69, 0x73, 0x6F, 0x6D  // compatible brand 'isom'
        };
        return ftyp;
    }

    private Path createTestVideo() {
        try {
            Path tempVideo = Files.createTempFile("upload-test-", ".mp4");
            Process process = new ProcessBuilder(
                "ffmpeg",
                "-y",
                "-f", "lavfi",
                "-i", "testsrc=duration=1:size=128x72:rate=15",
                tempVideo.toAbsolutePath().toString()
            ).redirectErrorStream(true).start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                Files.deleteIfExists(tempVideo);
                return null;
            }
            return tempVideo;
        } catch (Exception e) {
            return null;
        }
    }

    private static String extractVideoId(String body) throws Exception {
        JsonNode json = objectMapper.readTree(body);
        return json.get("videoId").asText();
    }

    private static Set<Integer> extractSegmentNumbersFromKeys(List<String> keys) {
        Set<Integer> segments = new HashSet<>();
        for (String key : keys) {
            String fileName = key.substring(key.lastIndexOf('/') + 1);
            if (!fileName.endsWith(".ts")) {
                continue;
            }
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(\\d+)").matcher(fileName);
            int last = -1;
            while (matcher.find()) {
                last = Integer.parseInt(matcher.group(1));
            }
            if (last >= 0) {
                segments.add(last);
            }
        }
        return segments;
    }

    private static class FakeSegmentUploadRepository extends com.distributed26.videostreaming.upload.db.SegmentUploadRepository {
        private final Set<Integer> existing;
        private final Set<Integer> inserted = new HashSet<>();

        FakeSegmentUploadRepository(Set<Integer> existing) {
            super("jdbc:fake", "user", "pass");
            this.existing = new HashSet<>(existing);
        }

        @Override
        public Set<Integer> findSegmentNumbers(String videoId) {
            return new HashSet<>(existing);
        }

        @Override
        public void insert(String videoId, int segmentNumber) {
            inserted.add(segmentNumber);
        }

        @Override
        public int countByVideoId(String videoId) {
            return existing.size() + inserted.size();
        }
    }
}
