package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.javalin.Javalin;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingServiceApplicationTest {
    private static final String VIDEO_ID = "11111111-1111-1111-1111-111111111111";
    private Javalin app;
    private int port;
    private HttpClient httpClient;
    private Map<String, String> statuses;
    private Map<String, byte[]> storage;

    @BeforeEach
    void setUp() {
        statuses = new HashMap<>();
        storage = new HashMap<>();

        FakeStatusRepository statusRepository = new FakeStatusRepository(statuses);
        FakeStorageClient storageClient = new FakeStorageClient(storage);

        app = StreamingServiceApplication.createStreamingApp(storageClient, statusRepository);
        app.start(0);
        port = app.port();
        httpClient = HttpClient.newHttpClient();

        // Seed data for tests
        statuses.put(VIDEO_ID, "COMPLETED");
        storage.put(VIDEO_ID + "/chunks/output.m3u8",
            "#EXTM3U\n#EXTINF:10,\n000.ts\n".getBytes(StandardCharsets.UTF_8));
        storage.put(VIDEO_ID + "/chunks/000.ts", "segment-000".getBytes(StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    void manifestReturnsContentWhenCompleted() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/manifest"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        String body = new String(response.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("#EXTM3U"));
        assertTrue(body.contains("segment/000.ts"));
    }

    @Test
    void segmentReturnsContentWhenCompleted() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/segment/000"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        String body = new String(response.body(), StandardCharsets.UTF_8);
        assertEquals("segment-000", body);
    }

    @Test
    void returns409WhenNotCompleted() throws Exception {
        String videoId = "22222222-2222-2222-2222-222222222222";
        statuses.put(videoId, "PROCESSING");

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/manifest"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_CONFLICT, response.statusCode());
    }

    @Test
    void returns404WhenVideoNotFound() throws Exception {
        String videoId = "33333333-3333-3333-3333-333333333333";

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/manifest"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    @Test
    void returns404WhenManifestMissingFromStorage() throws Exception {
        String videoId = "44444444-4444-4444-4444-444444444444";
        statuses.put(videoId, "COMPLETED");

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/manifest"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    @Test
    void returns404WhenSegmentMissingFromStorage() throws Exception {
        String videoId = "55555555-5555-5555-5555-555555555555";
        statuses.put(videoId, "COMPLETED");

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/segment/999.ts"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    private static class FakeStatusRepository extends VideoStatusRepository {
        private final Map<String, String> statuses;

        FakeStatusRepository(Map<String, String> statuses) {
            super("jdbc:fake", "user", "pass");
            this.statuses = statuses;
        }

        @Override
        public Optional<String> findStatusByVideoId(String videoId) {
            return Optional.ofNullable(statuses.get(videoId));
        }
    }

    private static class FakeStorageClient implements ObjectStorageClient {
        private final Map<String, byte[]> storage;

        FakeStorageClient(Map<String, byte[]> storage) {
            this.storage = storage;
        }

        @Override
        public void uploadFile(String key, InputStream data, long size) {
            throw new UnsupportedOperationException("Not used in unit tests");
        }

        @Override
        public InputStream downloadFile(String key) {
            byte[] payload = storage.get(key);
            if (payload == null) {
                throw NoSuchKeyException.builder().message("Missing key: " + key).build();
            }
            return new ByteArrayInputStream(payload);
        }

        @Override
        public void deleteFile(String key) {
            storage.remove(key);
        }

        @Override
        public boolean fileExists(String key) {
            return storage.containsKey(key);
        }

        @Override
        public List<String> listFiles(String prefix) {
            return storage.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .toList();
        }

        @Override
        public void ensureBucketExists() {
            // No-op for tests.
        }
    }
}
