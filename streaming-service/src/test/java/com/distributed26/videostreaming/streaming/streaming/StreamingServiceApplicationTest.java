package com.distributed26.videostreaming.streaming.streaming;

import com.distributed26.videostreaming.shared.storage.ObjectStorageClient;
import com.distributed26.videostreaming.streaming.db.VideoStatusRepository;
import io.javalin.Javalin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        storage.put(VIDEO_ID + "/manifest/master.m3u8",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nlow/playlist.m3u8\n".getBytes(StandardCharsets.UTF_8));
        storage.put(VIDEO_ID + "/manifest/low.m3u8",
            "#EXTM3U\n#EXTINF:10,\n000.ts\n".getBytes(StandardCharsets.UTF_8));
        storage.put(VIDEO_ID + "/processed/low/000.ts", "segment-000".getBytes(StandardCharsets.UTF_8));
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
        assertTrue(body.contains("variant/low/playlist.m3u8"));
    }

    @Test
    void variantPlaylistContainsProxySegmentUrls() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/variant/low/playlist.m3u8"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        String body = new String(response.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("#EXTM3U"));
        assertTrue(body.contains("/stream/" + VIDEO_ID + "/segment/low/000.ts"),
                "Expected proxy segment URLs, got: " + body);
    }

    @Test
    void segmentEndpointRedirectsWithPresignedUrl() throws Exception {
        HttpClient noRedirectClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/segment/low/000.ts"))
            .GET()
            .build();

        HttpResponse<byte[]> response = noRedirectClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, response.statusCode());
        String location = response.headers().firstValue("Location").orElse("");
        assertTrue(location.contains("presigned://"), "Expected presigned URL in Location header, got: " + location);
        assertTrue(location.contains(VIDEO_ID + "/processed/low/000.ts"));
    }

    @Test
    void segmentEndpointRejectsInvalidSegmentName() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/segment/low/../../../etc/passwd"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        // Invalid segment names (containing path traversal or lacking .ts) must be rejected
        assertTrue(response.statusCode() == HttpURLConnection.HTTP_BAD_REQUEST
                        || response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND,
                "Expected 400 or 404 for invalid segment name, got: " + response.statusCode());
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
    void returns404WhenVariantManifestMissingFromStorage() throws Exception {
        String videoId = "55555555-5555-5555-5555-555555555555";
        statuses.put(videoId, "COMPLETED");

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/stream/" + videoId + "/variant/low/playlist.m3u8"))
            .GET()
            .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    @Test
    void clientCanRetryManifestRequestAgainstHealthyReplicaAfterConnectionDrops() throws Exception {
        try (DroppingManifestServer failedReplica = new DroppingManifestServer()) {
            failedReplica.start();

            HttpRequest firstAttempt = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + failedReplica.port() + "/stream/" + VIDEO_ID + "/manifest"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();

            assertThrows(IOException.class,
                    () -> httpClient.send(firstAttempt, HttpResponse.BodyHandlers.ofByteArray()),
                    "first replica should drop the manifest response mid-request");

            failedReplica.awaitServedRequest();

            HttpRequest retry = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/stream/" + VIDEO_ID + "/manifest"))
                .GET()
                .build();

            HttpResponse<byte[]> response = httpClient.send(retry, HttpResponse.BodyHandlers.ofByteArray());

            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            String body = new String(response.body(), StandardCharsets.UTF_8);
            assertTrue(body.contains("#EXTM3U"));
            assertTrue(body.contains("variant/low/playlist.m3u8"));
        }
    }

    @Test
    void clientGivesUpAfterManifestRetriesWhenAllReplicasAreUnavailable() throws Exception {
        try (RejectingReplica unavailableReplicaOne = new RejectingReplica();
             RejectingReplica unavailableReplicaTwo = new RejectingReplica()) {
            HttpClient retryingClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .build();
            AtomicInteger attempts = new AtomicInteger();

            URI[] replicas = {
                URI.create("http://localhost:" + unavailableReplicaOne.port() + "/stream/" + VIDEO_ID + "/manifest"),
                URI.create("http://localhost:" + unavailableReplicaTwo.port() + "/stream/" + VIDEO_ID + "/manifest"),
                URI.create("http://localhost:" + unavailableReplicaOne.port() + "/stream/" + VIDEO_ID + "/manifest")
            };

            assertThrows(IOException.class,
                    () -> fetchManifestWithRetries(retryingClient, replicas, attempts),
                    "client should give up after retrying unavailable replicas");
            assertEquals(replicas.length, attempts.get(),
                    "client should exhaust each configured manifest retry before giving up");
        }
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

        @Override
        public List<ReadyVideoRecord> findCompletedVideos(int limit) {
            return statuses.entrySet().stream()
                .filter(entry -> "COMPLETED".equalsIgnoreCase(entry.getValue()))
                .limit(limit)
                .map(entry -> new ReadyVideoRecord(entry.getKey(), entry.getKey()))
                .toList();
        }

        @Override
        public boolean deleteByVideoId(String videoId) {
            return statuses.remove(videoId) != null;
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

        @Override
        public String generatePresignedUrl(String key, long durationSeconds) {
            return "presigned://" + key + "?duration=" + durationSeconds;
        }
    }

    private static byte[] fetchManifestWithRetries(HttpClient client, URI[] replicas, AtomicInteger attempts)
            throws IOException, InterruptedException {
        IOException lastFailure = null;
        for (URI replica : replicas) {
            attempts.incrementAndGet();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(replica)
                .timeout(Duration.ofSeconds(1))
                .GET()
                .build();
            try {
                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    return response.body();
                }
                lastFailure = new IOException("Unexpected manifest status " + response.statusCode() + " from " + replica);
            } catch (IOException e) {
                lastFailure = e;
            }
        }
        throw lastFailure != null ? lastFailure : new IOException("No replicas were attempted");
    }

    private static final class DroppingManifestServer implements AutoCloseable {
        private final ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final CountDownLatch servedRequest = new CountDownLatch(1);

        private DroppingManifestServer() throws IOException {
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void start() {
            executor.submit(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout(2_000);
                    readRequestHeaders(socket.getInputStream());
                    byte[] partialBody = "#EXTM3U\n".getBytes(StandardCharsets.UTF_8);
                    byte[] headers = (
                            "HTTP/1.1 200 OK\r\n"
                                    + "Content-Type: application/vnd.apple.mpegurl\r\n"
                                    + "Content-Length: 64\r\n"
                                    + "Connection: close\r\n"
                                    + "\r\n")
                            .getBytes(StandardCharsets.UTF_8);
                    OutputStream out = socket.getOutputStream();
                    out.write(headers);
                    out.write(partialBody);
                    out.flush();
                    servedRequest.countDown();
                } catch (IOException ignored) {
                    servedRequest.countDown();
                }
            });
        }

        private void readRequestHeaders(InputStream inputStream) throws IOException {
            int matched = 0;
            while (matched < 4) {
                int next = inputStream.read();
                if (next == -1) {
                    return;
                }
                matched = switch (matched) {
                    case 0 -> next == '\r' ? 1 : 0;
                    case 1 -> next == '\n' ? 2 : next == '\r' ? 1 : 0;
                    case 2 -> next == '\r' ? 3 : 0;
                    case 3 -> next == '\n' ? 4 : next == '\r' ? 1 : 0;
                    default -> matched;
                };
            }
        }

        void awaitServedRequest() throws InterruptedException {
            assertTrue(servedRequest.await(5, TimeUnit.SECONDS),
                    "dropping replica should receive the manifest request");
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                    "dropping manifest server should shut down promptly");
        }
    }

    private static final class RejectingReplica implements AutoCloseable {
        private final ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        private RejectingReplica() throws IOException {
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                    try (Socket socket = serverSocket.accept()) {
                        socket.close();
                    } catch (IOException ignored) {
                        if (serverSocket.isClosed()) {
                            return;
                        }
                    }
                }
            });
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                    "rejecting replica should shut down promptly");
        }
    }
}
