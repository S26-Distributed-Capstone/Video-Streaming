package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository.FailedTransitionResult;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TerminalFailureHandlerTest {

    private static final class StubVideoUploadRepository extends VideoUploadRepository {
        private FailedTransitionResult nextResult = FailedTransitionResult.UPDATED;
        private String lastVideoId;

        StubVideoUploadRepository() {
            super("jdbc:test", "user", "pass");
        }

        @Override
        public FailedTransitionResult markFailedIfProcessing(String videoId) {
            this.lastVideoId = videoId;
            return nextResult;
        }
    }

    private static final class StubStatusEventBus implements StatusEventBus {
        private int publishCount;

        @Override
        public void publish(JobEvent event) {
            publishCount++;
        }

        @Override
        public void subscribe(String jobId, JobEventListener listener) {
        }

        @Override
        public void unsubscribe(String jobId, JobEventListener listener) {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void marksVideoFailedAndReturnsNoContent() {
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        StubStatusEventBus statusEventBus = new StubStatusEventBus();
        TerminalFailureHandler handler = new TerminalFailureHandler(repository, statusEventBus, "test-machine", "test-container");
        Javalin app = Javalin.create();
        app.post("/upload/{videoId}/fail", handler::markFailed);

        String videoId = UUID.randomUUID().toString();

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.post("/upload/" + videoId + "/fail", "")) {
                assertEquals(204, response.code());
            }
        });

        assertEquals(videoId, repository.lastVideoId);
        assertEquals(1, statusEventBus.publishCount);
    }

    @Test
    void returnsNotFoundWhenVideoIdDoesNotExist() {
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        StubStatusEventBus statusEventBus = new StubStatusEventBus();
        TerminalFailureHandler handler = new TerminalFailureHandler(repository, statusEventBus, "test-machine", "test-container");
        Javalin app = Javalin.create();
        app.post("/upload/{videoId}/fail", handler::markFailed);

        String videoId = UUID.randomUUID().toString();
        repository.nextResult = FailedTransitionResult.NOT_FOUND;

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.post("/upload/" + videoId + "/fail", "")) {
                assertEquals(404, response.code());
            }
        });

        assertEquals(videoId, repository.lastVideoId);
        assertEquals(0, statusEventBus.publishCount);
    }

    @Test
    void returnsConflictWhenVideoIsAlreadyCompleted() {
        StubVideoUploadRepository repository = new StubVideoUploadRepository();
        StubStatusEventBus statusEventBus = new StubStatusEventBus();
        TerminalFailureHandler handler = new TerminalFailureHandler(repository, statusEventBus, "test-machine", "test-container");
        Javalin app = Javalin.create();
        app.post("/upload/{videoId}/fail", handler::markFailed);

        String videoId = UUID.randomUUID().toString();
        repository.nextResult = FailedTransitionResult.NOT_PROCESSING;

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.post("/upload/" + videoId + "/fail", "")) {
                assertEquals(409, response.code());
            }
        });

        assertEquals(videoId, repository.lastVideoId);
        assertEquals(0, statusEventBus.publishCount);
    }
}
