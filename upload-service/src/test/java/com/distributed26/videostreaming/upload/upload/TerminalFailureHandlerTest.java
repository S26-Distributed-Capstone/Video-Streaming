package com.distributed26.videostreaming.upload.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TerminalFailureHandlerTest {

    @Test
    void marksVideoFailedAndReturnsNoContent() {
        VideoUploadRepository repository = mock(VideoUploadRepository.class);
        StatusEventBus statusEventBus = mock(StatusEventBus.class);
        TerminalFailureHandler handler = new TerminalFailureHandler(repository, statusEventBus, "test-machine", "test-container");
        Javalin app = Javalin.create();
        app.post("/upload/{videoId}/fail", handler::markFailed);

        String videoId = UUID.randomUUID().toString();

        JavalinTest.test(app, (server, client) -> {
            try (var response = client.post("/upload/" + videoId + "/fail", "")) {
                assertEquals(204, response.code());
            }
        });

        verify(repository).updateStatus(videoId, "FAILED");
        verify(statusEventBus).publish(org.mockito.ArgumentMatchers.any());
    }
}
