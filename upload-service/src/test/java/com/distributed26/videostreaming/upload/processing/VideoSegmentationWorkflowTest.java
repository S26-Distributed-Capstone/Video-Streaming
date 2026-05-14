package com.distributed26.videostreaming.upload.processing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class VideoSegmentationWorkflowTest {

    @Test
    void runCommandWithDiagnosticsIncludesStderrInFailureMessage() {
        IOException error = org.junit.jupiter.api.Assertions.assertThrows(
                IOException.class,
                () -> VideoSegmentationWorkflow.runCommandWithDiagnostics(
                        List.of("sh", "-c", "echo 'segmentation boom' >&2; exit 7"),
                        "videoId=test-video input=test.mp4"
                )
        );

        assertTrue(error.getMessage().contains("exit code 7"));
        assertTrue(error.getMessage().contains("segmentation boom"));
        assertTrue(error.getMessage().contains("videoId=test-video"));
    }

    @Test
    void runCommandWithDiagnosticsAllowsSuccessfulCommandWithStderrOutput() {
        assertDoesNotThrow(() -> VideoSegmentationWorkflow.runCommandWithDiagnostics(
                List.of("sh", "-c", "echo 'benign stderr' >&2; exit 0"),
                "videoId=test-video input=test.mp4"
        ));
    }
}

