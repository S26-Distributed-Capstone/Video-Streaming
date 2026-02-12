package com.distributed26.videostreaming.upload;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


public class UploadStatusWebSocketClientTest {

    @Test
    void connectAndPrintEvents() throws Exception {
        String wsUrl = System.getProperty("wsUrl");
        String uploadId = System.getProperty("uploadId");

        if (wsUrl == null || wsUrl.isBlank() || uploadId == null || uploadId.isBlank()) {
            throw new IllegalArgumentException("Missing -DwsUrl and/or -DuploadId");
        }

        HttpClient client = HttpClient.newHttpClient();

        WebSocket ws = client.newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        System.out.println("Connected");
                        webSocket.sendText(uploadId, true);
                        WebSocket.Listener.super.onOpen(webSocket);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        System.out.println("Event: " + data);
                        return WebSocket.Listener.super.onText(webSocket, data, last);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                        System.out.println("Error: " + error.getMessage());
                        WebSocket.Listener.super.onError(webSocket, error);
                    }
                }).join();

        Thread.sleep(30_000);
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
    }
}
