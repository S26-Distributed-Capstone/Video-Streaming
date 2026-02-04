package com.distributed26.videostreaming.upload;

import io.javalin.http.Context;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class HealthHandler {
    public static void health(Context ctx){
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("status", "UP");
        payload.put("service", "upload-service");
        payload.put("timestamp", Instant.now().toString());
        ctx.json(payload);
    }
}
