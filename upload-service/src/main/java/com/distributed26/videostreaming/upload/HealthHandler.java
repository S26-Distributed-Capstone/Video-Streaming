package com.distributed26.videostreaming.upload;

import io.javalin.http.Context;

public class HealthHandler {
    public static void health(Context ctx){
        ctx.result("healthy!");
    }
}
