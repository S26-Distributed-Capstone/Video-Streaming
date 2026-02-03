package com.distributed26.videostreaming.upload;

import io.javalin.Javalin;

/**
 * Hello world!
 *
 */
public class UploadWholeVideo {

	private static final int DEFAULT_PORT = 7000;
	public static void main(String[] args) {
		Javalin app = Javalin.create();
		app.get("/health", HealthHandler::health);
		app.start(DEFAULT_PORT);

	}


}
