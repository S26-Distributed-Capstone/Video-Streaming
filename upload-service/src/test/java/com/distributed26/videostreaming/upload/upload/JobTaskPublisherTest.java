package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.InMemoryJobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskEvent;

import io.javalin.Javalin;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class JobTaskPublisherTest {

    /*@Test
    void publishJobTasksPeriodically() throws Exception {
        JobTaskBus jobTaskBus = new InMemoryJobTaskBus();

        Javalin app = UploadServiceApplication.createApp(jobTaskBus);
        app.start(8080);

        String jobId = UUID.randomUUID().toString();
        System.out.println("JobId: " + jobId);
        Thread.sleep(5000);
        for (int i = 1; i <= 20; i++) {
            JobTaskEvent event = new JobTaskEvent(jobId, "task-" + i);
            jobTaskBus.publish(event);
            Thread.sleep(1000);
        }

        app.stop();
    }*/
}
