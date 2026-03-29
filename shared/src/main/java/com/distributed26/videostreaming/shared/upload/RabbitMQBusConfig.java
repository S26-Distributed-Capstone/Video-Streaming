package com.distributed26.videostreaming.shared.upload;

import io.github.cdimascio.dotenv.Dotenv;

record RabbitMQBusConfig(
        String host,
        int port,
        String username,
        String password,
        String vhost,
        String exchange,
        String statusQueue,
        String statusBinding,
        String failureBinding,
        String taskQueue,
        String taskBinding,
        String devLogQueue,
        String devLogBinding
) {
    static RabbitMQBusConfig fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        return new RabbitMQBusConfig(
                getEnvOrDotenv(dotenv, "RABBITMQ_HOST", "localhost"),
                Integer.parseInt(getEnvOrDotenv(dotenv, "RABBITMQ_PORT", "5672")),
                getEnvOrDotenv(dotenv, "RABBITMQ_USER", "guest"),
                getEnvOrDotenv(dotenv, "RABBITMQ_PASS", "guest"),
                getEnvOrDotenv(dotenv, "RABBITMQ_VHOST", "/"),
                getEnvOrDotenv(dotenv, "RABBITMQ_EXCHANGE", "upload.events"),
                getEnvOrDotenv(dotenv, "RABBITMQ_STATUS_QUEUE", "upload.status.queue"),
                getEnvOrDotenv(dotenv, "RABBITMQ_STATUS_BINDING", "upload.status.*"),
                getEnvOrDotenv(dotenv, "RABBITMQ_FAILURE_BINDING", "upload.failure"),
                getEnvOrDotenv(dotenv, "RABBITMQ_TASK_QUEUE", "processing.tasks.queue"),
                getEnvOrDotenv(dotenv, "RABBITMQ_TASK_BINDING", "upload.task.transcode"),
                getEnvOrDotenv(dotenv, "RABBITMQ_DEV_LOG_QUEUE", "dev.logs.queue"),
                getEnvOrDotenv(dotenv, "RABBITMQ_DEV_LOG_BINDING", "dev.log")
        );
    }

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        String dotenvVal = dotenv.get(key);
        if (dotenvVal == null || dotenvVal.isBlank()) {
            return defaultValue;
        }
        return dotenvVal;
    }
}
