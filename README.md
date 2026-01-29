# Video Upload/Processing/Playback System
> Distributed Systems Spring 2026 Capstone Project

## Code Structure (Example File Names)
```
video-streaming/
├── README.md
├── pom.xml
│
├── shared/
│   ├── pom.xml
│   └── src/main/java/com/distributed26/videostreaming/shared/
│       ├── model/
│       │   ├── Video.java
│       │   ├── VideoId.java
│       │   ├── VideoState.java (enum)
│       │   ├── ProcessingStatus.java
│       │   └── StreamingMetadata.java
│       ├── storage/
│       │   ├── ObjectStorageClient.java (interface)
│       │   └── S3StorageClient.java (implementation)
│       ├── config/
│       │   └── StorageConfig.java
│       └── util/
│           ├── VideoIdGenerator.java
│           └── RetryUtil.java
│
├── upload-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/
│       │   ├── java/com/distributed26/videostreaming/upload/
│       │   │   ├── UploadServiceApplication.java
│       │   │   ├── controller/
│       │   │   │   ├── UploadController.java
│       │   │   │   ├── HealthController.java
│       │   │   │   └── StatusController.java
│       │   │   ├── service/
│       │   │   │   ├── UploadService.java
│       │   │   │   ├── VideoStateService.java
│       │   │   │   └── DuplicateDetectionService.java
│       │   │   ├── repository/
│       │   │   │   └── VideoMetadataRepository.java
│       │   │   ├── model/
│       │   │   │   ├── UploadRequest.java
│       │   │   │   └── UploadResponse.java
│       │   │   └── config/
│       │   │       └── UploadServiceConfig.java
│       │   └── resources/
│       │       ├── application.yml
│       │       └── logback-spring.xml
│       └── test/
│
├── processing-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/
│       │   ├── java/com/distributed26/videostreaming/processing/
│       │   │   ├── ProcessingServiceApplication.java
│       │   │   ├── worker/
│       │   │   │   ├── VideoProcessingWorker.java
│       │   │   │   └── ProcessingCoordinator.java
│       │   │   ├── segmentation/
│       │   │   │   ├── VideoSegmenter.java
│       │   │   │   └── SegmentationStrategy.java
│       │   │   ├── transcoding/
│       │   │   │   ├── VideoTranscoder.java
│       │   │   │   └── BitrateProfile.java
│       │   │   ├── service/
│       │   │   │   ├── ProcessingService.java
│       │   │   │   ├── IdempotencyService.java
│       │   │   │   └── RecoveryService.java
│       │   │   ├── repository/
│       │   │   │   └── ProcessingStateRepository.java
│       │   │   └── config/
│       │   │       └── ProcessingServiceConfig.java
│       │   └── resources/
│       │       ├── application.yml
│       │       └── logback-spring.xml
│       └── test/
│
├── streaming-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/
│       │   ├── java/com/distributed26/videostreaming/streaming/
│       │   │   ├── StreamingServiceApplication.java
│       │   │   ├── controller/
│       │   │   │   ├── StreamingController.java
│       │   │   │   ├── ManifestController.java
│       │   │   │   └── HealthController.java
│       │   │   ├── service/
│       │   │   │   ├── StreamingService.java
│       │   │   │   ├── ManifestService.java
│       │   │   │   └── AdaptiveBitrateService.java
│       │   │   ├── cache/
│       │   │   │   └── SegmentCache.java
│       │   │   └── config/
│       │   │       └── StreamingServiceConfig.java
│       │   └── resources/
│       │       ├── application.yml
│       │       └── logback-spring.xml
│       └── test/
│
├── docs/
│   ├── week-01-scope.md
│   ├── week-02-api-contract.md
│   ├── week-04-lifecycle-model.md
│   ├── architecture-diagrams/
│   └── final-design.md
│
├── scripts/
│   ├── start-all.sh
│   ├── stop-all.sh
│   └── setup-minio.sh
│
└── docker-compose.yml
```