package com.distributed26.videostreaming.shared.upload;

public interface StatusBus {
    void publish(UploadStatusEvent event);

    void subscribe(String uploadId, UploadStatusListener listener);

    void unsubscribe(String uploadId, UploadStatusListener listener);
}
