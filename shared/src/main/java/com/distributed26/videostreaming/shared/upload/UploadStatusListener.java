package com.distributed26.videostreaming.shared.upload;

@FunctionalInterface
public interface UploadStatusListener {
    void onStatus(UploadStatusEvent event);
}
