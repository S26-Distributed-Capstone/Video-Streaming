package com.distributed26.videostreaming.shared.upload.events;

public enum TranscodeSegmentState {
    QUEUED,
    TRANSCODING,
    UPLOADING,
    DONE,
    FAILED
}
