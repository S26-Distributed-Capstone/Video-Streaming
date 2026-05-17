package com.distributed26.videostreaming.shared.upload.events;

import java.util.Objects;

public class SourceChunkRepairEvent extends JobEvent {
    private final String type = "source_chunk_repair";
    private final int segmentNumber;
    private final String state;
    private final String preset;
    private final String message;

    public SourceChunkRepairEvent(String jobId, int segmentNumber, String state, String preset, String message) {
        super(jobId, "source-repair:" + segmentNumber);
        this.segmentNumber = segmentNumber;
        this.state = Objects.requireNonNull(state, "state is null");
        this.preset = Objects.requireNonNull(preset, "preset is null");
        this.message = Objects.requireNonNull(message, "message is null");
    }

    public String getType() {
        return type;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    public String getState() {
        return state;
    }

    public String getPreset() {
        return preset;
    }

    public String getMessage() {
        return message;
    }
}