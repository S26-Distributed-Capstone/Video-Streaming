package com.distributed26.videostreaming.shared.upload.events;

import java.util.Objects;

public class TranscodeProgressEvent extends JobEvent {
    private final String type = "transcode_progress";
    private final String profile;
    private final int segmentNumber;
    private final TranscodeSegmentState state;
    private final int doneSegments;
    private final int totalSegments;

    public TranscodeProgressEvent(
            String jobId,
            String profile,
            int segmentNumber,
            TranscodeSegmentState state,
            int doneSegments,
            int totalSegments
    ) {
        super(jobId, "transcode:" + Objects.requireNonNull(profile, "profile is null") + ":" + segmentNumber);
        this.profile = profile;
        this.segmentNumber = segmentNumber;
        this.state = Objects.requireNonNull(state, "state is null");
        this.doneSegments = doneSegments;
        this.totalSegments = totalSegments;
    }

    public String getType() {
        return type;
    }

    public String getProfile() {
        return profile;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    public TranscodeSegmentState getState() {
        return state;
    }

    public int getDoneSegments() {
        return doneSegments;
    }

    public int getTotalSegments() {
        return totalSegments;
    }
}
