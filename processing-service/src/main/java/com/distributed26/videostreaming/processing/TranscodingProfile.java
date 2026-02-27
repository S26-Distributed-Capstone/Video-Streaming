package com.distributed26.videostreaming.processing;

import java.util.Objects;

public class TranscodingProfile {
    public static final TranscodingProfile LOW    = new TranscodingProfile("low",    480,    800_000);
    public static final TranscodingProfile MEDIUM = new TranscodingProfile("medium", 720,  2_500_000);
    public static final TranscodingProfile HIGH   = new TranscodingProfile("high",   1080, 5_000_000);

    private final String name;
    private final int verticalResolution;
    private final int bitrate;

    public TranscodingProfile(String name, int verticalResolution, int bitrate) {
        this.name = Objects.requireNonNull(name, "name");
        if (verticalResolution <= 0) throw new IllegalArgumentException("verticalResolution must be > 0");
        if (bitrate <= 0) throw new IllegalArgumentException("bitrate must be > 0");
        this.verticalResolution = verticalResolution;
        this.bitrate = bitrate;
    }

    public String getName() { return name; }
    public int getVerticalResolution() { return verticalResolution; }
    public int getBitrate() { return bitrate; }

    @Override
    public String toString() {
        return "TranscodingProfile{name='" + name + "', " + verticalResolution + "p, " + bitrate + "bps}";
    }
}
