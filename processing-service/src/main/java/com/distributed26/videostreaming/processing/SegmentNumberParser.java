package com.distributed26.videostreaming.processing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SegmentNumberParser {
    private static final Pattern LAST_DIGIT_PATTERN = Pattern.compile("(\\d+)(?!.*\\d)");

    private SegmentNumberParser() {}

    static int parse(String value) {
        if (value == null || value.isBlank()) {
            return -1;
        }
        Matcher matcher = LAST_DIGIT_PATTERN.matcher(value);
        if (!matcher.find()) {
            return -1;
        }
        return Integer.parseInt(matcher.group(1));
    }
}
