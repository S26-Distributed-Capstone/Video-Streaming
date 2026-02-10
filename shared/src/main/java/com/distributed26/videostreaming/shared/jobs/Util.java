package com.distributed26.videostreaming.shared.jobs;

import java.util.HashMap;
import java.util.Map;

final class Util {
    private Util() {}

    static Map<String, String> validateAndCopy(Map<String, String> metadata) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("metadata key is null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("metadata value is null");
            }
        }
        return new HashMap<>(metadata);
    }
}
