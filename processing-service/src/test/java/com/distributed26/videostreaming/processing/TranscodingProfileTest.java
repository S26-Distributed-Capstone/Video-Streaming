package com.distributed26.videostreaming.processing;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TranscodingProfileTest {

    // ── Static constants ───────────────────────────────────────────────────────

    @Test
    void low_hasExpectedValues() {
        assertEquals("low", TranscodingProfile.LOW.getName());
        assertEquals(480, TranscodingProfile.LOW.getVerticalResolution());
        assertEquals(800_000, TranscodingProfile.LOW.getBitrate());
    }

    @Test
    void medium_hasExpectedValues() {
        assertEquals("medium", TranscodingProfile.MEDIUM.getName());
        assertEquals(720, TranscodingProfile.MEDIUM.getVerticalResolution());
        assertEquals(2_500_000, TranscodingProfile.MEDIUM.getBitrate());
    }

    @Test
    void high_hasExpectedValues() {
        assertEquals("high", TranscodingProfile.HIGH.getName());
        assertEquals(1080, TranscodingProfile.HIGH.getVerticalResolution());
        assertEquals(5_000_000, TranscodingProfile.HIGH.getBitrate());
    }

    @Test
    void constants_areSingletons() {
        assertSame(TranscodingProfile.LOW, TranscodingProfile.LOW);
        assertSame(TranscodingProfile.MEDIUM, TranscodingProfile.MEDIUM);
        assertSame(TranscodingProfile.HIGH, TranscodingProfile.HIGH);
    }

    // ── Constructor validation ─────────────────────────────────────────────────

    @Test
    void constructor_nullName_throws() {
        assertThrows(NullPointerException.class, () -> new TranscodingProfile(null, 480, 800_000));
    }

    @Test
    void constructor_zeroResolution_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TranscodingProfile("x", 0, 800_000));
    }

    @Test
    void constructor_negativeResolution_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TranscodingProfile("x", -1, 800_000));
    }

    @Test
    void constructor_zeroBitrate_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TranscodingProfile("x", 480, 0));
    }

    @Test
    void constructor_negativeBitrate_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TranscodingProfile("x", 480, -1));
    }

    @Test
    void toString_containsAllFields() {
        String s = TranscodingProfile.MEDIUM.toString();
        assertTrue(s.contains("medium"));
        assertTrue(s.contains("720"));
        assertTrue(s.contains("2500000"));
    }
}
