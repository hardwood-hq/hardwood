/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BinaryValuesTest {

    /// A WKB `Point`, as GeoParquet 1.x stores geometry in an unannotated
    /// `BYTE_ARRAY`: a byte-order flag, a geometry type and two little-endian
    /// doubles. `0xc0` is not a legal UTF-8 lead byte, so the payload is
    /// unambiguously binary.
    private static final byte[] WKB_POINT =
            HexFormat.of().parseHex("010100000000000000005366c0f71622f0fa1955c0");

    @Test
    void printableUtf8RendersAsText() {
        assertThat(BinaryValues.render("hello".getBytes(StandardCharsets.UTF_8))).isEqualTo("hello");
    }

    @Test
    void nonAsciiTextStillRendersAsText() {
        assertThat(BinaryValues.render("Ñuble".getBytes(StandardCharsets.UTF_8))).isEqualTo("Ñuble");
    }

    @Test
    void malformedUtf8IsBinary() {
        // Lead byte announcing a two-byte sequence, followed by a byte that
        // cannot continue one.
        assertThat(BinaryValues.asText(new byte[]{(byte) 0xC3, (byte) 0x28})).isNull();
    }

    @Test
    void wellFormedUtf8CarryingControlCharactersIsBinary() {
        // Decodes cleanly, but a NUL in a table cell is not text a reader can
        // use — and a column holding one is far more likely to be a payload.
        assertThat(BinaryValues.asText(new byte[]{'a', 0x00, 'b'})).isNull();
    }

    /// Text spelled like hex would read back as the bytes it spells, so it
    /// renders as hex itself: a `0x…` value always means bytes.
    @Test
    void textSpelledLikeHexRendersAsHex() {
        assertThat(BinaryValues.render("0x0102".getBytes(StandardCharsets.UTF_8))).isEqualTo("0x307830313032");
    }

    @Test
    void binaryRendersAsPrefixedHex() {
        assertThat(BinaryValues.render(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}))
                .isEqualTo("0xdeadbeef");
    }

    /// The hex is complete however long the payload is. Fitting it to a cell is
    /// the surface's job, and every surface already caps long values.
    @Test
    void hexIsCompleteRegardlessOfLength() {
        assertThat(BinaryValues.render(WKB_POINT))
                .isEqualTo("0x010100000000000000005366c0f71622f0fa1955c0");
    }

    /// A caller with a budget gets a prefix just past what it can show, so the
    /// value still reads as over-length and the caller marks what it cut. The
    /// bytes past that point are never hexed.
    @Test
    void hexStopsJustPastTheCallersBudget() {
        String rendered = BinaryValues.render(WKB_POINT, 20);

        assertThat(rendered).isEqualTo("0x01010000000000000000");
        assertThat(rendered.length()).isGreaterThan(20);
        assertThat("0x010100000000000000005366c0f71622f0fa1955c0").startsWith(rendered);
    }

    /// A payload that fits is rendered whole and reads as complete, so nothing
    /// downstream marks it.
    @Test
    void hexWithinTheBudgetIsUntouched() {
        assertThat(BinaryValues.render(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, 20))
                .isEqualTo("0xdeadbeef");
    }

    /// The bound is a budget, not a policy: a caller that states none gets the
    /// whole value, which is what `convert` and the modals rely on.
    @Test
    void noLimitRendersTheWholePayload() {
        assertThat(BinaryValues.render(WKB_POINT, BinaryValues.NO_LIMIT))
                .isEqualTo("0x010100000000000000005366c0f71622f0fa1955c0");
    }

    /// A budget too small for even one byte still yields one. Bare `0x` would
    /// fit the width a caller actually renders at — a table widens a column to
    /// hold the marker it plans to add — and would print unmarked, reading as
    /// the whole value.
    @Test
    void aBudgetTooSmallForAByteStillYieldsOne() {
        assertThat(BinaryValues.render(WKB_POINT, 1)).isEqualTo("0x01");
        assertThat(BinaryValues.render(WKB_POINT, 0)).isEqualTo("0x01");
    }

    @Test
    void emptyRendersAsTheEmptyString() {
        assertThat(BinaryValues.render(new byte[0])).isEmpty();
    }
}
