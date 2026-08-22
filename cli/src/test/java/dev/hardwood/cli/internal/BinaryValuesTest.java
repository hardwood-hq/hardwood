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
    void printableUtf8RendersAsTextInEitherForm() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        assertThat(BinaryValues.render(bytes, BinaryValues.Form.COMPACT)).isEqualTo("hello");
        assertThat(BinaryValues.render(bytes, BinaryValues.Form.FULL)).isEqualTo("hello");
    }

    @Test
    void nonAsciiTextStillRendersAsText() {
        byte[] bytes = "Ñuble".getBytes(StandardCharsets.UTF_8);

        assertThat(BinaryValues.render(bytes, BinaryValues.Form.COMPACT)).isEqualTo("Ñuble");
    }

    @Test
    void malformedUtf8IsBinary() {
        // Lead byte announcing a two-byte sequence, followed by a byte that
        // cannot continue one.
        byte[] bytes = {(byte) 0xC3, (byte) 0x28};

        assertThat(BinaryValues.asText(bytes)).isNull();
    }

    @Test
    void wellFormedUtf8CarryingControlCharactersIsBinary() {
        // Decodes cleanly, but a NUL in a table cell is not text a reader can
        // use — and a column holding one is far more likely to be a payload.
        byte[] bytes = {'a', 0x00, 'b'};

        assertThat(BinaryValues.asText(bytes)).isNull();
    }

    @Test
    void shortBinaryRendersAsHexEvenInACell() {
        byte[] bytes = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};

        assertThat(BinaryValues.render(bytes, BinaryValues.Form.COMPACT)).isEqualTo("0xdeadbeef");
        assertThat(BinaryValues.render(bytes, BinaryValues.Form.FULL)).isEqualTo("0xdeadbeef");
    }

    @Test
    void binaryTooLongForACellRendersAsItsByteCount() {
        assertThat(BinaryValues.render(WKB_POINT, BinaryValues.Form.COMPACT)).isEqualTo("<21 bytes>");
    }

    @Test
    void binaryTooLongForACellStillRendersInFull() {
        assertThat(BinaryValues.render(WKB_POINT, BinaryValues.Form.FULL))
                .isEqualTo("0x010100000000000000005366c0f71622f0fa1955c0");
    }

    @Test
    void theCellBudgetIsAWholeNumberOfBytes() {
        // Eight bytes fit; nine do not. Pinned so the boundary cannot drift
        // without the tables that depend on it being reconsidered.
        assertThat(BinaryValues.render(new byte[8], BinaryValues.Form.COMPACT))
                .isEqualTo("0x0000000000000000");
        assertThat(BinaryValues.render(new byte[9], BinaryValues.Form.COMPACT))
                .isEqualTo("<9 bytes>");
    }

    @Test
    void emptyRendersAsTheEmptyString() {
        assertThat(BinaryValues.render(new byte[0], BinaryValues.Form.COMPACT)).isEmpty();
        assertThat(BinaryValues.render(new byte[0], BinaryValues.Form.FULL)).isEmpty();
    }
}
