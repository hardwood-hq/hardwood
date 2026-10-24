/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What the `PLAIN` readers ([FlatPageValues#plain], [BinaryPageValues#plain]) do with a data page
/// whose value section is not the size its values take, read through [PageDecoder]'s entry points.
/// A section that runs out is an EOF; one with bytes left over would otherwise decode into values
/// read at the wrong boundaries, so it is refused as well.
class PageDecoderPlainSizeTest {

    private static final int FIXED_LENGTH = 4;

    /// Two FIXED_LEN_BYTE_ARRAY(4) values written with BYTE_ARRAY length prefixes, as arrow-rs
    /// did before apache/arrow-rs#11262.
    @Test
    void rejectsFixedLengthValuesWithLengthPrefixes() {
        byte[] body = { 4, 0, 0, 0, 'A', 'B', 'C', 'D', 4, 0, 0, 0, 'w', 'x', 'y', 'z' };

        assertThatThrownBy(() -> decode(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PLAIN value section does not end with its 2 FIXED_LEN_BYTE_ARRAY values; leftover bytes: 8");
    }

    @Test
    void rejectsIntValuesWithTrailingBytes() {
        byte[] body = { 1, 0, 0, 0, 2, 0, 0, 0, 3 };

        assertThatThrownBy(() -> decode(PhysicalType.INT32, 2, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PLAIN value section does not end with its 2 INT32 values; leftover bytes: 1");
    }

    @Test
    void rejectsBooleanValuesWithTrailingBytes() {
        // Nine booleans take two bytes; the third is left over
        byte[] body = { (byte) 0xFF, 1, 0 };

        assertThatThrownBy(() -> decode(PhysicalType.BOOLEAN, 9, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PLAIN value section does not end with its 9 BOOLEAN values; leftover bytes: 1");
    }

    @Test
    void rejectsFixedLengthValuesThatRunOut() {
        byte[] body = { 'A', 'B', 'C', 'D', 'w', 'x', 'y' };

        assertThatThrownBy(() -> decode(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading fixed-length byte array");
    }

    @Test
    void rejectsIntValuesThatRunOut() {
        byte[] body = { 1, 0, 0, 0, 2, 0, 0 };

        assertThatThrownBy(() -> decode(PhysicalType.INT32, 2, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading INT32 values");
    }

    @Test
    void rejectsBooleanValuesThatRunOut() {
        // Nine booleans take two bytes
        byte[] body = { (byte) 0xFF };

        assertThatThrownBy(() -> decode(PhysicalType.BOOLEAN, 9, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BOOLEAN values");
    }

    @Test
    void rejectsInt96ValuesThatRunOut() {
        assertThatThrownBy(() -> decode(PhysicalType.INT96, 1, new byte[11]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading INT96");
    }

    @Test
    void rejectsAByteArrayLengthPrefixThatRunsOut() {
        byte[] body = { 1, 0, 0, 0, 'a', 1, 0 };

        assertThatThrownBy(() -> decode(PhysicalType.BYTE_ARRAY, 2, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BYTE_ARRAY length");
    }

    @Test
    void rejectsANegativeByteArrayLength() {
        byte[] body = { -1, -1, -1, -1, 'a' };

        assertThatThrownBy(() -> decode(PhysicalType.BYTE_ARRAY, 1, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid BYTE_ARRAY length: -1");
    }

    @Test
    void rejectsByteArrayDataThatRunsOut() {
        byte[] body = { 5, 0, 0, 0, 'a' };

        assertThatThrownBy(() -> decode(PhysicalType.BYTE_ARRAY, 1, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BYTE_ARRAY data");
    }

    @Test
    void rejectsByteArrayValuesWithTrailingBytes() {
        byte[] body = { 1, 0, 0, 0, 'a', 9 };

        assertThatThrownBy(() -> decode(PhysicalType.BYTE_ARRAY, 1, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PLAIN value section does not end with its 1 BYTE_ARRAY values; leftover bytes: 1");
    }

    @Test
    void decodesFixedLengthValuesFillingTheSection() {
        // The first value looks like a length prefix; only the section's size tells the layouts apart
        byte[] body = { 4, 0, 0, 0, 'A', 'B', 'C', 'D' };

        byte[][] values = (byte[][]) decode(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, body);
        assertThat(values[0]).containsExactly(4, 0, 0, 0);
        assertThat(values[1]).containsExactly('A', 'B', 'C', 'D');
    }

    @Test
    void decodesBooleansPaddedToAByte() {
        byte[] body = { 0b0000_0101 };

        assertThat((boolean[]) decode(PhysicalType.BOOLEAN, 3, body)).containsExactly(true, false, true);
    }

    private static Object decode(PhysicalType type, int numValues, byte[] body) {
        return DataPages.readRequired(Encoding.PLAIN, type,
                type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? FIXED_LENGTH : null, numValues, body);
    }
}
