/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DictionaryTest {

    @Test
    void parseByteArrayDictionary() throws IOException {
        byte[] data = {
            2, 0, 0, 0, 'A', 'B',       // len=2, "AB"
            3, 0, 0, 0, 'X', 'Y', 'Z'   // len=3, "XYZ"
        };
        Dictionary dict = Dictionary.parse(data, data.length, 2, PhysicalType.BYTE_ARRAY, null);
        assertThat(dict.size()).isEqualTo(2);
    }

    /// Reproduces the "Unexpected EOF while reading BYTE_ARRAY data" bug that
    /// occurred with the Overture Maps places file. The last dictionary entry had
    /// length 0 (empty string). `ByteArrayInputStream.read(byte[0])` returns -1 at
    /// stream EOF instead of 0, causing the EOF check to trip.
    @Test
    void parseByteArrayDictionaryWithEmptyLastEntry() throws IOException {
        byte[] data = {
            2, 0, 0, 0, 'S', 'P',     // len=2, "SP"
            2, 0, 0, 0, 'G', 'O',     // len=2, "GO"
            0, 0, 0, 0                  // len=0, ""
        };
        Dictionary dict = Dictionary.parse(data, data.length, 3, PhysicalType.BYTE_ARRAY, null);
        assertThat(dict.size()).isEqualTo(3);
    }

    @Test
    void parseByteArrayDictionaryKeepsEveryEntry() {
        byte[] data = {
            2, 0, 0, 0, 'A', 'B',
            0, 0, 0, 0,
            3, 0, 0, 0, 'X', 'Y', 'Z'
        };

        Dictionary.ByteArrayDictionary dict =
                (Dictionary.ByteArrayDictionary) Dictionary.parse(data, data.length, 3, PhysicalType.BYTE_ARRAY, null);

        assertThat(dict.entry(0)).containsExactly('A', 'B');
        assertThat(dict.entry(1)).isEmpty();
        assertThat(dict.entry(2)).containsExactly('X', 'Y', 'Z');
        assertThat(dict.entryOffsets()).containsExactly(0, 2, 2, 5);
    }

    @Test
    void parseByteArrayDictionaryReadsOnlyThePagesLength() {
        byte[] data = { 1, 0, 0, 0, 'A', 9, 9, 9 };

        Dictionary.ByteArrayDictionary dict =
                (Dictionary.ByteArrayDictionary) Dictionary.parse(data, 5, 1, PhysicalType.BYTE_ARRAY, null);

        assertThat(dict.entry(0)).containsExactly('A');
    }

    @Test
    void rejectsByteArrayDictionaryTooShortForItsLengthPrefixes() {
        byte[] data = { 1, 0, 0, 0, 'A' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.BYTE_ARRAY, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BYTE_ARRAY length");
    }

    @Test
    void rejectsByteArrayDictionaryEntryReachingIntoLaterLengthPrefixes() {
        byte[] data = { 1, 0, 0, 0, 'A', 2, 0, 0 };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.BYTE_ARRAY, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BYTE_ARRAY length");
    }

    @Test
    void rejectsByteArrayDictionaryWithNegativeLength() {
        byte[] data = { -1, -1, -1, -1, 'A', 'B' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 1, PhysicalType.BYTE_ARRAY, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid BYTE_ARRAY length: -1");
    }

    @Test
    void rejectsByteArrayDictionaryWithEntryPastThePagesEnd() {
        byte[] data = { 5, 0, 0, 0, 'A', 'B' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 1, PhysicalType.BYTE_ARRAY, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading BYTE_ARRAY data");
    }

    @Test
    void parseInt96Dictionary() {
        byte[] data = new byte[24];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        Dictionary.ByteArrayDictionary dict =
                (Dictionary.ByteArrayDictionary) Dictionary.parse(data, data.length, 2, PhysicalType.INT96, null);

        assertThat(dict.entry(0)).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        assertThat(dict.entry(1)).containsExactly(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23);
    }

    @Test
    void rejectsInt96DictionaryTooShortForItsEntries() {
        byte[] data = new byte[20];

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.INT96, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading INT96");
    }

    @Test
    void rejectsFixedLengthDictionaryTooShortForItsEntries() {
        byte[] data = { 'A', 'B', 'C', 'D', 'E' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.FIXED_LEN_BYTE_ARRAY, 4))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading fixed-length byte array");
    }

    @Test
    void rejectsFixedLengthDictionaryWithoutTypeLength() {
        byte[] data = { 'A', 'B', 'C', 'D' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 1, PhysicalType.FIXED_LEN_BYTE_ARRAY, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY requires a positive type length, got null");
    }

    @Test
    void rejectsFixedLengthDictionaryWithNonPositiveTypeLength() {
        byte[] data = { 'A', 'B', 'C', 'D' };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.FIXED_LEN_BYTE_ARRAY,
                Integer.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY requires a positive type length, got -2147483648");
    }

    @Test
    void parseIntDictionary() throws IOException {
        byte[] data = {
            10, 0, 0, 0,
            20, 0, 0, 0,
            30, 0, 0, 0
        };
        Dictionary dict = Dictionary.parse(data, data.length, 3, PhysicalType.INT32, null);
        assertThat(dict.size()).isEqualTo(3);
    }

    /// A FIXED_LEN_BYTE_ARRAY(4) dictionary written with BYTE_ARRAY length prefixes, as arrow-rs
    /// did before apache/arrow-rs#11262: three entries in 24 bytes instead of 12.
    @Test
    void rejectsFixedLengthDictionaryWithLengthPrefixes() {
        byte[] data = {
            4, 0, 0, 0, 'A', 'B', 'C', 'D',
            4, 0, 0, 0, 'W', 'X', 'Y', 'Z',
            4, 0, 0, 0, 'w', 'x', 'y', 'z'
        };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 3, PhysicalType.FIXED_LEN_BYTE_ARRAY, 4))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page does not end with its 3 FIXED_LEN_BYTE_ARRAY values; leftover bytes: 12");
    }

    @Test
    void rejectsIntDictionaryWithTrailingBytes() {
        byte[] data = { 10, 0, 0, 0, 20, 0, 0, 0, 30 };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 2, PhysicalType.INT32, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page does not end with its 2 INT32 values; leftover bytes: 1");
    }

    @Test
    void rejectsByteArrayDictionaryWithTrailingBytes() {
        byte[] data = { 2, 0, 0, 0, 'A', 'B', 0 };

        assertThatThrownBy(() -> Dictionary.parse(data, data.length, 1, PhysicalType.BYTE_ARRAY, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page does not end with its 1 BYTE_ARRAY values; leftover bytes: 1");
    }

    @Test
    void parseFixedLengthDictionary() throws IOException {
        byte[] data = { 4, 0, 0, 0, 'A', 'B', 'C', 'D' };

        Dictionary dict = Dictionary.parse(data, data.length, 2, PhysicalType.FIXED_LEN_BYTE_ARRAY, 4);

        // An entry may look like a length prefix; only the section's size tells the layouts apart
        Dictionary.ByteArrayDictionary byteArrays = (Dictionary.ByteArrayDictionary) dict;
        assertThat(byteArrays.size()).isEqualTo(2);
        assertThat(byteArrays.entry(0)).containsExactly(4, 0, 0, 0);
        assertThat(byteArrays.entry(1)).containsExactly('A', 'B', 'C', 'D');
    }
}
