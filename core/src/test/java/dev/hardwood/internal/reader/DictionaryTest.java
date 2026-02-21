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

import static org.assertj.core.api.Assertions.assertThat;

class DictionaryTest {

    @Test
    void parseByteArrayDictionary() throws IOException {
        byte[] data = {
            2, 0, 0, 0, 'A', 'B',       // len=2, "AB"
            3, 0, 0, 0, 'X', 'Y', 'Z'   // len=3, "XYZ"
        };
        Dictionary dict = Dictionary.parse(data, 2, PhysicalType.BYTE_ARRAY, null);
        assertThat(dict.size()).isEqualTo(2);
    }

    /**
     * Reproduces the "Unexpected EOF while reading BYTE_ARRAY data" bug that
     * occurred with the Overture Maps places file. The last dictionary entry had
     * length 0 (empty string). ByteArrayInputStream.read(byte[0]) returns -1 at
     * stream EOF instead of 0, causing the EOF check to trip.
     */
    @Test
    void parseByteArrayDictionaryWithEmptyLastEntry() throws IOException {
        byte[] data = {
            2, 0, 0, 0, 'S', 'P',     // len=2, "SP"
            2, 0, 0, 0, 'G', 'O',     // len=2, "GO"
            0, 0, 0, 0                  // len=0, ""
        };
        Dictionary dict = Dictionary.parse(data, 3, PhysicalType.BYTE_ARRAY, null);
        assertThat(dict.size()).isEqualTo(3);
    }

    @Test
    void parseIntDictionary() throws IOException {
        byte[] data = {
            10, 0, 0, 0,
            20, 0, 0, 0,
            30, 0, 0, 0
        };
        Dictionary dict = Dictionary.parse(data, 3, PhysicalType.INT32, null);
        assertThat(dict.size()).isEqualTo(3);
    }
}
