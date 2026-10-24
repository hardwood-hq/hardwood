/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.decode.DataPages;
import dev.hardwood.internal.reader.decode.TestDictionaries;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The per-chunk interned-`String` cache decodes each dictionary entry once and hands back the same
/// instance on every request; a byte-array page refers to the entries by id, each checked against
/// the dictionary when the page is read
/// ([dev.hardwood.internal.reader.decode.BinaryPageValues#ids]).
class ByteArrayDictionaryInternTest {

    @Test
    void internedStringDecodesEachEntryOnceAndReusesIt() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {
            "alpha".getBytes(StandardCharsets.UTF_8),
            "beta".getBytes(StandardCharsets.UTF_8),
        });

        String alpha = dict.internedString(0);
        String beta = dict.internedString(1);

        assertThat(alpha).isEqualTo("alpha");
        assertThat(beta).isEqualTo("beta");
        // Same entry -> the same cached instance (decoded once per chunk).
        assertThat(dict.internedString(0)).isSameAs(alpha);
        assertThat(dict.internedString(1)).isSameAs(beta);
        // Different entries -> different instances.
        assertThat(beta).isNotSameAs(alpha);
    }

    @Test
    void internedStringHandlesAnEmptyEntry() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {
            new byte[0],
            "x".getBytes(StandardCharsets.UTF_8),
        });

        String empty = dict.internedString(0);
        assertThat(empty).isEmpty();
        // A zero-length entry is cached and reused like any other.
        assertThat(dict.internedString(0)).isSameAs(empty);
        assertThat(dict.internedString(1)).isEqualTo("x");
    }

    /// A non-string (`INT96` / `FIXED_LEN_BYTE_ARRAY`) column is still a `ByteArrayDictionary`, and
    /// its page's reader carries one id per value, even though those columns never intern; reading
    /// them must not corrupt the entry bytes.
    @Test
    void readsANonStringDictionaryPageThroughItsIds() {
        byte[] entry0 = {1, 2, 3, 4};
        byte[] entry1 = {5, 6, 7, 8};
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {entry0, entry1});

        // Bit width 1, then an RLE run: header (4 << 1) | 0 = 8 = "repeat 4 times", value 1.
        byte[][] values = read(DataPages.required(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4), 4, new byte[]{ 1, 8, 0x01 },
                dict);

        assertThat(values).isDeepEqualTo(new byte[][]{ entry1, entry1, entry1, entry1 });
    }

    @Test
    void readsNullPositionsAsNull() {
        byte[] entry0 = {1, 2, 3, 4};
        byte[] entry1 = {5, 6, 7, 8};
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {entry0, entry1});

        // Definition levels 1, 0, 1, 0 as one bit-packed group, then two present values, both
        // entry 1: bit width 1, RLE run header (2 << 1) | 0 = 4, value 1.
        byte[][] values = read(DataPages.optional(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4), 4,
                new byte[]{ 2, 0, 0, 0, 3, 0b0000_0101, 1, 4, 0x01 }, dict);

        assertThat(values).isDeepEqualTo(new byte[][]{ entry1, null, entry1, null });
    }

    @Test
    void rejectsAnIdBeyondTheDictionary() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {{1}, {2}});

        // RLE run of 4 values with index 3, beyond the 2-entry dictionary (bit width 2).
        assertThatThrownBy(() -> read(DataPages.required(PhysicalType.BYTE_ARRAY, null), 4,
                new byte[]{ 2, 8, 0x03 }, dict))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 3 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void rejectsAnIdBeyondTheDictionaryOnAPageWithNulls() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {{1}, {2}});

        // Definition levels 1, 0, 1, 0, then two present values, both index 3 (bit width 2).
        assertThatThrownBy(() -> read(DataPages.optional(PhysicalType.BYTE_ARRAY, null), 4,
                new byte[]{ 2, 0, 0, 0, 3, 0b0000_0101, 2, 4, 0x03 }, dict))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 3 out of bounds for a dictionary of 2 entries");
    }

    /// At bit width 32 an id can decode negative, which must not pass for the `-1` null marker.
    @Test
    void rejectsANegativeId() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[][] {{1}, {2}});

        // RLE run of 4 values, value 0x80000000 as 4 little-endian bytes (bit width 32).
        assertThatThrownBy(() -> read(DataPages.required(PhysicalType.BYTE_ARRAY, null), 4,
                new byte[]{ 32, 8, 0x00, 0x00, 0x00, (byte) 0x80 }, dict))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index -2147483648 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void rejectsAnyPresentValueAgainstAnEmptyDictionary() {
        Dictionary.ByteArrayDictionary dict = TestDictionaries.byteArrays(new byte[0][]);

        // Bit width 0: every id decodes as 0 without reading the stream.
        assertThatThrownBy(() -> read(DataPages.required(PhysicalType.BYTE_ARRAY, null), 4, new byte[]{ 0 }, dict))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 0 out of bounds for a dictionary of 0 entries");
    }

    private static byte[][] read(ColumnSchema column, int numValues, byte[] body, Dictionary dictionary) {
        return (byte[][]) DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                DataPages.page(Encoding.RLE_DICTIONARY, numValues, body), dictionary);
    }
}
