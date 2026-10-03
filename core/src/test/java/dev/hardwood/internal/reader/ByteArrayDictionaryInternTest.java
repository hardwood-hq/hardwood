/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The per-chunk interned-`String` cache decodes each dictionary entry once and
/// hands back the same instance on every request.
class ByteArrayDictionaryInternTest {

    @Test
    void internedStringDecodesEachEntryOnceAndReusesIt() {
        Dictionary.ByteArrayDictionary dict = new Dictionary.ByteArrayDictionary(new byte[][] {
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
        Dictionary.ByteArrayDictionary dict = new Dictionary.ByteArrayDictionary(new byte[][] {
            new byte[0],
            "x".getBytes(StandardCharsets.UTF_8),
        });

        String empty = dict.internedString(0);
        assertThat(empty).isEmpty();
        // A zero-length entry is cached and reused like any other.
        assertThat(dict.internedString(0)).isSameAs(empty);
        assertThat(dict.internedString(1)).isEqualTo("x");
    }

    /// A non-string (`INT96` / `FIXED_LEN_BYTE_ARRAY`) column is still a
    /// `ByteArrayDictionary`. `decodePage` now builds a per-value `dictIndices`
    /// array for every such page, even though those columns never intern; it must
    /// not corrupt the decoded entry bytes.
    @Test
    void decodePageDecodesANonStringDictionaryThroughTheWidenedPath() throws Exception {
        byte[] entry0 = {1, 2, 3, 4};
        byte[] entry1 = {5, 6, 7, 8};
        Dictionary.ByteArrayDictionary dict =
                new Dictionary.ByteArrayDictionary(new byte[][] {entry0, entry1});

        // RLE index run: header (4 << 1) | 0 = 8 = "repeat 4 times", value 1 (bit width 1).
        byte[] indexStream = {8, 0x01};
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indexStream, 1);
        Page.DictionaryByteArrayPage page = (Page.DictionaryByteArrayPage) dict.decodePage(indexDecoder, 4, null, null, 0);

        assertThat(page.dictIndices()).containsExactly(1, 1, 1, 1);
        assertThat(page.dictionary()).isSameAs(dict);
        for (int i = 0; i < page.size(); i++) {
            assertThat(page.get(i)).isSameAs(entry1);
        }
    }

    @Test
    void decodePageResolvesNullPositionsToNull() throws Exception {
        byte[] entry0 = {1, 2, 3, 4};
        byte[] entry1 = {5, 6, 7, 8};
        Dictionary.ByteArrayDictionary dict =
                new Dictionary.ByteArrayDictionary(new byte[][] {entry0, entry1});

        // Two present values, both entry 1: RLE run header (2 << 1) | 0 = 4, value 1.
        byte[] indexStream = {4, 0x01};
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indexStream, 1);
        int[] definitionLevels = {1, 0, 1, 0};
        Page.DictionaryByteArrayPage page = (Page.DictionaryByteArrayPage) dict.decodePage(indexDecoder, 4, definitionLevels, null, 1);

        assertThat(page.dictIndices()).containsExactly(1, -1, 1, -1);
        assertThat(page.get(0)).isSameAs(entry1);
        assertThat(page.get(1)).isNull();
        assertThat(page.get(2)).isSameAs(entry1);
        assertThat(page.get(3)).isNull();
    }

    @Test
    void decodePageRejectsAnIndexBeyondTheDictionary() {
        Dictionary.ByteArrayDictionary dict =
                new Dictionary.ByteArrayDictionary(new byte[][] {{1}, {2}});

        // RLE run of 4 values with index 3, beyond the 2-entry dictionary (bit width 2).
        byte[] indexStream = {8, 0x03};
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indexStream, 2);

        assertThatThrownBy(() -> dict.decodePage(indexDecoder, 4, null, null, 0))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 3 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void decodePageRejectsAnIndexBeyondTheDictionaryOnAPageWithNulls() {
        Dictionary.ByteArrayDictionary dict =
                new Dictionary.ByteArrayDictionary(new byte[][] {{1}, {2}});

        // Two present values, both index 3: RLE run header (2 << 1) | 0 = 4, value 3 (bit width 2).
        byte[] indexStream = {4, 0x03};
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indexStream, 2);
        int[] definitionLevels = {1, 0, 1, 0};

        assertThatThrownBy(() -> dict.decodePage(indexDecoder, 4, definitionLevels, null, 1))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 3 out of bounds for a dictionary of 2 entries");
    }

    /// At bit width 32 an index can decode negative, which must not pass for the `-1` null marker.
    @Test
    void decodePageRejectsANegativeIndex() {
        Dictionary.ByteArrayDictionary dict =
                new Dictionary.ByteArrayDictionary(new byte[][] {{1}, {2}});

        // RLE run of 4 values, value 0x80000000 as 4 little-endian bytes (bit width 32).
        byte[] indexStream = {8, 0x00, 0x00, 0x00, (byte) 0x80};
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indexStream, 32);

        assertThatThrownBy(() -> dict.decodePage(indexDecoder, 4, null, null, 0))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index -2147483648 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void decodePageRejectsAnyPresentValueAgainstAnEmptyDictionary() {
        Dictionary.ByteArrayDictionary dict = new Dictionary.ByteArrayDictionary(new byte[0][]);

        // Bit width 0: every index decodes as 0 without reading the stream.
        RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(new byte[0], 0);

        assertThatThrownBy(() -> dict.decodePage(indexDecoder, 4, null, null, 0))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 0 out of bounds for a dictionary of 0 entries");
    }
}
