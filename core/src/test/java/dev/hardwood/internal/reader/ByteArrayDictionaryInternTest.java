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
        Page.ByteArrayPage page = (Page.ByteArrayPage) dict.decodePage(indexDecoder, 4, null, null, 0);

        assertThat(page.dictIndices()).containsExactly(1, 1, 1, 1);
        assertThat(page.dictionary()).isSameAs(dict);
        for (byte[] value : page.values()) {
            assertThat(value).isEqualTo(entry1);
        }
    }
}
