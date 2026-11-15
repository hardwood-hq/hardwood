/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BitPackedLevelDecoderTest {

    /// The example of the format's encoding description: 0 to 7 at three bits.
    @Test
    void decodesTheFormatsExample() {
        int[] levels = new int[8];

        BitPackedLevelDecoder.decode(new byte[]{ 0b00000101, 0b00111001, 0b01110111 }, 0, 3, levels, 8);

        assertThat(levels).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    }

    /// Levels wider than a byte straddle byte boundaries; decoding starts at `offset`.
    @Test
    void decodesLevelsWiderThanAByteFromAnOffset() {
        int[] levels = new int[3];

        // 0x1FF, 0x000, 0x155 at nine bits, behind one byte that is not part of the section
        BitPackedLevelDecoder.decode(new byte[]{ 0x7F, (byte) 0xFF, (byte) 0x80, 0x2A, (byte) 0xA0 }, 1, 9, levels, 3);

        assertThat(levels).containsExactly(0x1FF, 0x000, 0x155);
    }
}
