/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Round-trips [RleBitPackingHybridEncoder] against [RleBitPackingHybridDecoder]: the two
/// share no code, so agreement pins the encoded byte layout to what the reader decodes.
class RleBitPackingHybridEncoderTest {

    @Test
    void encodesConstantStreamAsSingleRleRun() {
        int[] values = new int[1000];
        Arrays.fill(values, 1);
        byte[] encoded = encode(values, 1);

        // A constant stream must collapse to one RLE run so the reader's all-present
        // definition-level fast path fires.
        assertThat(new RleBitPackingHybridDecoder(encoded, 1).isSingleRleRunOf(1, 1000)).isTrue();
        assertRoundTrip(values, 1, encoded);
    }

    @Test
    void encodesAlternatingStreamViaBitPacking() {
        int[] values = new int[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 2;
        }
        assertRoundTrip(values, 1);
    }

    @Test
    void encodesCountNotMultipleOfEight() {
        int[] values = { 1, 0, 1, 1, 0 };
        assertRoundTrip(values, 1);
    }

    @Test
    void encodesEmptyStream() {
        assertRoundTrip(new int[0], 1);
    }

    @Test
    void encodesMixedRunsAndBitPacking() {
        // Long constant runs interleaved with noisy stretches exercise both run types and
        // the transitions between them.
        int[] values = new int[300];
        int i = 0;
        while (i < 100) {
            values[i++] = 3;
        }
        for (int j = 0; j < 100; j++) {
            values[i++] = j % 4;
        }
        while (i < 300) {
            values[i++] = 0;
        }
        assertRoundTrip(values, 2);
    }

    @Test
    void encodesAcrossBitWidths() {
        Random random = new Random(42);
        for (int bitWidth = 1; bitWidth <= 20; bitWidth++) {
            int max = bitWidth == 32 ? Integer.MAX_VALUE : (1 << bitWidth) - 1;
            int[] values = new int[500];
            for (int i = 0; i < values.length; i++) {
                values[i] = random.nextInt(max + 1);
            }
            assertRoundTrip(values, bitWidth);
        }
    }

    @Test
    void encodesWideBitWidthsThroughFullWidth() {
        // Widths 21-32 exercise the encoder's upper range that the def-level path (width 1)
        // never reaches but the dictionary-index path will: notably the bitWidth == 32
        // full-word mask branch and values that set the top bit (negative as signed ints).
        Random random = new Random(99);
        for (int bitWidth = 21; bitWidth <= 32; bitWidth++) {
            long mask = bitWidth == 32 ? 0xFFFFFFFFL : (1L << bitWidth) - 1;
            int[] values = new int[500];
            for (int i = 0; i < values.length; i++) {
                values[i] = (int) (random.nextInt() & mask);
            }
            // Pin the widest value so the top bit of the range is always covered.
            values[0] = (int) mask;
            assertRoundTrip(values, bitWidth);
        }
    }

    @Test
    void spansManyBitPackedGroupsBeyondOneRunHeader() {
        // More than 63 eight-value groups forces a second bit-packed run header.
        int[] values = new int[8 * 100];
        Random random = new Random(7);
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(8);
        }
        assertRoundTrip(values, 3);
    }

    @Test
    void bitWidthZeroEncodesNothing() {
        byte[] encoded = encode(new int[] { 0, 0, 0 }, 0);
        assertThat(encoded).isEmpty();
    }

    @Test
    void rejectsWritingAfterFinish() {
        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(1);
        encoder.writeInt(1);
        encoder.toByteArray();
        assertThatThrownBy(() -> encoder.writeInt(0)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsInvalidBitWidth() {
        assertThatThrownBy(() -> new RleBitPackingHybridEncoder(33))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RleBitPackingHybridEncoder(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] encode(int[] values, int bitWidth) {
        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(bitWidth);
        encoder.writeInts(values, 0, values.length);
        return encoder.toByteArray();
    }

    private static void assertRoundTrip(int[] values, int bitWidth) {
        assertRoundTrip(values, bitWidth, encode(values, bitWidth));
    }

    private static void assertRoundTrip(int[] values, int bitWidth, byte[] encoded) {
        int[] decoded = new int[values.length];
        new RleBitPackingHybridDecoder(encoded, bitWidth).readInts(decoded, 0, values.length);
        assertThat(decoded).containsExactly(values);
    }
}
