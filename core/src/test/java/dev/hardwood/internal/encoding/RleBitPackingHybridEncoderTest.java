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

    /// A zero-bit stream still needs its RLE run header. Hardwood's own decoder short-circuits
    /// on the bit width and never reads the bytes, but parquet-java and Arrow C++ drive the
    /// decode from the stream itself and read past its end if the header is missing, so a
    /// single-entry dictionary produced an index page they could not decode (#901).
    ///
    /// The run is `<varint (count << 1)>` with no value bytes, since a zero-bit value occupies
    /// `ceil(0 / 8) == 0` bytes.
    @Test
    void encodesZeroBitStreamAsAnRleRunHeader() {
        byte[] encoded = encode(new int[300], 0);

        // 300 << 1 == 600, a two-byte unsigned varint.
        assertThat(encoded).containsExactly(0xD8, 0x04);
    }

    @Test
    void encodesEmptyZeroBitStreamAsNoBytes() {
        assertThat(encode(new int[0], 0)).isEmpty();
    }

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

    /// [RleBitPackingHybridEncoder#writeInts] takes eight values at a time where it can; its
    /// bytes must be exactly those of the same values written one by one, however the stream is
    /// split across calls, since a page's bytes are what the writer's output is pinned on.
    @Test
    void bulkWritesMatchValueByValueWrites() {
        Random random = new Random(1513);
        for (int trial = 0; trial < 2000; trial++) {
            // At most eight bits, so the same values also fit the byte-wide level path.
            int bitWidth = random.nextInt(9);
            int[] values = runHeavyValues(random, bitWidth);
            RleBitPackingHybridEncoder single = new RleBitPackingHybridEncoder(bitWidth);
            for (int value : values) {
                single.writeInt(value);
            }
            RleBitPackingHybridEncoder bulk = new RleBitPackingHybridEncoder(bitWidth);
            for (int from = 0; from < values.length; ) {
                int length = Math.min(values.length - from, random.nextInt(40));
                if (random.nextInt(4) == 0 && length > 0) {
                    bulk.writeInt(values[from]);
                    length = 1;
                }
                else {
                    bulk.writeInts(values, from, length);
                }
                from += length;
            }
            byte[] expected = single.toByteArray();
            assertThat(bulk.toByteArray()).as("trial %d", trial).isEqualTo(expected);

            byte[] levels = new byte[values.length];
            for (int i = 0; i < values.length; i++) {
                levels[i] = (byte) values[i];
            }
            RleBitPackingHybridEncoder bytes = new RleBitPackingHybridEncoder(bitWidth);
            bytes.writeUnsignedBytes(levels, 0, levels.length);
            assertThat(bytes.toByteArray()).as("trial %d", trial).isEqualTo(expected);
        }
    }

    /// Up to 2000 values of `bitWidth` bits, drawn as runs of random length so that both run
    /// kinds and the transitions between them, at every alignment, occur.
    private static int[] runHeavyValues(Random random, int bitWidth) {
        int[] values = new int[random.nextInt(2000)];
        int max = 1 << bitWidth;
        int i = 0;
        while (i < values.length) {
            int value = random.nextInt(max);
            int run = random.nextInt(3) == 0 ? 1 + random.nextInt(30) : 1 + random.nextInt(3);
            for (int k = 0; k < run && i < values.length; k++) {
                values[i++] = value;
            }
        }
        return values;
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
