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
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [BitPacker] against an independent bit reader, at every width its callers can ask for.
///
/// The reader here reads one bit at a time from the LSB of the first byte upwards, which is the
/// layout stated rather than the one the packer happens to produce — so a packer and a reader
/// that agreed on the wrong order would still fail here.
class BitPackerTest {

    /// Eight values is the RLE hybrid's group, 32 a delta miniblock and 1024 an ALP vector: at
    /// most widths the packer fills and stores its accumulator several times within the larger
    /// two, and ends on a part-filled one.
    private static final int[] GROUP_SIZES = { 8, 32, 1024 };

    @ParameterizedTest(name = "width {0}")
    @MethodSource("intWidths")
    void packsIntsAtEveryWidthTheRleEncoderUses(int bitWidth) {
        long mask = bitWidth == 32 ? 0xFFFFFFFFL : (1L << bitWidth) - 1;
        Random random = new Random(bitWidth);
        for (int count : GROUP_SIZES) {
            int[] values = new int[count];
            for (int i = 0; i < count; i++) {
                values[i] = (int) (random.nextLong() & mask);
            }

            byte[] out = new byte[BitPacker.packedLength(count, bitWidth)];
            int written = BitPacker.pack(values, 0, count, bitWidth, out, 0);

            assertThat(written).as("width %d, %d values: bytes written", bitWidth, count)
                    .isEqualTo(out.length);
            BitReader reader = new BitReader(out, 0);
            for (int i = 0; i < count; i++) {
                assertThat(reader.read(bitWidth)).as("width %d, value %d", bitWidth, i)
                        .isEqualTo(values[i] & mask);
            }
        }
    }

    @ParameterizedTest(name = "width {0}")
    @MethodSource("longWidths")
    void packsLongsAtEveryWidthADeltaMiniblockUses(int bitWidth) {
        // At a width that does not divide 64, a value straddles each full accumulator: its low
        // bits finish it and the rest start the next. At 64, every value fills one exactly.
        long mask = bitWidth == 64 ? -1L : (1L << bitWidth) - 1;
        Random random = new Random(bitWidth);
        for (int count : GROUP_SIZES) {
            long[] values = new long[count];
            for (int i = 0; i < count; i++) {
                values[i] = random.nextLong() & mask;
            }

            byte[] out = new byte[BitPacker.packedLength(count, bitWidth)];
            int written = BitPacker.pack(values, 0, count, bitWidth, out, 0);

            assertThat(written).as("width %d, %d values: bytes written", bitWidth, count)
                    .isEqualTo(out.length);
            BitReader reader = new BitReader(out, 0);
            for (int i = 0; i < count; i++) {
                assertThat(reader.read(bitWidth)).as("width %d, value %d", bitWidth, i)
                        .isEqualTo(values[i]);
            }
        }
    }

    static IntStream intWidths() {
        return IntStream.rangeClosed(1, Integer.SIZE);
    }

    static IntStream longWidths() {
        return IntStream.rangeClosed(1, Long.SIZE);
    }

    @Test
    void packsAllOnesAtTheFullLongWidth() {
        // The saturated case at width 64: every bit set, so a packer that dropped the top bit
        // of a value or shifted by the wrong amount cannot round-trip.
        long[] values = new long[32];
        Arrays.fill(values, -1L);

        byte[] out = new byte[BitPacker.packedLength(32, 64)];
        BitPacker.pack(values, 0, 32, 64, out, 0);

        assertThat(out).as("every byte saturated").containsOnly((byte) 0xFF);
    }

    @Test
    void writesNothingAtZeroWidth() {
        // A zero width is the constant miniblock and the single-entry dictionary: the values
        // are implied and occupy no bytes at all.
        assertThat(BitPacker.packedLength(32, 0)).isZero();
        assertThat(BitPacker.pack(new long[32], 0, 32, 0, new byte[0], 0)).isZero();
        assertThat(BitPacker.pack(new int[8], 0, 8, 0, new byte[0], 0)).isZero();
    }

    @Test
    void packsAGroupThatEndsInsideAByte() {
        // Five values at 3 bits are 15 bits: the second byte holds one bit of padding, left zero.
        int[] ints = { 7, 0, 5, 2, 6 };
        long[] longs = { 7, 0, 5, 2, 6 };
        byte[] fromInts = new byte[BitPacker.packedLength(5, 3)];
        byte[] fromLongs = new byte[BitPacker.packedLength(5, 3)];

        assertThat(BitPacker.pack(ints, 0, 5, 3, fromInts, 0)).isEqualTo(2);
        assertThat(BitPacker.pack(longs, 0, 5, 3, fromLongs, 0)).isEqualTo(2);

        BitReader reader = new BitReader(fromInts, 0);
        for (int value : ints) {
            assertThat(reader.read(3)).isEqualTo(value);
        }
        assertThat(reader.read(1)).as("padding bit").isZero();
        assertThat(fromLongs).isEqualTo(fromInts);
    }

    @Test
    void refusesAWidthItsValueTypeCannotHold() {
        assertThatThrownBy(() -> BitPacker.pack(new int[8], 0, 8, 33, new byte[64], 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bitWidth must be between 0 and 32 but was 33");
        assertThatThrownBy(() -> BitPacker.pack(new long[8], 0, 8, 65, new byte[128], 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bitWidth must be between 0 and 64 but was 65");
        assertThatThrownBy(() -> BitPacker.pack(new long[8], 0, 8, -1, new byte[8], 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bitWidth must be between 0 and 64 but was -1");
    }

    @Test
    void packsAtAnOffsetIntoTheDestination() {
        // The delta encoder packs miniblock after miniblock into one growing buffer, so the
        // destination offset has to be honoured and nothing before it disturbed.
        int[] values = new int[8];
        Arrays.fill(values, 0xFF);
        byte[] out = new byte[12];
        Arrays.fill(out, (byte) 0x5a);

        int written = BitPacker.pack(values, 0, 8, 8, out, 4);

        assertThat(written).isEqualTo(8);
        assertThat(out[0]).as("bytes before the offset are untouched").isEqualTo((byte) 0x5a);
        assertThat(out[3]).isEqualTo((byte) 0x5a);
        for (int i = 4; i < 12; i++) {
            assertThat(out[i]).as("packed byte %d", i).isEqualTo((byte) 0xFF);
        }
    }

    /// Reads fixed-width values LSB-first, independently of how [BitPacker] writes them.
    private static final class BitReader {

        private final byte[] data;
        private int bitPosition;

        BitReader(byte[] data, int startBit) {
            this.data = data;
            this.bitPosition = startBit;
        }

        long read(int bitWidth) {
            long value = 0;
            for (int i = 0; i < bitWidth; i++) {
                int bit = (data[bitPosition >>> 3] >>> (bitPosition & 7)) & 1;
                value |= (long) bit << i;
                bitPosition++;
            }
            return value;
        }
    }
}
