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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [BitPacker] against an independent bit reader, at every width both callers can ask for.
///
/// The reader here reads one bit at a time from the LSB of the first byte upwards, which is the
/// layout stated rather than the one the packer happens to produce — so a packer and a reader
/// that agreed on the wrong order would still fail here.
class BitPackerTest {

    /// Eight values is the RLE hybrid's group; 32 is a delta miniblock.
    private static final int[] GROUP_SIZES = { 8, 32 };

    @ParameterizedTest(name = "width {0}")
    @ValueSource(ints = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 24, 31, 32 })
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
    @ValueSource(ints = { 1, 7, 8, 32, 33, 48, 56, 57, 58, 63, 64 })
    void packsLongsAtEveryWidthADeltaMiniblockUses(int bitWidth) {
        // Widths past 57 take the split path, where one value's bits finish the accumulator and
        // the rest start the next — the case an INT64 column with type-spanning deltas reaches.
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
    void refusesAGroupItCannotWriteWhole() {
        // Only whole bytes are flushed, so a group whose bits do not divide by eight would be
        // short by a byte and unreadable. Both real callers are safe at every width by virtue of
        // their group sizes; the guard is what stops a future one truncating silently.
        assertThatThrownBy(() -> BitPacker.pack(new int[5], 0, 5, 3, new byte[8], 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Packing 5 values at 3 bits does not fill a whole number of bytes; this packer "
                         + "writes whole bytes only");
        assertThatThrownBy(() -> BitPacker.pack(new long[5], 0, 5, 3, new byte[8], 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Packing 5 values at 3 bits does not fill a whole number of bytes; this packer "
                         + "writes whole bytes only");
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
