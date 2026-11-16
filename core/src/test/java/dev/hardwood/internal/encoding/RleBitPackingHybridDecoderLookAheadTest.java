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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Covers where [RleBitPackingHybridDecoder]'s bulk paths stop near the end of the data. Each
/// reads past the bytes a group owns, so it takes only the groups whose reads stay inside the
/// data and leaves the rest to the bit buffer. A bit-packed run of every group count from 1 to 40
/// ends the stream, at every bit width, so the last group the bulk path can take falls on every
/// group of the run in turn.
class RleBitPackingHybridDecoderLookAheadTest {

    static IntStream widths() {
        return IntStream.rangeClosed(1, 32);
    }

    /// The stream fills its array exactly, so a read past the data's end is a read past the array.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesARunThatEndsTheData(int bitWidth) {
        for (int groups = 1; groups <= 40; groups++) {
            int[] values = random(groups * 8, bitWidth, groups);
            byte[] stream = bitPackedRun(values, bitWidth);

            int[] decoded = new int[values.length];
            new RleBitPackingHybridDecoder(stream, bitWidth).readInts(decoded, 0, values.length);

            assertThat(decoded).as("%d groups", groups).containsExactly(values);
        }
    }

    /// The data starts inside its array, so every group is read from a non-zero offset.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesARunThatEndsTheDataAtAnOffset(int bitWidth) {
        for (int groups = 1; groups <= 40; groups++) {
            int[] values = random(groups * 8, bitWidth, groups);
            byte[] stream = bitPackedRun(values, bitWidth);
            byte[] array = new byte[stream.length + 40];
            Arrays.fill(array, (byte) 0xFF);
            System.arraycopy(stream, 0, array, 3, stream.length);

            int[] decoded = new int[values.length];
            new RleBitPackingHybridDecoder(array, 3, stream.length, bitWidth).readInts(decoded, 0, values.length);

            assertThat(decoded).as("%d groups", groups).containsExactly(values);
        }
    }

    private static int[] random(int count, int bitWidth, long seed) {
        Random random = new Random(seed * 37 + bitWidth);
        int mask = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = random.nextInt() & mask;
        }
        return values;
    }

    /// One bit-packed run of `values`, a multiple of eight: its header, then the values packed
    /// least significant bit first.
    private static byte[] bitPackedRun(int[] values, int bitWidth) {
        int groups = values.length / 8;
        byte[] header = varint(((long) groups << 1) | 1);
        byte[] stream = new byte[header.length + groups * bitWidth];
        System.arraycopy(header, 0, stream, 0, header.length);
        long bit = (long) header.length * 8;
        for (int value : values) {
            for (int b = 0; b < bitWidth; b++, bit++) {
                if ((value >>> b & 1) != 0) {
                    stream[(int) (bit >>> 3)] |= (byte) (1 << (bit & 7));
                }
            }
        }
        return stream;
    }

    private static byte[] varint(long value) {
        byte[] out = new byte[10];
        int n = 0;
        while ((value & ~0x7FL) != 0) {
            out[n++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out[n++] = (byte) value;
        return Arrays.copyOf(out, n);
    }
}
