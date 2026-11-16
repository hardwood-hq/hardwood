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

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Passes over values with [RleBitPackingHybridDecoder#skip] between reads: whole RLE runs and
/// bit-packed groups, and skips that start or end inside a group, with part of it in the bit
/// buffer. The values read after a skip must be those a full decode has there.
class RleBitPackingHybridDecoderSkipTest {

    /// Alternating skip and read counts, splitting groups of eight and crossing runs.
    private static final int[] STEPS = { 3, 5, 0, 8, 13, 1, 64, 7, 200, 9, 16, 2 };

    static IntStream widths() {
        return IntStream.rangeClosed(1, 32);
    }

    @ParameterizedTest
    @MethodSource("widths")
    void readsAfterSkipsWhatAFullDecodeReads(int bitWidth) {
        int[] values = mixedRuns(bitWidth);
        byte[] encoded = encode(values, bitWidth);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);
        int pos = 0;
        for (int step = 0; pos < values.length; step++) {
            int n = Math.min(STEPS[step % STEPS.length], values.length - pos);
            if (step % 2 == 0) {
                decoder.skip(n);
            }
            else {
                int[] read = new int[n];
                decoder.readInts(read, 0, n);
                assertThat(read).as("values %d to %d", pos, pos + n).containsExactly(
                        Arrays.copyOfRange(values, pos, pos + n));
            }
            pos += n;
        }
    }

    /// A skip from the stream's start to every seventh position, then a read of the rest.
    @ParameterizedTest
    @MethodSource("widths")
    void readsTheRestAfterASkipToAnyPosition(int bitWidth) {
        int[] values = mixedRuns(bitWidth);
        byte[] encoded = encode(values, bitWidth);
        for (int from = 0; from < values.length; from += 7) {
            RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);
            decoder.skip(from);
            int[] rest = new int[values.length - from];
            decoder.readInts(rest, 0, rest.length);
            assertThat(rest).as("from %d", from).containsExactly(Arrays.copyOfRange(values, from, values.length));
        }
    }

    /// A stream built run by run, so that its boundaries are known: a bit-packed run of two groups,
    /// an RLE run of five, a bit-packed run of one group. A skip to each position, the run ends
    /// among them, reads the rest as written.
    @Test
    void readsTheRestAfterASkipToEachRunBoundary() {
        int bitWidth = 3;
        int[] first = { 1, 2, 3, 4, 5, 6, 7, 0, 7, 6, 5, 4, 3, 2, 1, 0 };
        int[] last = { 2, 4, 6, 1, 3, 5, 7, 0 };
        byte[] encoded = concat(new byte[]{ 0x05 }, pack(first, bitWidth), new byte[]{ 0x0A, 6, 0x03 },
                pack(last, bitWidth));
        int[] values = IntStream.concat(IntStream.concat(Arrays.stream(first), IntStream.of(6, 6, 6, 6, 6)),
                Arrays.stream(last)).toArray();
        for (int from = 0; from <= values.length; from++) {
            RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);
            decoder.skip(from);
            int[] rest = new int[values.length - from];
            decoder.readInts(rest, 0, rest.length);
            assertThat(rest).as("from %d", from).containsExactly(Arrays.copyOfRange(values, from, values.length));
        }
    }

    @Test
    void refusesASkipPastTheStream() {
        byte[] encoded = encode(new int[]{ 1, 2, 3, 4, 5, 6, 7, 0 }, 3);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, 3);

        assertThatThrownBy(() -> decoder.skip(9))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Insufficient RLE/Bit-Packing data: skipped 8 of 9 requested values");
    }

    @Test
    void refusesASkipIntoABitPackedRunCutShort() {
        // A bit-packed run of two groups at width 8, whose data holds the first group and three
        // values of the second
        byte[] encoded = { 0x05, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, 8);

        assertThatThrownBy(() -> decoder.skip(12))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Insufficient RLE/Bit-Packing data: a bit-packed run declares 16 values, but the data "
                        + "ends after 11 of them");
    }

    /// Random values, a long run of one value the encoder writes as an RLE run, then more random
    /// values.
    private static int[] mixedRuns(int bitWidth) {
        int[] head = random(301, bitWidth);
        int[] repeated = new int[150];
        Arrays.fill(repeated, head[0]);
        int[] tail = random(523, bitWidth);
        return IntStream.concat(IntStream.concat(Arrays.stream(head), Arrays.stream(repeated)),
                Arrays.stream(tail)).toArray();
    }

    private static int[] random(int count, int bitWidth) {
        Random random = new Random(bitWidth * 7919L + count);
        int mask = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = random.nextInt() & mask;
        }
        return values;
    }

    private static byte[] encode(int[] values, int bitWidth) {
        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(bitWidth);
        encoder.writeInts(values, 0, values.length);
        return encoder.toByteArray();
    }

    /// `values` bit-packed at `bitWidth`, each value's bits from its least significant one, filled
    /// into each byte from its least significant bit.
    private static byte[] pack(int[] values, int bitWidth) {
        byte[] packed = new byte[(values.length * bitWidth + 7) / 8];
        int bit = 0;
        for (int value : values) {
            for (int b = 0; b < bitWidth; b++) {
                if ((value >>> b & 1) != 0) {
                    packed[bit / 8] |= (byte) (1 << (bit % 8));
                }
                bit++;
            }
        }
        return packed;
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }
}
