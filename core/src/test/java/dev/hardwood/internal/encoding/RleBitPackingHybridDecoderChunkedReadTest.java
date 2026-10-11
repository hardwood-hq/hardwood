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

/// Reads a stream through [RleBitPackingHybridDecoder#readInts] in several calls whose counts
/// split groups of eight, which leaves part of a group in the decoder's bit buffer between
/// calls. Every width must decode the same values as a single call does.
class RleBitPackingHybridDecoderChunkedReadTest {

    private static final int[] CHUNKS = { 1, 5, 8, 13, 3, 64, 7, 16 };

    static IntStream widths() {
        return IntStream.rangeClosed(1, 32);
    }

    @ParameterizedTest
    @MethodSource("widths")
    void decodesBitPackedValuesReadInChunks(int bitWidth) {
        int[] values = random(1003, bitWidth);
        assertThat(readInChunks(encode(values, bitWidth), bitWidth, values.length)).containsExactly(values);
    }

    /// Chunks also cross from a bit-packed run into an RLE run and back.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesMixedRunsReadInChunks(int bitWidth) {
        int[] head = random(77, bitWidth);
        int[] repeated = new int[150];
        Arrays.fill(repeated, head[0]);
        int[] tail = random(91, bitWidth);
        int[] values = IntStream.concat(IntStream.concat(Arrays.stream(head), Arrays.stream(repeated)),
                Arrays.stream(tail)).toArray();
        assertThat(readInChunks(encode(values, bitWidth), bitWidth, values.length)).containsExactly(values);
    }

    private static int[] readInChunks(byte[] encoded, int bitWidth, int count) {
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);
        int[] decoded = new int[count];
        int pos = 0;
        for (int c = 0; pos < count; c++) {
            int n = Math.min(CHUNKS[c % CHUNKS.length], count - pos);
            decoder.readInts(decoded, pos, n);
            pos += n;
        }
        return decoded;
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
}
