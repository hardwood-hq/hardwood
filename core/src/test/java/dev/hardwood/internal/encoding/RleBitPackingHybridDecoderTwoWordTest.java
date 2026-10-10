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

/// Covers the two-word extraction [RleBitPackingHybridDecoder] uses for bit widths 9 to 16: each
/// group of eight is read from two little-endian words, the second of which runs past the group,
/// so the cases that matter are values straddling the word boundary, groups split across
/// `readInts` calls, runs that follow a bit-packed run, and streams too short for two words.
class RleBitPackingHybridDecoderTwoWordTest {

    static IntStream widths() {
        return IntStream.rangeClosed(9, 16);
    }

    @ParameterizedTest
    @MethodSource("widths")
    void decodesRandomValuesAcrossManyGroups(int bitWidth) {
        assertRoundTrip(random(1003, bitWidth, 1), bitWidth);
    }

    /// All-ones and all-zeros groups put a set bit on both sides of every word boundary a value
    /// can straddle.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesExtremeValues(int bitWidth) {
        int max = (1 << bitWidth) - 1;
        int[] values = new int[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = switch (i % 3) {
                case 0 -> max;
                case 1 -> 0;
                default -> i % 2 == 0 ? max : 1;
            };
        }
        assertRoundTrip(values, bitWidth);
    }

    /// Chunks that are not multiples of eight leave bits in the decoder's buffer, so the decode
    /// alternates between the bit buffer and the two-word groups.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesInChunksThatSplitGroups(int bitWidth) {
        int[] values = random(517, bitWidth, 2);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encode(values, bitWidth), bitWidth);
        int[] decoded = new int[values.length];
        int[] chunks = { 1, 5, 8, 13, 3, 64, 7, 16 };
        int pos = 0;
        for (int c = 0; pos < values.length; c++) {
            int n = Math.min(chunks[c % chunks.length], values.length - pos);
            decoder.readInts(decoded, pos, n);
            pos += n;
        }
        assertThat(decoded).containsExactly(values);
    }

    /// The second word of the last groups reads into the RLE run that follows, which must still
    /// decode from its own header.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesRunsThatFollowABitPackedRun(int bitWidth) {
        int[] head = random(72, bitWidth, 3);
        int[] repeated = new int[200];
        Arrays.fill(repeated, (1 << bitWidth) - 2);
        int[] tail = random(40, bitWidth, 4);
        int[] values = IntStream.concat(IntStream.concat(Arrays.stream(head), Arrays.stream(repeated)),
                Arrays.stream(tail)).toArray();
        assertRoundTrip(values, bitWidth);
    }

    /// Eight values fill fewer than 16 bytes, so they decode through the bit buffer. Of sixteen,
    /// the first group still has 16 bytes ahead of it and takes the two words, the second does
    /// not.
    @ParameterizedTest
    @MethodSource("widths")
    void decodesStreamsShorterThanTwoWords(int bitWidth) {
        assertRoundTrip(random(8, bitWidth, 5), bitWidth);
        assertRoundTrip(random(16, bitWidth, 6), bitWidth);
    }

    private static int[] random(int count, int bitWidth, long seed) {
        Random random = new Random(seed * 31 + bitWidth);
        int mask = (1 << bitWidth) - 1;
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

    private static void assertRoundTrip(int[] values, int bitWidth) {
        int[] decoded = new int[values.length];
        new RleBitPackingHybridDecoder(encode(values, bitWidth), bitWidth).readInts(decoded, 0, values.length);
        assertThat(decoded).containsExactly(values);
    }
}
