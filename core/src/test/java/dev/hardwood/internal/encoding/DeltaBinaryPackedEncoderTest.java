/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/// [DeltaBinaryPackedEncoder] against the [DeltaBinaryPackedDecoder] beside it, over the edges
/// the encoding has: block and miniblock boundaries, a constant column, and the wrap-around
/// cases where a delta does not fit the column's own type.
class DeltaBinaryPackedEncoderTest {

    // ==================== INT32 ====================

    @ParameterizedTest(name = "{0} values")
    @ValueSource(ints = { 1, 2, 31, 32, 33, 127, 128, 129, 512, 1000 })
    void roundTripsIntsAcrossBlockAndMiniblockBoundaries(int count) throws IOException {
        // 32 values fill a miniblock and 128 a block, so these counts land either side of both
        // boundaries and on a trailing block that leaves miniblocks unused.
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = i * 7 - 13;
        }

        assertThat(decodeInts(DeltaBinaryPackedEncoder.encodeInts(values, 0, count), count))
                .as("%d values", count).containsExactly(values);
    }

    @Test
    void roundTripsAConstantIntColumn() throws IOException {
        // Every delta is zero, so each block records a zero minimum and a zero bit width and
        // writes no miniblock bytes at all — the encoding's most compact shape.
        int[] values = new int[300];
        Arrays.fill(values, 42);

        byte[] encoded = DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length);

        assertThat(decodeInts(encoded, values.length)).containsExactly(values);
        assertThat(encoded.length).as("a constant column packs into the headers alone")
                .isLessThan(values.length);
    }

    @Test
    void roundTripsAlternatingIntExtremes() throws IOException {
        // The wrap-around case, and the reason encoder and decoder have to be held together
        // rather than each to its own idea of the format: the delta from MAX_VALUE to
        // MIN_VALUE does not fit an int, so it is taken with wrap-around, and the decoder
        // recovers the value only because its long accumulator is narrowed back to int.
        int[] values = new int[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 2 == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }

        assertThat(decodeInts(DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    @Test
    void roundTripsIntsDescending() throws IOException {
        // Descending values give every block a negative minimum delta, which is the sign the
        // zigzag header field exists for.
        int[] values = new int[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = 1_000_000 - i * 3;
        }

        assertThat(decodeInts(DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    @ParameterizedTest(name = "step {0}")
    @ValueSource(ints = { 1, -1, 7, -7 })
    void packsAUniformStepIntoAFewBitsPerValue(int step) throws IOException {
        // A column moving by a constant step has one distinct delta, so every residue is zero
        // and the values cost no miniblock bytes at all — the encoding's whole point.
        //
        // Size is asserted because a round trip cannot see this failing: an encoder that drove
        // the bit width to the full 32 bits would still decode perfectly, just an order of
        // magnitude larger. A descending step is the case that gets there by comparing the
        // block minimum unsigned.
        int[] values = new int[512];
        for (int i = 0; i < values.length; i++) {
            values[i] = 500_000 + i * step;
        }

        byte[] encoded = DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length);

        assertThat(decodeInts(encoded, values.length)).containsExactly(values);
        assertThat(encoded.length).as("step %d: bytes for %d values", step, values.length)
                .isLessThan(values.length / 8);
    }

    @Test
    void packsANarrowRangeFarMoreTightlyThanPlain() throws IOException {
        // Values that move in small steps but sit at large magnitudes: the deltas are what
        // costs, not the values, which is what separates this encoding from PLAIN.
        int[] values = new int[1_000];
        Random random = new Random(7L);
        int value = 1_700_000_000;
        for (int i = 0; i < values.length; i++) {
            values[i] = value;
            value += random.nextInt(16) - 8;
        }

        byte[] encoded = DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length);

        assertThat(decodeInts(encoded, values.length)).containsExactly(values);
        assertThat(encoded.length).as("bytes against PLAIN's %d", values.length * Integer.BYTES)
                .isLessThan(values.length * Integer.BYTES / 4);
    }

    @Test
    void roundTripsRandomIntsSpanningTheType() throws IOException {
        // Unordered values across the whole range: every block's deltas wrap, so the residues
        // need the full 32 bits and the bit width reaches its ceiling.
        int[] values = new int[1_000];
        Random random = new Random(19760401L);
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt();
        }

        assertThat(decodeInts(DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    @Test
    void encodesOnlyTheRequestedRange() throws IOException {
        // A page is a range of the chunk's stored values, so the offset has to be honoured.
        int[] values = { 999, 999, 10, 11, 12, 13, 999 };

        assertThat(decodeInts(DeltaBinaryPackedEncoder.encodeInts(values, 2, 4), 4))
                .containsExactly(10, 11, 12, 13);
    }

    // ==================== INT64 ====================

    @ParameterizedTest(name = "{0} values")
    @ValueSource(ints = { 1, 2, 31, 32, 33, 127, 128, 129, 512 })
    void roundTripsLongsAcrossBlockAndMiniblockBoundaries(int count) throws IOException {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = i * 1_000_000_007L - 5;
        }

        assertThat(decodeLongs(DeltaBinaryPackedEncoder.encodeLongs(values, 0, count), count))
                .as("%d values", count).containsExactly(values);
    }

    @Test
    void roundTripsAlternatingLongExtremes() throws IOException {
        // The INT64 wrap-around, which additionally drives the packed residues to the full
        // 64-bit width — the case the shared bit packer exists to handle.
        long[] values = new long[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }

        assertThat(decodeLongs(DeltaBinaryPackedEncoder.encodeLongs(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    @Test
    void roundTripsRandomLongsSpanningTheType() throws IOException {
        long[] values = new long[1_000];
        Random random = new Random(20260820L);
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextLong();
        }

        assertThat(decodeLongs(DeltaBinaryPackedEncoder.encodeLongs(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    @Test
    void roundTripsAConstantLongColumn() throws IOException {
        long[] values = new long[300];
        Arrays.fill(values, Long.MIN_VALUE);

        assertThat(decodeLongs(DeltaBinaryPackedEncoder.encodeLongs(values, 0, values.length), values.length))
                .containsExactly(values);
    }

    // ==================== Helpers ====================

    private static int[] decodeInts(byte[] encoded, int count) throws IOException {
        int[] output = new int[count];
        new DeltaBinaryPackedDecoder(encoded, 0, encoded.length).readInts(output, null, 0);
        return output;
    }

    private static long[] decodeLongs(byte[] encoded, int count) throws IOException {
        long[] output = new long[count];
        new DeltaBinaryPackedDecoder(encoded, 0, encoded.length).readLongs(output, null, 0);
        return output;
    }
}
