/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [RleBitPackingHybridDecoder#isSingleRleRunOf], the all-present
/// definition-level probe.
class RleBitPackingHybridDecoderSingleRunTest {

    @Test
    void detectsRleRunMatchingValueAndCovering() {
        byte[] encoded = encodeRleRun(1, 1000, 1);
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 1000)).isTrue();
    }

    @Test
    void detectsRleRunLongerThanRequested() {
        byte[] encoded = encodeRleRun(1, 1000, 1);
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 500)).isTrue();
    }

    @Test
    void rejectsRleRunShorterThanRequested() {
        byte[] encoded = encodeRleRun(1, 500, 1);
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 1000)).isFalse();
    }

    @Test
    void rejectsRleRunOfDifferentValue() {
        // An all-null page: def levels are a single RLE run of 0, not maxDef.
        byte[] encoded = encodeRleRun(0, 1000, 1);
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 1000)).isFalse();
    }

    @Test
    void rejectsLeadingBitPackedRun() {
        byte[] encoded = encodeBitPackedRun(new int[] {1, 0, 1, 1, 0, 1, 0, 1}, 1);
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 8)).isFalse();
    }

    @Test
    void rejectsRunFollowedByMoreWhenCoverageExceedsFirstRun() {
        // First run covers 8 values; asking for 16 must fail (the second run
        // could differ, and only the first run header is inspected).
        byte[] encoded = concat(
                encodeRleRun(1, 8, 1),
                encodeRleRun(0, 8, 1));
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 8)).isTrue();
        assertThat(decoder(encoded, 1).isSingleRleRunOf(1, 16)).isFalse();
    }

    @Test
    void handlesMaxDefGreaterThanOne() {
        byte[] encoded = encodeRleRun(2, 100, 2);
        assertThat(decoder(encoded, 2).isSingleRleRunOf(2, 100)).isTrue();
        assertThat(decoder(encoded, 2).isSingleRleRunOf(1, 100)).isFalse();
    }

    @Test
    void zeroCountIsTriviallyTrue() {
        assertThat(decoder(new byte[0], 1).isSingleRleRunOf(1, 0)).isTrue();
    }

    @Test
    void rejectsWhenNoDataButCountPositive() {
        assertThat(decoder(new byte[0], 1).isSingleRleRunOf(1, 1)).isFalse();
    }

    @Test
    void readIntsAfterProbeStillReadsWholeStreamAtEndOfData() {
        // A single RLE run of a non-max value covering the whole page (e.g. an
        // all-def-1 page with maxDef=2). The probe must leave the decoder
        // untouched so a following readInts reads the run from the beginning.
        byte[] encoded = encodeRleRun(1, 100, 2);
        RleBitPackingHybridDecoder decoder = decoder(encoded, 2);

        assertThat(decoder.isSingleRleRunOf(2, 100)).isFalse();

        int[] decoded = new int[100];
        decoder.readInts(decoded, 0, 100);
        assertThat(decoded).containsOnly(1);
    }

    @Test
    void readIntsAfterProbeStillReadsAllRuns() {
        byte[] encoded = concat(
                encodeRleRun(1, 8, 2),
                encodeBitPackedRun(new int[] {0, 1, 2, 3, 3, 2, 1, 0}, 2),
                encodeRleRun(2, 4, 2));
        RleBitPackingHybridDecoder decoder = decoder(encoded, 2);

        assertThat(decoder.isSingleRleRunOf(3, 20)).isFalse();

        int[] decoded = new int[20];
        decoder.readInts(decoded, 0, 20);
        assertThat(decoded).containsExactly(
                1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 2, 3, 3, 2, 1, 0, 2, 2, 2, 2);
    }

    @Test
    void probeIsReadOnlyAndRepeatable() {
        // The probe must not advance decoder state: repeated calls agree, and a
        // full read afterwards still returns every value.
        byte[] encoded = concat(
                encodeRleRun(1, 8, 2),
                encodeRleRun(2, 4, 2));
        RleBitPackingHybridDecoder decoder = decoder(encoded, 2);

        assertThat(decoder.isSingleRleRunOf(3, 12)).isFalse();
        assertThat(decoder.isSingleRleRunOf(3, 12)).isFalse();
        assertThat(decoder.isSingleRleRunOf(1, 8)).isTrue();

        int[] decoded = new int[12];
        decoder.readInts(decoded, 0, 12);
        assertThat(decoded).containsExactly(1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2);
    }

    private static RleBitPackingHybridDecoder decoder(byte[] data, int bitWidth) {
        return new RleBitPackingHybridDecoder(data, 0, data.length, bitWidth);
    }

    private static byte[] encodeRleRun(int value, int count, int bitWidth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, (long) count << 1);
        int bytesNeeded = (bitWidth + 7) / 8;
        for (int i = 0; i < bytesNeeded; i++) {
            out.write((value >> (i * 8)) & 0xFF);
        }
        return out.toByteArray();
    }

    private static byte[] encodeBitPackedRun(int[] values, int bitWidth) {
        int groups = (values.length + 7) / 8;
        int padded = groups * 8;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, ((long) groups << 1) | 1L);

        long bits = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < padded; i++) {
            int value = i < values.length ? values[i] : 0;
            bits |= ((long) value) << bitsInBuffer;
            bitsInBuffer += bitWidth;
            while (bitsInBuffer >= 8) {
                out.write((int) (bits & 0xFF));
                bits >>>= 8;
                bitsInBuffer -= 8;
            }
        }
        if (bitsInBuffer > 0) {
            out.write((int) (bits & 0xFF));
        }
        return out.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] out = new byte[length];
        int pos = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, out, pos, part.length);
            pos += part.length;
        }
        return out;
    }

    private static void writeUnsignedVarInt(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }
}
