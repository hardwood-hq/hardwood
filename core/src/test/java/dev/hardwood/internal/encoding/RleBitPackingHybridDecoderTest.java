/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RleBitPackingHybridDecoderTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
    void readsBitPackedRunsForWidthsOneThroughEight(int bitWidth) {
        Random random = new Random(0x680L + bitWidth);
        int[] values = IntStream.range(0, 257)
                .map(i -> random.nextInt(1 << bitWidth))
                .toArray();
        byte[] encoded = encodeBitPackedRun(values, bitWidth);

        int[] decoded = new int[values.length];
        new RleBitPackingHybridDecoder(encoded, bitWidth).readInts(decoded, 0, decoded.length);

        assertThat(decoded).isEqualTo(values);
    }

    @Test
    void preservesBitPackedStateAcrossSplitReads() {
        int bitWidth = 3;
        int[] values = {0, 1, 7, 2, 6, 3, 5, 4, 1, 2, 3, 4, 5, 6, 7, 0, 1};
        byte[] encoded = encodeBitPackedRun(values, bitWidth);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(encoded, bitWidth);

        int[] decoded = new int[values.length];
        decoder.readInts(decoded, 0, 5);
        decoder.readInts(decoded, 5, 4);
        decoder.readInts(decoded, 9, 8);

        assertThat(decoded).isEqualTo(values);
    }

    @Test
    void readsMixedRleAndBitPackedRuns() {
        int bitWidth = 2;
        byte[] encoded = concat(
                encodeRleRun(2, 4, bitWidth),
                encodeBitPackedRun(new int[] {0, 1, 2, 3, 3, 2, 1, 0}, bitWidth),
                encodeRleRun(1, 3, bitWidth));

        int[] decoded = new int[15];
        new RleBitPackingHybridDecoder(encoded, bitWidth).readInts(decoded, 0, decoded.length);

        assertThat(decoded).containsExactly(2, 2, 2, 2, 0, 1, 2, 3, 3, 2, 1, 0, 1, 1, 1);
    }

    @Test
    void respectsOffsetAndLengthSlice() {
        int bitWidth = 2;
        int[] values = {0, 1, 2, 3, 3, 2, 1, 0};
        byte[] encoded = encodeBitPackedRun(values, bitWidth);
        byte[] wrapped = new byte[encoded.length + 5];
        Arrays.fill(wrapped, (byte) 0x7F);
        System.arraycopy(encoded, 0, wrapped, 2, encoded.length);

        int[] decoded = new int[values.length];
        new RleBitPackingHybridDecoder(wrapped, 2, encoded.length, bitWidth)
                .readInts(decoded, 0, decoded.length);

        assertThat(decoded).isEqualTo(values);
    }

    @Test
    void throwsWhenRequestedValuesExceedCurrentSlice() {
        int bitWidth = 2;
        byte[] encoded = encodeBitPackedRun(new int[] {0, 1, 2, 3, 3, 2, 1, 0}, bitWidth);
        byte[] wrapped = concat(encoded, encodeRleRun(3, 8, bitWidth));

        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(wrapped, 0, encoded.length, bitWidth);
        int[] decoded = new int[16];

        assertThatThrownBy(() -> decoder.readInts(decoded, 0, decoded.length))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Insufficient RLE/Bit-Packing data");
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

    private static byte[] encodeRleRun(int value, int count, int bitWidth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnsignedVarInt(out, (long) count << 1);
        int bytesNeeded = (bitWidth + 7) / 8;
        for (int i = 0; i < bytesNeeded; i++) {
            out.write((value >> (i * 8)) & 0xFF);
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
