/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [RleBitPackingHybridDecoder#readLevels], which decodes as [RleBitPackingHybridDecoder#readInts]
/// does and reports the largest value it decoded.
class RleBitPackingHybridDecoderReadLevelsTest {

    @Test
    void reportsTheLargestValueOfABitPackedRun() {
        int[] values = concat(repeat(1, 20), new int[] {0, 1, 2, 3, 0, 1, 2, 0}, repeat(2, 16));

        assertThat(readLevels(values, 2, values.length)).isEqualTo(3);
    }

    @Test
    void reportsTheLargestValueOfAnRleRun() {
        int[] values = concat(new int[] {0, 1, 0, 1, 0, 1, 0, 1}, repeat(3, 16), new int[] {1, 0, 1, 0, 1, 0, 1, 0});

        assertThat(readLevels(values, 2, values.length)).isEqualTo(3);
    }

    @Test
    void reportsOnlyTheValuesItDecodes() {
        int[] values = concat(new int[] {0, 1, 2, 1, 0, 1, 2, 1}, repeat(0, 8), new int[] {3, 1, 0, 1, 0, 1, 0, 1});

        assertThat(readLevels(values, 2, 16)).isEqualTo(2);
    }

    /// A read ending inside a bit-packed group leaves the rest of the group buffered; the next read
    /// takes those values first and reports them.
    @Test
    void reportsTheValuesOfAGroupSplitAcrossReads() {
        int[] values = {0, 1, 0, 1, 0, 3, 0, 1, 0, 1, 0, 1, 0, 1, 0, 2};
        RleBitPackingHybridDecoder decoder = decoder(values, 2);
        int[] buffer = new int[values.length];

        assertThat(decoder.readLevels(buffer, 3)).isEqualTo(1);
        assertThat(decoder.readLevels(buffer, 5)).isEqualTo(3);
        assertThat(decoder.readLevels(buffer, 8)).isEqualTo(2);
    }

    @Test
    void decodesTheSameValuesAsReadInts() {
        int[] values = IntStream.range(0, 1000).map(i -> i % 7 == 0 ? 2 : (i / 50) % 3).toArray();
        int[] read = new int[values.length];
        int[] levels = new int[values.length];

        decoder(values, 2).readInts(read, 0, values.length);
        int max = decoder(values, 2).readLevels(levels, values.length);

        assertThat(levels).containsExactly(read);
        assertThat(levels).containsExactly(values);
        assertThat(max).isEqualTo(2);
    }

    private static int readLevels(int[] values, int bitWidth, int count) {
        return decoder(values, bitWidth).readLevels(new int[count], count);
    }

    private static RleBitPackingHybridDecoder decoder(int[] values, int bitWidth) {
        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(bitWidth);
        encoder.writeInts(values, 0, values.length);
        byte[] data = encoder.toByteArray();
        return new RleBitPackingHybridDecoder(data, 0, data.length, bitWidth);
    }

    private static int[] repeat(int value, int count) {
        int[] values = new int[count];
        Arrays.fill(values, value);
        return values;
    }

    private static int[] concat(int[]... parts) {
        return Arrays.stream(parts).flatMapToInt(Arrays::stream).toArray();
    }
}
