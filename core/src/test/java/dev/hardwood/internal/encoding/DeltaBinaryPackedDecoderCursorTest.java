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

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [DeltaBinaryPackedDecoder] read the way a page's reader drives it: [DeltaBinaryPackedDecoder#skip]
/// to a run's first value, then a run into an array at an offset. The decoder holds one block of
/// 128 values (the first fill also the header's first value, 129 in all) and fills a caller's
/// array directly wherever a whole block fits.
class DeltaBinaryPackedDecoderCursorTest {

    private static final int COUNT = 1_000;
    private static final long[] VALUES = values();
    private static final byte[] ENCODED = DeltaBinaryPackedEncoder.encodeLongs(VALUES, 0, COUNT);

    private static long[] values() {
        long[] values = new long[COUNT];
        Random random = new Random(3);
        for (int i = 1; i < COUNT; i++) {
            values[i] = values[i - 1] + random.nextInt(2_001) - 1_000;
        }
        return values;
    }

    /// A skip ending exactly on a block boundary leaves the buffer drained; the next read decodes
    /// the next block.
    @ParameterizedTest
    @ValueSource(ints = { 129, 257, 385 })
    void readsAfterASkipEndingOnABlockBoundary(int skipped) {
        DeltaBinaryPackedDecoder decoder = decoder();

        decoder.skip(skipped);

        assertThat(decoder.readLong()).isEqualTo(VALUES[skipped]);
    }

    /// A run longer than a block after a skip that drained the buffer is decoded straight into the
    /// caller's array, then finished through the buffer.
    @Test
    void fillsTheCallersArrayDirectlyAfterASkip() {
        DeltaBinaryPackedDecoder decoder = decoder();
        long[] out = new long[600];

        decoder.skip(129);
        decoder.readLongs(out, 5, 500);

        assertThat(Arrays.copyOfRange(out, 5, 505)).containsExactly(Arrays.copyOfRange(VALUES, 129, 629));
        assertThat(out[4]).isZero();
        assertThat(out[505]).isZero();
    }

    @Test
    void readsARunStraddlingBlocksAfterASkipIntoABlock() {
        DeltaBinaryPackedDecoder decoder = decoder();
        long[] out = new long[310];

        decoder.skip(50);
        decoder.readLongs(out, 3, 300);

        assertThat(Arrays.copyOfRange(out, 3, 303)).containsExactly(Arrays.copyOfRange(VALUES, 50, 350));
    }

    /// Runs and skips of every length in turn read the stream as one pass over it would.
    @Test
    void interleavedSkipsAndRunsReadTheStreamInOrder() {
        DeltaBinaryPackedDecoder decoder = decoder();
        Random random = new Random(5);
        int pos = 0;
        while (pos < COUNT) {
            int skip = Math.min(random.nextInt(200), COUNT - pos);
            decoder.skip(skip);
            pos += skip;
            int run = Math.min(random.nextInt(300), COUNT - pos);
            long[] out = new long[run];
            decoder.readLongs(out, 0, run);
            assertThat(out).as("run at %d", pos).containsExactly(Arrays.copyOfRange(VALUES, pos, pos + run));
            pos += run;
        }
    }

    /// The INT32 form narrows the 64-bit prefix sum, which is the wrap-around the format calls for.
    @Test
    void readsIntsAtAnOffsetNarrowedFromThePrefixSum() {
        int[] values = new int[300];
        values[0] = Integer.MAX_VALUE - 5;
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + 3;
        }
        byte[] encoded = DeltaBinaryPackedEncoder.encodeInts(values, 0, values.length);
        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(encoded, 0, encoded.length);
        int[] out = new int[260];

        decoder.skip(1);
        decoder.readInts(out, 2, 258);

        assertThat(Arrays.copyOfRange(out, 2, 260)).containsExactly(Arrays.copyOfRange(values, 1, 259));
        // values[2] is one past Integer.MAX_VALUE, wrapped
        assertThat(out[3]).isEqualTo(Integer.MIN_VALUE);
        assertThat(out[1]).isZero();
    }

    @Test
    void refusesASkipPastTheLastValue() {
        DeltaBinaryPackedDecoder decoder = decoder();

        assertThatThrownBy(() -> decoder.skip(COUNT + 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("No more values to read");
    }

    private static DeltaBinaryPackedDecoder decoder() {
        return new DeltaBinaryPackedDecoder(ENCODED, 0, ENCODED.length);
    }
}
