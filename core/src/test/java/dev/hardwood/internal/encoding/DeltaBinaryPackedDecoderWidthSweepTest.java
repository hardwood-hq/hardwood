/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Sweeps every bit width — 0–32 for INT32, 0–64 for INT64 — and both ways of draining the decoder,
/// in bulk and one value at a time.
///
/// One loop unpacks every width, so there is no band of widths a sweep can afford to sample rather
/// than walk: nothing about width 21 follows from width 20 working. Only width 0 branches, and it
/// branches away from the loop entirely.
///
/// parquet-java writes the bytes. A round trip against Hardwood's own encoder cannot see the two
/// implementations agreeing on a bit layout that the format does not have, and the layout is what
/// these hand-written unpack kernels have to be right about.
///
/// Each case encodes 50 values: a full miniblock of 32 plus a partial miniblock of 17, which
/// exercises the partial-last-miniblock truncation in [DeltaBinaryPackedDecoder].
///
/// Every case asserts the width the encoder actually chose before asserting the values, because a
/// sweep that silently encodes at some other width tests that width twice and the target not at
/// all. Alternating `MIN_VALUE`/`MAX_VALUE` reads like the way to reach the ceiling width and is
/// not: its deltas are −1 and +1, so it encodes at width 2.
class DeltaBinaryPackedDecoderWidthSweepTest {

    private static final int VALUE_COUNT = 50;
    private static final int BLOCK_SIZE = 128;
    private static final int MINIBLOCK_COUNT = 4;

    // The two miniblocks the 50 values occupy; the other two are declared at width 0 and hold
    // nothing.
    private static final int USED_MINIBLOCKS = 2;

    // ==================== INT32 ====================

    static IntStream int32BitWidths() {
        return IntStream.rangeClosed(0, 32);
    }

    @ParameterizedTest(name = "INT32 width {0}")
    @MethodSource("int32BitWidths")
    void sweepsInt32BitWidths(int w) throws IOException {
        int[] values = int32ValuesForWidth(w);
        byte[] encoded = encode(values);

        assertThat(declaredBitWidths(encoded)).as("INT32 width %d: widths the encoder chose", w)
                .startsWith(w, w);

        int[] bulk = new int[VALUE_COUNT];
        new DeltaBinaryPackedDecoder(encoded, 0).readInts(bulk, null, 0);
        assertThat(bulk).as("INT32 width %d, read in bulk", w).containsExactly(values);

        DeltaBinaryPackedDecoder oneAtATime = new DeltaBinaryPackedDecoder(encoded, 0);
        int[] single = new int[VALUE_COUNT];
        for (int i = 0; i < VALUE_COUNT; i++) {
            single[i] = oneAtATime.readInt();
        }
        assertThat(single).as("INT32 width %d, read one value at a time", w).containsExactly(values);
    }

    /// Values whose block minimum is 0 and whose largest residual needs exactly `w` bits.
    ///
    /// Width 32 is the exception: no positive `int` delta spans 32 bits, so it is reached with a
    /// block minimum of `MIN_VALUE` and a largest delta of `MAX_VALUE`, whose difference is the
    /// all-ones residual.
    private static int[] int32ValuesForWidth(int w) {
        int[] deltas = new int[VALUE_COUNT - 1];
        Random random = new Random(w);
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = w == 0 ? 0
                    : w == 32 ? (random.nextBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE)
                    : random.nextInt() >>> (32 - w);
        }
        // Pin the extremes into both used miniblocks so each declares the target width, and pin the
        // block minimum so the residuals are the deltas themselves.
        int low = w == 32 ? Integer.MIN_VALUE : 0;
        int high = w == 32 ? Integer.MAX_VALUE : (w == 0 ? 0 : -1 >>> (32 - w));
        for (int miniblock = 0; miniblock < USED_MINIBLOCKS; miniblock++) {
            int base = miniblock * (BLOCK_SIZE / MINIBLOCK_COUNT);
            deltas[base] = high;
            deltas[base + 1] = low;
        }

        int[] values = new int[VALUE_COUNT];
        for (int i = 1; i < VALUE_COUNT; i++) {
            values[i] = values[i - 1] + deltas[i - 1];
        }
        return values;
    }

    // ==================== INT64 ====================

    static IntStream int64BitWidths() {
        return IntStream.rangeClosed(0, 64);
    }

    @ParameterizedTest(name = "INT64 width {0}")
    @MethodSource("int64BitWidths")
    void sweepsInt64BitWidths(int w) throws IOException {
        long[] values = int64ValuesForWidth(w);
        byte[] encoded = encode(values);

        assertThat(declaredBitWidths(encoded)).as("INT64 width %d: widths the encoder chose", w)
                .startsWith(w, w);

        long[] bulk = new long[VALUE_COUNT];
        new DeltaBinaryPackedDecoder(encoded, 0).readLongs(bulk, null, 0);
        assertThat(bulk).as("INT64 width %d, read in bulk", w).containsExactly(values);

        DeltaBinaryPackedDecoder oneAtATime = new DeltaBinaryPackedDecoder(encoded, 0);
        long[] single = new long[VALUE_COUNT];
        for (int i = 0; i < VALUE_COUNT; i++) {
            single[i] = oneAtATime.readLong();
        }
        assertThat(single).as("INT64 width %d, read one value at a time", w).containsExactly(values);
    }

    /// The INT64 counterpart of [#int32ValuesForWidth], with width 64 as the exception.
    private static long[] int64ValuesForWidth(int w) {
        long[] deltas = new long[VALUE_COUNT - 1];
        Random random = new Random(w);
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = w == 0 ? 0L
                    : w == 64 ? (random.nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE)
                    : random.nextLong() >>> (64 - w);
        }
        long low = w == 64 ? Long.MIN_VALUE : 0L;
        long high = w == 64 ? Long.MAX_VALUE : (w == 0 ? 0L : -1L >>> (64 - w));
        for (int miniblock = 0; miniblock < USED_MINIBLOCKS; miniblock++) {
            int base = miniblock * (BLOCK_SIZE / MINIBLOCK_COUNT);
            deltas[base] = high;
            deltas[base + 1] = low;
        }

        long[] values = new long[VALUE_COUNT];
        for (int i = 1; i < VALUE_COUNT; i++) {
            values[i] = values[i - 1] + deltas[i - 1];
        }
        return values;
    }

    /// Value counts that land the page's last miniblock in different places relative to the end of
    /// the byte array, which is what decides whether the unpack reads the page directly or a padded
    /// copy of its tail. 33 ends on a full miniblock and always forces the copy; 50 leaves a partial
    /// one and mostly does not; 129 and 1000 span several blocks and do both.
    @ParameterizedTest(name = "INT64 width {1}, {0} values")
    @MethodSource("tailShapes")
    void decodesEveryTailShape(int count, int w) throws IOException {
        long[] values = valuesOfWidth(count, w);
        byte[] encoded = encode(values);

        long[] decoded = new long[count];
        new DeltaBinaryPackedDecoder(encoded, 0).readLongs(decoded, null, 0);
        assertThat(decoded).as("%d values at width %d", count, w).containsExactly(values);

        long[] padded = new long[count];
        new DeltaBinaryPackedDecoder(withTrailingSlack(encoded), 0).readLongs(padded, null, 0);
        assertThat(padded).as("%d values at width %d, with room to read wide", count, w)
                .containsExactly(values);
    }

    static Stream<Arguments> tailShapes() {
        return Stream.of(33, 50, 129, 1000)
                .flatMap(count -> Stream.of(1, 8, 21, 57, 64)
                        .map(w -> Arguments.of(count, w)));
    }

    /// The composite byte-array decoders read their lengths through this decoder and then carry on
    /// from where it stopped, so the position it leaves behind is a contract: it has to land on the
    /// byte after the delta stream, whatever shape the last block took.
    @ParameterizedTest(name = "{0} values, width {1}")
    @MethodSource("tailShapes")
    void leavesThePositionAfterTheEncodedStream(int count, int w) throws IOException {
        byte[] encoded = encode(valuesOfWidth(count, w));

        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(encoded, 0);
        decoder.readLongs(new long[count], null, 0);

        assertThat(decoder.getPos()).as("%d values at width %d", count, w).isEqualTo(encoded.length);
    }

    /// Layouts other than the 128/4 every writer emits.
    ///
    /// The block and miniblock sizes are declared per stream, not fixed by the format, and the
    /// decoder derives from them the size of its buffer, the bytes a miniblock occupies and where a
    /// partial last miniblock stops — so a decoder that is only ever handed 32 values per miniblock
    /// has only ever had one set of those bounds exercised.
    @ParameterizedTest(name = "{0}/{1}, {2} values, width {3}")
    @MethodSource("layouts")
    void decodesEveryBlockAndMiniblockLayout(int blockSize, int miniblockCount, int count, int w)
            throws IOException {
        long[] values = valuesOfWidth(count, w);
        byte[] encoded = encode(values, blockSize, miniblockCount);

        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(encoded, 0);
        long[] decoded = new long[count];
        decoder.readLongs(decoded, null, 0);

        assertThat(decoded).as("%d/%d, %d values at width %d", blockSize, miniblockCount, count, w)
                .containsExactly(values);
        assertThat(decoder.getPos()).as("position after %d/%d", blockSize, miniblockCount)
                .isEqualTo(encoded.length);
    }

    /// Miniblocks of 16 through 128 values, at counts that stop inside the first miniblock, inside a
    /// later one, and past the end of the first block.
    static Stream<Arguments> layouts() {
        return Stream.of(new int[] { 128, 8 }, new int[] { 128, 4 }, new int[] { 128, 1 },
                new int[] { 256, 4 }, new int[] { 1024, 8 })
                .flatMap(layout -> Stream.of(10, 200, 1500)
                        .flatMap(count -> Stream.of(3, 33, 64)
                                .map(w -> Arguments.of(layout[0], layout[1], count, w))));
    }

    @Test
    void refusesToReadPastTheDeclaredValueCount() throws IOException {
        byte[] encoded = encode(valuesOfWidth(VALUE_COUNT, 11));

        assertThatThrownBy(() -> new DeltaBinaryPackedDecoder(encoded, 0)
                .readLongs(new long[VALUE_COUNT + 1], null, 0))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("No more values to read");
    }

    // ==================== Helpers ====================

    private static long[] valuesOfWidth(int count, int w) {
        long[] values = new long[count];
        Random random = new Random(w);
        for (int i = 1; i < count; i++) {
            values[i] = values[i - 1] + (w == 0 ? 0L : random.nextLong() >>> (64 - w));
        }
        return values;
    }

    /// The same page with eight bytes of slack after it.
    ///
    /// The unpack reads eight bytes past the start of each value and works from a padded copy of the
    /// miniblock where the page ends inside that window — which a page does whenever its last
    /// miniblock is full. Decoding both ways puts the direct read and the copied one against each
    /// other; without the slack, some shapes would only ever take the copy.
    private static byte[] withTrailingSlack(byte[] encoded) {
        byte[] padded = new byte[encoded.length + Long.BYTES];
        System.arraycopy(encoded, 0, padded, 0, encoded.length);
        return padded;
    }

    private static byte[] encode(int[] values) throws IOException {
        try (DeltaBinaryPackingValuesWriterForInteger writer = new DeltaBinaryPackingValuesWriterForInteger(
                BLOCK_SIZE, MINIBLOCK_COUNT, 1024, 4096, HeapByteBufferAllocator.getInstance())) {
            for (int value : values) {
                writer.writeInteger(value);
            }
            return writer.getBytes().toByteArray();
        }
    }

    private static byte[] encode(long[] values) throws IOException {
        return encode(values, BLOCK_SIZE, MINIBLOCK_COUNT);
    }

    private static byte[] encode(long[] values, int blockSize, int miniblockCount) throws IOException {
        try (DeltaBinaryPackingValuesWriterForLong writer = new DeltaBinaryPackingValuesWriterForLong(
                blockSize, miniblockCount, 1024, 4096, HeapByteBufferAllocator.getInstance())) {
            for (long value : values) {
                writer.writeLong(value);
            }
            return writer.getBytes().toByteArray();
        }
    }

    /// The bit widths the first block declares, read straight out of the encoded header.
    private static int[] declaredBitWidths(byte[] encoded) {
        int[] pos = { 0 };
        uleb128(encoded, pos); // block size
        int miniblockCount = uleb128(encoded, pos);
        uleb128(encoded, pos); // total value count
        uleb128(encoded, pos); // first value, zigzag
        uleb128(encoded, pos); // block minimum delta, zigzag

        int[] widths = new int[miniblockCount];
        for (int i = 0; i < miniblockCount; i++) {
            widths[i] = encoded[pos[0]++] & 0xFF;
        }
        return widths;
    }

    private static int uleb128(byte[] encoded, int[] pos) {
        int result = 0;
        int shift = 0;
        int b;
        do {
            b = encoded[pos[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
