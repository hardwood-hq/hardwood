/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Exercises [PageRecordCounter.countTopLevelRecords] against hand-crafted
/// RLE/bit-packing-hybrid encoded rep-level streams. The byte sequences match
/// what a Parquet writer emits in `DataPageHeaderV2.repetition_levels_byte_length`
/// bytes for a column with `maxRepetitionLevel == 1`.
class PageRecordCounterTest {

    /// All-zeros RLE run: every value is a top-level record start.
    /// Encoding: header byte `(count << 1) | 0` (RLE), value byte `0x00`.
    @Test
    void testAllZerosRleRunCountsEveryValueAsRecord() {
        byte[] data = { (byte) (5 << 1), 0x00 };
        int records = PageRecordCounter.countTopLevelRecords(data, 0, data.length, 5, 1);
        assertThat(records).isEqualTo(5);
    }

    /// All-ones RLE run: no top-level records (every value is a continuation).
    @Test
    void testAllOnesRleRunCountsZeroRecords() {
        byte[] data = { (byte) (5 << 1), 0x01 };
        int records = PageRecordCounter.countTopLevelRecords(data, 0, data.length, 5, 1);
        assertThat(records).isEqualTo(0);
    }

    /// Bit-packed run encoding "0,1,0,1,0,_,_,_" — five logical values, three of
    /// which are zero (records). Header byte `(numGroups << 1) | 1` with one
    /// 8-value group; data byte packs values LSB-first per the Parquet spec.
    @Test
    void testBitPackedAlternatingValuesCountsCorrectly() {
        byte packed = 0b0000_1010;
        byte[] data = { (byte) ((1 << 1) | 1), packed };
        int records = PageRecordCounter.countTopLevelRecords(data, 0, data.length, 5, 1);
        assertThat(records).isEqualTo(3);
    }

    /// `numValues == 0` is a degenerate page — return 0 without touching the
    /// decoder. Exercised because the iterator may encounter empty pages on
    /// pathological writers without fetching the rep-level prefix.
    @Test
    void testZeroNumValuesReturnsZeroWithoutDecoding() {
        int records = PageRecordCounter.countTopLevelRecords(new byte[0], 0, 0, 0, 1);
        assertThat(records).isEqualTo(0);
    }

    /// Flat columns must not reach the rep-level walk — caller is responsible
    /// for dispatching on `maxRepetitionLevel == 0` and using `num_values`
    /// directly. The helper enforces this with an explicit guard.
    @Test
    void testFlatColumnRejectsCountTopLevelRecords() {
        assertThatThrownBy(() ->
                PageRecordCounter.countTopLevelRecords(new byte[]{0}, 0, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flat column");
    }

    /// Honours an `offset` into the supplied byte array — the caller may pass a
    /// larger buffer holding the whole page header + rep-level prefix. The two
    /// leading `0xFF` bytes are deliberate garbage that must be ignored when
    /// `offset == 2`; if the helper read from index 0 they would decode as a
    /// nonsense run and either throw or produce a wrong count.
    @Test
    void testHonoursOffsetIntoSuppliedBuffer() {
        byte[] data = { (byte) 0xFF, (byte) 0xFF, (byte) (3 << 1), 0x00 };
        int records = PageRecordCounter.countTopLevelRecords(data, 2, 2, 3, 1);
        assertThat(records).isEqualTo(3);
    }

    /// `maxRepetitionLevel == 2` (e.g. doubly-nested lists) drives the decoder
    /// at bit-width 2, so each value occupies two bits in the bit-packed
    /// stream. Sequence "0, 1, 0, 2, 0" — three records, one continuation each
    /// at rep-level 1 and 2. Bit-packed run with a single 8-value group:
    /// values pack LSB-first per byte, so bits `[1:0]=00`, `[3:2]=01`,
    /// `[5:4]=00`, `[7:6]=10` give byte0 `0b1000_0100 = 0x84`; values 4..7
    /// (only value 4 is meaningful, the rest are padding) all zero, giving
    /// byte1 `0x00`.
    @Test
    void testWiderBitWidthCountsCorrectly() {
        byte[] data = { (byte) ((1 << 1) | 1), (byte) 0x84, 0x00 };
        int records = PageRecordCounter.countTopLevelRecords(data, 0, data.length, 5, 2);
        assertThat(records).isEqualTo(3);
    }

    /// Round-trip equivalence: for varied rep-level sequences, the walker's
    /// zero-count must match what you get by fully decoding the same bytes via
    /// [RleBitPackingHybridDecoder] and counting zeros in the materialised
    /// `int[]`. This guards the count-only optimisation against drift from the
    /// canonical decode path.
    @ParameterizedTest
    @MethodSource("roundTripCases")
    void testWalkerMatchesMaterialisedDecode(String name, int[] values, int maxRepLevel) {
        int bitWidth = PageRecordCounter.bitWidthFor(maxRepLevel);
        byte[] bytes = encodeBitPacked(values, bitWidth);

        int[] decoded = new int[values.length];
        new RleBitPackingHybridDecoder(bytes, bitWidth).readInts(decoded, 0, values.length);
        int expectedZeros = 0;
        for (int v : decoded) {
            if (v == 0) expectedZeros++;
        }

        int walkerZeros = PageRecordCounter.countTopLevelRecords(
                bytes, 0, bytes.length, values.length, maxRepLevel);

        assertThat(walkerZeros)
                .as("case %s: walker count must match materialised-decode count", name)
                .isEqualTo(expectedZeros);
    }

    /// Round-trip with RLE-encoded runs (single repeated value). Catches RLE
    /// dispatch bugs the bit-packed cases can't reach.
    @ParameterizedTest
    @MethodSource("rleRoundTripCases")
    void testWalkerMatchesMaterialisedRleDecode(String name, int value, int count, int maxRepLevel) {
        int bitWidth = PageRecordCounter.bitWidthFor(maxRepLevel);
        byte[] bytes = encodeRle(value, count, bitWidth);

        int[] decoded = new int[count];
        new RleBitPackingHybridDecoder(bytes, bitWidth).readInts(decoded, 0, count);
        int expectedZeros = 0;
        for (int v : decoded) {
            if (v == 0) expectedZeros++;
        }

        int walkerZeros = PageRecordCounter.countTopLevelRecords(
                bytes, 0, bytes.length, count, maxRepLevel);

        assertThat(walkerZeros)
                .as("case %s: walker count must match materialised-decode count", name)
                .isEqualTo(expectedZeros);
    }

    static Stream<Arguments> roundTripCases() {
        Random rng = new Random(0x37137137L);
        return Stream.of(
                Arguments.of("all-zeros bw=1", filled(64, 0), 1),
                Arguments.of("all-ones bw=1", filled(64, 1), 1),
                Arguments.of("alternating bw=1", alternating(64, 0, 1), 1),
                Arguments.of("alternating bw=1 odd-length", alternating(63, 0, 1), 1),
                Arguments.of("partial-group bw=1", alternating(5, 0, 1), 1),
                Arguments.of("random bw=1 large", randomLevels(rng, 4096, 1), 1),
                Arguments.of("random bw=2", randomLevels(rng, 1024, 2), 2),
                Arguments.of("random bw=3", randomLevels(rng, 1024, 3), 5),
                Arguments.of("random bw=4", randomLevels(rng, 1024, 4), 12),
                Arguments.of("random bw=8", randomLevels(rng, 512, 8), 200),
                Arguments.of("non-multiple-of-8 bw=2", randomLevels(rng, 17, 2), 2));
    }

    static Stream<Arguments> rleRoundTripCases() {
        return Stream.of(
                Arguments.of("rle zero short bw=1", 0, 5, 1),
                Arguments.of("rle one short bw=1", 1, 5, 1),
                Arguments.of("rle zero long bw=1", 0, 1024, 1),
                Arguments.of("rle two bw=2", 2, 100, 2),
                Arguments.of("rle 200 bw=8", 200, 50, 200),
                Arguments.of("rle huge varint header bw=1", 0, 200_000, 1));
    }

    private static int[] filled(int length, int value) {
        int[] out = new int[length];
        Arrays.fill(out, value);
        return out;
    }

    private static int[] alternating(int length, int a, int b) {
        int[] out = new int[length];
        for (int i = 0; i < length; i++) {
            out[i] = (i % 2 == 0) ? a : b;
        }
        return out;
    }

    private static int[] randomLevels(Random rng, int length, int maxLevel) {
        int[] out = new int[length];
        for (int i = 0; i < length; i++) {
            out[i] = rng.nextInt(maxLevel + 1);
        }
        return out;
    }

    /// Mini RLE/bit-packing-hybrid encoder — emits a single bit-packed run
    /// covering `padded = ceil(values.length / 8) * 8` values. Padding values
    /// are zero. Sufficient for round-trip equivalence testing.
    private static byte[] encodeBitPacked(int[] values, int bitWidth) {
        int groups = (values.length + 7) / 8;
        int padded = groups * 8;
        int dataBytes = groups * bitWidth;
        byte[] out = new byte[10 + dataBytes];
        int pos = writeUnsignedVarInt(out, 0, ((long) groups << 1) | 1L);

        long bits = 0;
        int bitsInBuf = 0;
        for (int i = 0; i < padded; i++) {
            int v = (i < values.length) ? values[i] : 0;
            bits |= ((long) v) << bitsInBuf;
            bitsInBuf += bitWidth;
            while (bitsInBuf >= 8) {
                out[pos++] = (byte) (bits & 0xFF);
                bits >>>= 8;
                bitsInBuf -= 8;
            }
        }
        if (bitsInBuf > 0) {
            out[pos++] = (byte) (bits & 0xFF);
        }
        return Arrays.copyOf(out, pos);
    }

    private static byte[] encodeRle(int value, int count, int bitWidth) {
        int bytesNeeded = (bitWidth + 7) / 8;
        byte[] out = new byte[10 + bytesNeeded];
        int pos = writeUnsignedVarInt(out, 0, (long) count << 1);
        for (int i = 0; i < bytesNeeded; i++) {
            out[pos++] = (byte) ((value >> (i * 8)) & 0xFF);
        }
        return Arrays.copyOf(out, pos);
    }

    private static int writeUnsignedVarInt(byte[] out, int pos, long value) {
        while ((value & ~0x7FL) != 0) {
            out[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out[pos++] = (byte) value;
        return pos;
    }
}
