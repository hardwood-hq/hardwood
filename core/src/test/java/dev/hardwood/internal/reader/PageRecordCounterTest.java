/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import org.junit.jupiter.api.Test;

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
}
