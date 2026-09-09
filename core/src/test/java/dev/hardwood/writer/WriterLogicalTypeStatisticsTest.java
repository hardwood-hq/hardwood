/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for the stage-13b statistics: a logical type redefines its column's sort order, so the
/// bounds the writer accumulates must follow the annotation rather than the physical type.
class WriterLogicalTypeStatisticsTest {

    /// Under the signed order these values would bound as `[-1, 1]`; unsigned they bound as
    /// `[1, 4294967295]`. A reader pruning `v > 100` on the signed bounds would drop the whole
    /// chunk even though `0xFFFFFFFF` is in it.
    @Test
    void unsignedIntColumnBoundsUnsigned() throws Exception {
        int[] values = { 1, -1, 7 }; // -1 is 4294967295 unsigned

        Statistics statistics = writeAndReadStatistics(PhysicalType.INT32, null,
                LogicalType.intType(32, false), batch -> batch.ints(0, values));

        assertThat(toInt(statistics.minValue())).isEqualTo(1);
        assertThat(toInt(statistics.maxValue())).isEqualTo(-1);
    }

    @Test
    void signedIntColumnStillBoundsSigned() throws Exception {
        int[] values = { 1, -1, 7 };

        Statistics statistics = writeAndReadStatistics(PhysicalType.INT32, null,
                LogicalType.intType(32, true), batch -> batch.ints(0, values));

        assertThat(toInt(statistics.minValue())).isEqualTo(-1);
        assertThat(toInt(statistics.maxValue())).isEqualTo(7);
    }

    @Test
    void unsignedLongColumnBoundsUnsigned() throws Exception {
        long[] values = { 1L, -1L, 7L };

        Statistics statistics = writeAndReadStatistics(PhysicalType.INT64, null,
                LogicalType.intType(64, false), batch -> batch.longs(0, values));

        assertThat(toLong(statistics.minValue())).isEqualTo(1L);
        assertThat(toLong(statistics.maxValue())).isEqualTo(-1L);
    }

    /// A decimal's unscaled value is big-endian two's complement, so `0xFF` is `-1` and sorts
    /// below `0x01`, the opposite of the unsigned lexicographic order a plain binary column uses.
    @Test
    void binaryDecimalBoundsAsASignedInteger() throws Exception {
        byte[][] values = { hex("01"), hex("FF"), hex("7F") }; // 1, -1, 127

        Statistics statistics = writeAndReadStatistics(PhysicalType.BYTE_ARRAY, null,
                LogicalType.decimal(18, 0), batch -> batch.bytes(0, values));

        assertThat(statistics.minValue()).isEqualTo(hex("FF"));
        assertThat(statistics.maxValue()).isEqualTo(hex("7F"));
    }

    /// Values of different lengths compare as if the shorter were sign-extended: `0xFF` is `-1`
    /// and `0x00FF` is `255`, so the two-byte value is the larger despite its lower first byte.
    @Test
    void binaryDecimalSignExtendsShorterValues() throws Exception {
        byte[][] values = { hex("FF"), hex("00FF"), hex("FF00") }; // -1, 255, -256

        Statistics statistics = writeAndReadStatistics(PhysicalType.BYTE_ARRAY, null,
                LogicalType.decimal(18, 0), batch -> batch.bytes(0, values));

        assertThat(statistics.minValue()).isEqualTo(hex("FF00"));
        assertThat(statistics.maxValue()).isEqualTo(hex("00FF"));
    }

    /// A fixed-width decimal compares the same way: `0xFF…FF` is `-1` and sorts below `0x00…01`,
    /// the opposite of the unsigned lexicographic order the same physical type uses unannotated.
    @Test
    void fixedLengthDecimalBoundsAsASignedInteger() throws Exception {
        byte[][] values = { hex("0000000000000001"), hex("FFFFFFFFFFFFFFFF"), hex("000000000000007F") };

        Statistics statistics = writeAndReadStatistics(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.decimal(18, 0), batch -> batch.fixed(0, values));

        assertThat(statistics.minValue()).isEqualTo(hex("FFFFFFFFFFFFFFFF"));
        assertThat(statistics.maxValue()).isEqualTo(hex("000000000000007F"));
    }

    /// An undefined-order column accumulates no bounds at all, but must still report its null
    /// count — the only statistic a reader has left to prune on.
    @Test
    void undefinedOrderColumnsStillCountNulls() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        LogicalType.geometry("EPSG:4326"))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, new byte[][] { hex("01"), hex("02"), hex("03") },
                    Validity.ofNulls(new boolean[] { false, true, false })));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(statistics.minValue()).isNull();
            assertThat(statistics.maxValue()).isNull();
            assertThat(statistics.nullCount()).isEqualTo(1L);
        }
    }

    /// Truncation keeps a prefix as the lower bound and an incremented prefix as the upper one,
    /// which preserves the unsigned lexicographic order but not the signed big-endian one — a
    /// shorter byte string is a different number there. A decimal's bounds stay whole and exact.
    @Test
    void binaryDecimalBoundsAreNeverTruncated() throws Exception {
        byte[][] values = { hex("0102030405060708090A"), hex("0102030405060708090B") };
        WriterConfig config = WriterConfig.builder().statisticsTruncationLength(4).build();

        Statistics statistics = writeAndReadStatistics(PhysicalType.BYTE_ARRAY, null,
                LogicalType.decimal(24, 0), config, batch -> batch.bytes(0, values));

        assertThat(statistics.minValue()).isEqualTo(hex("0102030405060708090A"));
        assertThat(statistics.maxValue()).isEqualTo(hex("0102030405060708090B"));
        assertThat(statistics.isMinValueExact()).isTrue();
        assertThat(statistics.isMaxValueExact()).isTrue();
    }

    /// A string column's bounds still truncate, since a prefix is a valid lower bound under the
    /// lexicographic order.
    @Test
    void stringBoundsStillTruncate() throws Exception {
        byte[][] values = { "aaaaaaaaaaaa".getBytes(StandardCharsets.UTF_8),
                "bbbbbbbbbbbb".getBytes(StandardCharsets.UTF_8) };
        WriterConfig config = WriterConfig.builder().statisticsTruncationLength(4).build();

        Statistics statistics = writeAndReadStatistics(PhysicalType.BYTE_ARRAY, null,
                LogicalType.string(), config, batch -> batch.bytes(0, values));

        assertThat(statistics.minValue()).isEqualTo("aaaa".getBytes(StandardCharsets.UTF_8));
        assertThat(statistics.isMinValueExact()).isFalse();
        assertThat(statistics.maxValue()).isEqualTo("bbbc".getBytes(StandardCharsets.UTF_8));
        assertThat(statistics.isMaxValueExact()).isFalse();
    }

    /// A `FLOAT16` compares by its represented value, not its bytes: `-2.0` (`0xC000`) is the
    /// smallest here although its bytes are the largest unsigned.
    @Test
    void float16BoundsByRepresentedValue() throws Exception {
        byte[][] values = { half(1.0f), half(-2.0f), half(0.5f) };

        Statistics statistics = writeAndReadStatistics(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16(), batch -> batch.fixed(0, values));

        assertThat(toHalf(statistics.minValue())).isEqualTo(-2.0f);
        assertThat(toHalf(statistics.maxValue())).isEqualTo(1.0f);
    }

    /// The floating-point rules apply as they do to `FLOAT` and `DOUBLE`: a `NaN` never extends
    /// the bounds, and a zero bound is sign-normalized so either signed zero falls inside.
    @Test
    void float16ExcludesNaNAndNormalizesZero() throws Exception {
        byte[][] values = { half(Float.NaN), half(0.0f), half(2.0f) };

        Statistics statistics = writeAndReadStatistics(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16(), batch -> batch.fixed(0, values));

        assertThat(toHalf(statistics.minValue())).isEqualTo(-0.0f);
        assertThat(Float.floatToRawIntBits(toHalf(statistics.minValue())))
                .isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(toHalf(statistics.maxValue())).isEqualTo(2.0f);
        assertThat(statistics.nanCount()).isEqualTo(1L);
    }

    /// Zero is the only value that proves a `FLOAT16` chunk holds no NaN — absent means "not
    /// recorded".
    @Test
    void float16RecordsZeroNanCountWhenNoNaN() throws Exception {
        byte[][] values = { half(1.0f), half(-2.0f), half(0.5f) };

        Statistics statistics = writeAndReadStatistics(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16(), batch -> batch.fixed(0, values));

        assertThat(statistics.nanCount()).isEqualTo(0L);
    }

    /// A chunk of nothing but `NaN` has no value the bounds can be computed from, so it writes
    /// none — the NaN count is then all a reader learns about it.
    @Test
    void allNaNFloat16ColumnHasNoBounds() throws Exception {
        byte[][] values = { half(Float.NaN), half(Float.NaN), half(Float.NaN) };

        Statistics statistics = writeAndReadStatistics(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16(), batch -> batch.fixed(0, values));

        assertThat(statistics.minValue()).isNull();
        assertThat(statistics.maxValue()).isNull();
        assertThat(statistics.nanCount()).isEqualTo(3L);
    }

    /// `null_count` and `nan_count` describe disjoint slots: an absent value is not a `NaN`, and a
    /// present `NaN` is not a null.
    @Test
    void float16CountsNaNSeparatelyFromNulls() throws Exception {
        // The two null slots carry a value the bounds would show if a null were ever counted.
        byte[][] values = { half(1.0f), half(Float.NaN), half(-99.0f), half(Float.NaN),
                half(2.0f), half(-99.0f) };
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 2,
                        LogicalType.float16())
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values,
                    Validity.ofNulls(new boolean[] { false, false, true, false, false, true })));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics statistics = columnMeta(reader, 0).statistics();
            assertThat(statistics.nullCount()).isEqualTo(2L);
            assertThat(statistics.nanCount()).isEqualTo(2L);
            assertThat(toHalf(statistics.minValue())).isEqualTo(1.0f);
            assertThat(toHalf(statistics.maxValue())).isEqualTo(2.0f);
        }
    }

    /// The format requires `column_orders` wherever bounds are written, one entry per leaf
    /// column, without which their meaning is undefined.
    @Test
    void footerDeclaresATypeDefinedOrderPerLeafColumn() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, group -> group
                        .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                LogicalType.string()))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints(0, new int[] { 1 })
                    .struct("s", Validity.ofNulls(new boolean[] { false }))
                    .bytes("s.b", new byte[][] { "x".getBytes(StandardCharsets.UTF_8) },
                            Validity.ofNulls(new boolean[] { false })));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileMetaData().columnOrders())
                    .containsExactly(ColumnOrder.TYPE_DEFINED_ORDER, ColumnOrder.TYPE_DEFINED_ORDER);
        }
    }

    private interface BatchFiller {
        void fill(ColumnBatch batch);
    }

    private static Statistics writeAndReadStatistics(PhysicalType type, Integer typeLength,
                                                     LogicalType logicalType, BatchFiller filler)
            throws Exception {
        return writeAndReadStatistics(type, typeLength, logicalType, WriterConfig.defaults(), filler);
    }

    private static Statistics writeAndReadStatistics(PhysicalType type, Integer typeLength,
                                                     LogicalType logicalType, WriterConfig config,
                                                     BatchFiller filler) throws Exception {
        FileSchema.Builder builder = FileSchema.builder("schema");
        if (typeLength == null) {
            builder.addColumn("v", type, RepetitionType.REQUIRED, logicalType);
        }
        else {
            builder.addColumn("v", type, RepetitionType.REQUIRED, typeLength, logicalType);
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, builder.build(), config)) {
            writer.columnWriter().writeBatch(filler::fill);
        }

        try (ParquetFileReader reader = openReader(out)) {
            return columnMeta(reader, 0).statistics();
        }
    }

    private static byte[] hex(String value) {
        return HexFormat.of().parseHex(value);
    }

    private static byte[] half(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[] { (byte) bits, (byte) (bits >>> 8) };
    }

    private static float toHalf(byte[] value) {
        return Float.float16ToFloat((short) ((value[1] & 0xFF) << 8 | value[0] & 0xFF));
    }

    private static int toInt(byte[] value) {
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static long toLong(byte[] value) {
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static ParquetFileReader openReader(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }

    private static ColumnMetaData columnMeta(ParquetFileReader reader, int columnIndex) {
        return reader.getFileMetaData().rowGroups().get(0).columns().get(columnIndex).metaData();
    }
}
