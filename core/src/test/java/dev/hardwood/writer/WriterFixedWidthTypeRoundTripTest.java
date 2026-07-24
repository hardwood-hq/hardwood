/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip tests for the stage-12a fixed-width primitive types (`BOOLEAN`, `INT64`, `FLOAT`,
/// `DOUBLE`): write with [ParquetFileWriter], read back with [ParquetFileReader], and assert the
/// values, null positions, and statistics survive. `INT32` is covered by [WriterRoundTripTest].
class WriterFixedWidthTypeRoundTripTest {

    @Test
    void writesAndReadsBackEveryFixedWidthTypeInOneFile() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("b", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();

        boolean[] b = { true, false, true, true, false };
        int[] i = { 1, 2, 3, 4, 5 };
        long[] l = { 10L, Long.MIN_VALUE, Long.MAX_VALUE, -7L, 0L };
        float[] f = { 1.5f, -2.5f, Float.MIN_VALUE, Float.MAX_VALUE, 0.0f };
        double[] d = { 1.5, -2.5, Double.MIN_VALUE, Double.MAX_VALUE, 0.0 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.booleans(0, b).ints(1, i).longs(2, l).floats(3, f).doubles(4, d));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(readBooleans(reader, 0)).containsExactly(b);
            try (ColumnReader c = reader.columnReader(2)) {
                c.nextBatch();
                assertThat(Arrays.copyOf(c.getLongs(), c.getValueCount())).containsExactly(l);
            }
            try (ColumnReader c = reader.columnReader(3)) {
                c.nextBatch();
                assertThat(Arrays.copyOf(c.getFloats(), c.getValueCount())).containsExactly(f);
            }
            try (ColumnReader c = reader.columnReader(4)) {
                c.nextBatch();
                assertThat(Arrays.copyOf(c.getDoubles(), c.getValueCount())).containsExactly(d);
            }
        }
    }

    @Test
    void writesAndReadsBackNullableLongColumn() throws Exception {
        long[] values = { 7L, 0L, -3L, 0L, Long.MIN_VALUE, 0L, Long.MAX_VALUE };
        boolean[] nulls = { false, true, false, true, false, true, false };

        FileSchema schema = oneColumn("v", PhysicalType.INT64, RepetitionType.OPTIONAL);
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.longs(0, values, nulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Long[] result = new Long[values.length];
            try (ColumnReader c = reader.columnReader(0)) {
                int pos = 0;
                while (c.nextBatch()) {
                    int count = c.getRecordCount();
                    long[] batch = c.getLongs();
                    Validity validity = c.getLeafValidity();
                    for (int j = 0; j < count; j++) {
                        result[pos + j] = validity.isNull(j) ? null : batch[j];
                    }
                    pos += count;
                }
            }
            assertThat(result).containsExactly(7L, null, -3L, null, Long.MIN_VALUE, null, Long.MAX_VALUE);
        }
    }

    @Test
    void writesAndReadsBackNestedDoubleInList() throws Exception {
        // optional group prices (LIST) { repeated group list { optional double element } }
        FileSchema schema = FileSchema.builder("schema")
                .list("prices", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.DOUBLE, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        double[] elements = { 1.5, 2.5, 3.5, 0.0, 5.5 };
        boolean[] elementNulls = { false, false, false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("prices", offsets, listNulls)
                    .doubles("prices.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            int leaf = reader.getFileSchema().getColumn("prices.list.element").columnIndex();
            try (ColumnReader c = reader.columnReader(leaf)) {
                c.nextBatch();
                int[] listOffsets = c.getLayerOffsets(0);
                Validity listValidity = c.getLayerValidity(0);
                double[] values = c.getDoubles();
                Validity leafValidity = c.getLeafValidity();
                List<List<Double>> got = new ArrayList<>();
                for (int r = 0; r < c.getRecordCount(); r++) {
                    if (listValidity.isNull(r)) {
                        got.add(null);
                        continue;
                    }
                    List<Double> entry = new ArrayList<>();
                    for (int e = listOffsets[r]; e < listOffsets[r + 1]; e++) {
                        entry.add(leafValidity.isNull(e) ? null : values[e]);
                    }
                    got.add(entry);
                }
                assertThat(got).containsExactly(List.of(1.5, 2.5), List.of(), null, Arrays.asList(3.5, null, 5.5));
            }
        }
    }

    @Test
    void dictionaryEncodesLowCardinalityLongs() throws Exception {
        // Few distinct values over many rows: the chunk stays dictionary-encoded.
        long[] values = new long[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i % 4) * 100L;
        }

        FileSchema schema = oneColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED);
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.longs(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.dictionaryPageOffset()).isNotNull();
            try (ColumnReader c = reader.columnReader(0)) {
                long[] got = new long[values.length];
                int pos = 0;
                while (c.nextBatch()) {
                    int count = c.getValueCount();
                    System.arraycopy(c.getLongs(), 0, got, pos, count);
                    pos += count;
                }
                assertThat(got).containsExactly(values);
            }
        }
    }

    @Test
    void disablingDictionaryWritesPlainDoubles() throws Exception {
        double[] values = { 1.0, 1.0, 2.0, 2.0 };
        FileSchema schema = oneColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED);
        WriterConfig config = WriterConfig.builder().enableDictionary(false).build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.writeBatch(batch -> batch.doubles(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNull();
            try (ColumnReader c = reader.columnReader(0)) {
                c.nextBatch();
                assertThat(Arrays.copyOf(c.getDoubles(), c.getValueCount())).containsExactly(values);
            }
        }
    }

    @Test
    void booleansAreNeverDictionaryEncoded() throws Exception {
        boolean[] values = new boolean[500];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i & 1) == 0;
        }
        FileSchema schema = oneColumn("v", PhysicalType.BOOLEAN, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.booleans(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            // Even with dictionary encoding on by default, a boolean chunk carries no dictionary.
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNull();
            assertThat(readBooleans(reader, 0)).containsExactly(values);
        }
    }

    @Test
    void longStatisticsSpanPresentValuesAndCountNulls() throws Exception {
        long[] values = { 7L, 0L, -3L, 0L, 50L, 0L, 20L };
        boolean[] nulls = { false, true, false, true, false, true, false };

        FileSchema schema = oneColumn("v", PhysicalType.INT64, RepetitionType.OPTIONAL);
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.longs(0, values, nulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(StatisticsDecoder.decodeLong(stats.minValue())).isEqualTo(-3L);
            assertThat(StatisticsDecoder.decodeLong(stats.maxValue())).isEqualTo(50L);
            assertThat(stats.nullCount()).isEqualTo(3L);
        }
    }

    @Test
    void doubleStatisticsExcludeNaNFromBounds() throws Exception {
        double[] values = { Double.NaN, 1.5, -2.5, Double.NaN, 3.5 };
        FileSchema schema = oneColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.doubles(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(StatisticsDecoder.decodeDouble(stats.minValue())).isEqualTo(-2.5);
            assertThat(StatisticsDecoder.decodeDouble(stats.maxValue())).isEqualTo(3.5);
            assertThat(stats.nullCount()).isEqualTo(0L);
        }
    }

    @Test
    void allNaNDoubleColumnHasNoBounds() throws Exception {
        double[] values = { Double.NaN, Double.NaN, Double.NaN };
        FileSchema schema = oneColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.doubles(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.minValue()).isNull();
            assertThat(stats.maxValue()).isNull();
        }
    }

    @Test
    void doubleStatisticsNormalizeSignedZero() throws Exception {
        // Only zeros present: the min bound is written as -0.0 and the max as +0.0, so a reader's
        // [min, max] test brackets either signed zero.
        double[] values = { 0.0, 0.0 };
        FileSchema schema = oneColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.doubles(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(Double.doubleToRawLongBits(StatisticsDecoder.decodeDouble(stats.minValue())))
                    .isEqualTo(Double.doubleToRawLongBits(-0.0));
            assertThat(Double.doubleToRawLongBits(StatisticsDecoder.decodeDouble(stats.maxValue())))
                    .isEqualTo(Double.doubleToRawLongBits(0.0));
        }
    }

    @Test
    void floatStatisticsExcludeNaNFromBounds() throws Exception {
        float[] values = { Float.NaN, 1.5f, -2.5f, Float.NaN, 3.5f };
        FileSchema schema = oneColumn("v", PhysicalType.FLOAT, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.floats(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(StatisticsDecoder.decodeFloat(stats.minValue())).isEqualTo(-2.5f);
            assertThat(StatisticsDecoder.decodeFloat(stats.maxValue())).isEqualTo(3.5f);
            assertThat(stats.nullCount()).isEqualTo(0L);
        }
    }

    @Test
    void allNaNFloatColumnHasNoBounds() throws Exception {
        float[] values = { Float.NaN, Float.NaN, Float.NaN };
        FileSchema schema = oneColumn("v", PhysicalType.FLOAT, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.floats(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.minValue()).isNull();
            assertThat(stats.maxValue()).isNull();
        }
    }

    @Test
    void floatStatisticsNormalizeSignedZero() throws Exception {
        // Only zeros present: min bound written as -0.0f, max as +0.0f.
        float[] values = { 0.0f, 0.0f };
        FileSchema schema = oneColumn("v", PhysicalType.FLOAT, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.floats(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(Float.floatToRawIntBits(StatisticsDecoder.decodeFloat(stats.minValue())))
                    .isEqualTo(Float.floatToRawIntBits(-0.0f));
            assertThat(Float.floatToRawIntBits(StatisticsDecoder.decodeFloat(stats.maxValue())))
                    .isEqualTo(Float.floatToRawIntBits(0.0f));
        }
    }

    @Test
    void writesAndReadsBackNullableBooleanColumn() throws Exception {
        boolean[] values = { true, false, true, false, true };
        boolean[] nulls = { false, true, false, true, false };
        FileSchema schema = oneColumn("v", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.booleans(0, values, nulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Boolean[] result = new Boolean[values.length];
            try (ColumnReader c = reader.columnReader(0)) {
                int pos = 0;
                while (c.nextBatch()) {
                    int count = c.getRecordCount();
                    boolean[] batch = c.getBooleans();
                    Validity validity = c.getLeafValidity();
                    for (int j = 0; j < count; j++) {
                        result[pos + j] = validity.isNull(j) ? null : batch[j];
                    }
                    pos += count;
                }
            }
            assertThat(result).containsExactly(true, null, true, null, true);
            // The two null rows are counted and never appear in the bounds.
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.nullCount()).isEqualTo(2L);
            assertThat(StatisticsDecoder.decodeBoolean(stats.minValue())).isTrue();
            assertThat(StatisticsDecoder.decodeBoolean(stats.maxValue())).isTrue();
        }
    }

    @Test
    void booleanStatisticsRecordFalseAndTrue() throws Exception {
        boolean[] values = { true, true, true }; // no false present
        FileSchema schema = oneColumn("v", PhysicalType.BOOLEAN, RepetitionType.REQUIRED);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.booleans(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(StatisticsDecoder.decodeBoolean(stats.minValue())).isTrue();
            assertThat(StatisticsDecoder.decodeBoolean(stats.maxValue())).isTrue();
        }
    }

    private static FileSchema oneColumn(String name, PhysicalType type, RepetitionType repetition) {
        return FileSchema.builder("schema").addColumn(name, type, repetition).build();
    }

    private static ParquetFileReader openReader(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }

    private static ColumnMetaData columnMeta(ParquetFileReader reader, int columnIndex) {
        return reader.getFileMetaData().rowGroups().get(0).columns().get(columnIndex).metaData();
    }

    private static boolean[] readBooleans(ParquetFileReader reader, int columnIndex) {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            boolean[] result = new boolean[Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                System.arraycopy(column.getBooleans(), 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }
}
