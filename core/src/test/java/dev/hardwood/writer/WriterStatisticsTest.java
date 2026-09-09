/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.columnMeta;
import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static org.assertj.core.api.Assertions.assertThat;

/// The column statistics a chunk carries: bounds over its present values, a null count over
/// all of them, and one set per row group rather than one per file.
///
/// The sort order those bounds are computed in is the annotation's rather than the physical
/// type's wherever an annotation redefines it, which is [WriterLogicalTypeStatisticsTest].
class WriterStatisticsTest {

    @Test
    void writesMinMaxAndNullCountForRequiredColumn() throws Exception {
        // Signed bounds: the extremes must sort as MIN < ... < MAX, matching the INT32
        // type-defined (signed) ColumnOrder.
        int[] values = { 42, -100_000, 7, Integer.MAX_VALUE, Integer.MIN_VALUE, 0 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats).as("statistics written").isNotNull();
            assertThat(StatisticsDecoder.decodeInt(stats.minValue())).isEqualTo(Integer.MIN_VALUE);
            assertThat(StatisticsDecoder.decodeInt(stats.maxValue())).isEqualTo(Integer.MAX_VALUE);
            assertThat(stats.nullCount()).isEqualTo(0L);
            // Written through the preferred min_value/max_value fields, not the deprecated ones.
            assertThat(stats.isMinMaxDeprecated()).isFalse();
        }
    }

    @Test
    void boundsSpanOnlyPresentValuesAndNullsAreCounted() throws Exception {
        // The three null rows are counted but never widen the bounds, which cover 7..50.
        int[] values = { 7, 0, -3, 0, 50, 0, 20 };
        boolean[] nulls = { false, true, false, true, false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(StatisticsDecoder.decodeInt(stats.minValue())).isEqualTo(-3);
            assertThat(StatisticsDecoder.decodeInt(stats.maxValue())).isEqualTo(50);
            assertThat(stats.nullCount()).isEqualTo(3L);
        }
    }

    @Test
    void allNullColumnWritesNullCountButNoBounds() throws Exception {
        // No present value, so the chunk carries a null count but omits min/max.
        boolean[] nulls = { true, true, true, true };
        int[] values = new int[nulls.length];

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats).isNotNull();
            assertThat(stats.minValue()).isNull();
            assertThat(stats.maxValue()).isNull();
            assertThat(stats.nullCount()).isEqualTo(4L);
        }
    }

    @Test
    void eachRowGroupCarriesItsOwnStatistics() throws Exception {
        // Ascending values, cut every 1024 rows: each chunk's bounds cover only its own slice,
        // so they advance group by group. The row target states the boundary exactly, where a
        // byte target would put it wherever these values happen to retain their target.
        int n = 5_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(1024).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            List<RowGroup> groups = reader.getFileMetaData().rowGroups();
            assertThat(groups.size()).isGreaterThan(1);
            Statistics first = groups.get(0).columns().get(0).metaData().statistics();
            assertThat(StatisticsDecoder.decodeInt(first.minValue())).isEqualTo(0);
            assertThat(StatisticsDecoder.decodeInt(first.maxValue())).isEqualTo(1023);
            Statistics second = groups.get(1).columns().get(0).metaData().statistics();
            assertThat(StatisticsDecoder.decodeInt(second.minValue())).isEqualTo(1024);
            assertThat(StatisticsDecoder.decodeInt(second.maxValue())).isEqualTo(2047);
        }
    }

    @Test
    void listColumnNullCountCountsNullAndEmptyLists() throws Exception {
        // The leaf's null count spans every not-present slot — a null list, an empty list, and
        // a null element — while the bounds cover only the present elements 1..5.
        // record 0: [1,2]; 1: [] (empty); 2: null list; 3: [3, null, 5].
        FileSchema schema = FileSchema.builder("schema")
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int leaf = reader.getFileSchema().getColumn("phones.list.element").columnIndex();
            Statistics stats = columnMeta(reader, leaf).statistics();
            assertThat(StatisticsDecoder.decodeInt(stats.minValue())).isEqualTo(1);
            assertThat(StatisticsDecoder.decodeInt(stats.maxValue())).isEqualTo(5);
            // 1 empty list + 1 null list + 1 null element.
            assertThat(stats.nullCount()).isEqualTo(3L);
        }
    }
}
