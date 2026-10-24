/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A null predicate on a struct that is required from the root is decided without statistics:
/// the struct is present on every row. Its only leaf has a maximum definition level of 1, for
/// which no writer records a definition level histogram, so nothing in the file could decide it
/// otherwise (#1501).
class RequiredGroupNullPruningTest {

    private static byte[] file;
    private static List<RowGroup> rowGroups;

    /// Two row groups of `id` and a required `s` whose optional `zip` is null on every other row.
    @BeforeAll
    static void writeFile() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            for (int r = 0; r < 4; r++) {
                int id = r;
                rows.writeRow(row -> row.setInt("id", id)
                        .setStruct("s", s -> s.setString("zip", id % 2 == 0 ? null : "z" + id)));
                if (r == 1) {
                    writer.endRowGroup();
                }
            }
        }
        file = InMemoryFiles.toByteArray(out);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            rowGroups = reader.getFileMetaData().rowGroups();
        }
        assertThat(rowGroups).hasSize(2);
    }

    @Test
    void skipsEveryRowGroupForIsNull() throws Exception {
        CountingInputFile input = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(ids(input, FilterPredicate.isNull("s"))).isEmpty();
        assertThat(input.reads()).noneMatch(RequiredGroupNullPruningTest::touchesData);
    }

    @Test
    void skipsEveryRowGroupForTheNegationOfIsNotNull() throws Exception {
        CountingInputFile input = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(ids(input, FilterPredicate.not(FilterPredicate.isNotNull("s")))).isEmpty();
        assertThat(input.reads()).noneMatch(RequiredGroupNullPruningTest::touchesData);
    }

    /// A present `s` whose `zip` is null is still a present `s`, under the predicate and under the
    /// negation of its counterpart.
    @Test
    void keepsEveryRowForIsNotNull() throws Exception {
        assertThat(ids(InputFile.of(ByteBuffer.wrap(file)), FilterPredicate.isNotNull("s")))
                .containsExactly(0, 1, 2, 3);
        assertThat(ids(InputFile.of(ByteBuffer.wrap(file)), FilterPredicate.not(FilterPredicate.isNull("s"))))
                .containsExactly(0, 1, 2, 3);
    }

    /// `isNotNull` fully matches every row group, so a projection without `s.zip` never fetches
    /// that column.
    @Test
    void readsNoFilterOnlyColumnForIsNotNull() throws Exception {
        CountingInputFile input = new CountingInputFile(ByteBuffer.wrap(file));
        List<Integer> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(input);
                RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.isNotNull("s")).build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt("id"));
            }
        }
        assertThat(ids).containsExactly(0, 1, 2, 3);
        assertThat(input.reads()).noneMatch(read -> touchesColumn(read, 1));
        assertThat(input.reads()).anyMatch(read -> touchesColumn(read, 0));
    }

    /// The column reader answers the same rows, also where an `or` leaves the null test to the
    /// record-level matcher, which has no struct view of a leaf decoded by index.
    @Test
    void answersTheSameThroughTheColumnReader() throws Exception {
        assertThat(columnIds(FilterPredicate.isNull("s"))).isEmpty();
        assertThat(columnIds(FilterPredicate.isNotNull("s"))).containsExactly(0, 1, 2, 3);
        assertThat(columnIds(FilterPredicate.or(FilterPredicate.isNull("s"), FilterPredicate.eq("id", 2))))
                .containsExactly(2);
        assertThat(columnIds(FilterPredicate.and(FilterPredicate.isNotNull("s"), FilterPredicate.lt("id", 2))))
                .containsExactly(0, 1);
    }

    private static List<Integer> columnIds(FilterPredicate filter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                ColumnReader column = reader.buildColumnReader("id").filter(filter).build()) {
            while (column.nextBatch()) {
                int[] values = column.getInts();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    ids.add(values[i]);
                }
            }
        }
        return ids;
    }

    private static List<Integer> ids(InputFile input, FilterPredicate filter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(input);
                RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt("id"));
            }
        }
        return ids;
    }

    /// Whether `read` overlaps the pages of column `column` in any row group.
    private static boolean touchesColumn(CountingInputFile.Read read, int column) {
        for (RowGroup rowGroup : rowGroups) {
            if (overlaps(read, rowGroup.columns().get(column))) {
                return true;
            }
        }
        return false;
    }

    /// Whether `read` overlaps the pages of any column chunk.
    private static boolean touchesData(CountingInputFile.Read read) {
        for (RowGroup rowGroup : rowGroups) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                if (overlaps(read, chunk)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean overlaps(CountingInputFile.Read read, ColumnChunk chunk) {
        long start = chunk.metaData().dictionaryPageOffset() != null
                ? chunk.metaData().dictionaryPageOffset()
                : chunk.metaData().dataPageOffset();
        long end = start + chunk.metaData().totalCompressedSize();
        return read.offset() < end && read.end() > start;
    }
}
