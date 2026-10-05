/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.readInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Row groups the caller ends through [ParquetFileWriter#endRowGroup()].
///
/// A caller-placed boundary is an additional cut beside the row-group targets, not a second way
/// of writing a row group: a group it closes is byte-for-byte the group a target closes after the
/// same records, through either write API.
class WriterRowGroupBoundaryTest {

    private static final String FAILED = "A previous write failed; the writer accepts no more data";

    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
            .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
            .build();

    @Test
    void explicitBoundariesBandTheFile() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            writeRange(writer.columnWriter(), 0, 10);
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 10, 25);
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 35, 5);
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(rowCounts(reader)).containsExactly(10L, 25L, 5L);
            assertThat(readInts(reader, 0)).isEqualTo(ids(0, 40));
            assertThat(readNullable(reader, 1)).isEqualTo(nullableValues(0, 40));
        }
    }

    @Test
    void rowWriterBoundaryClosesTheGroupAfterTheStagedRecords() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            writeRows(writer.rowWriter(), 0, 10);
            writer.endRowGroup();
            writeRows(writer.rowWriter(), 10, 3);
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(rowCounts(reader)).containsExactly(10L, 3L);
            assertThat(readInts(reader, 0)).isEqualTo(ids(0, 13));
        }
    }

    @Test
    void endingARowGroupWithNothingWrittenWritesNone() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 0, 10);
            writer.endRowGroup();
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 10, 10);
            writer.endRowGroup();
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(rowCounts(reader)).containsExactly(10L, 10L);
        }
    }

    @Test
    void endingARowGroupOfAFileWithNoRecordsWritesNone() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            writer.endRowGroup();
            writer.endRowGroup();
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(reader.getFileMetaData().numRows()).isZero();
            assertThat(reader.getFileMetaData().rowGroups()).isEmpty();
        }
    }

    /// The same records in the same row groups, cut once by the row target and once by the
    /// caller, produce the same bytes.
    @Test
    void anExplicitCutWritesTheGroupATargetCutWrites() throws Exception {
        InMemoryOutputFile byTarget = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(byTarget, SCHEMA,
                WriterConfig.builder().rowGroupTargetRows(1_000).build())) {
            writeRange(writer.columnWriter(), 0, 3_000);
        }

        InMemoryOutputFile byCaller = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(byCaller, SCHEMA)) {
            for (int group = 0; group < 3; group++) {
                writeRange(writer.columnWriter(), group * 1_000, 1_000);
                writer.endRowGroup();
            }
        }

        try (ParquetFileReader reader = open(byCaller)) {
            assertThat(rowCounts(reader)).containsExactly(1_000L, 1_000L, 1_000L);
        }
        assertThat(InMemoryFiles.toByteArray(byCaller)).isEqualTo(InMemoryFiles.toByteArray(byTarget));
    }

    @Test
    void explicitCutsAndTargetCutsCombine() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA,
                WriterConfig.builder().rowGroupTargetRows(100).build())) {
            writeRange(writer.columnWriter(), 0, 250);
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 250, 30);
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(rowCounts(reader)).containsExactly(100L, 100L, 50L, 30L);
            assertThat(readInts(reader, 0)).isEqualTo(ids(0, 280));
        }
    }

    @Test
    void bothWriteApisProduceTheSameFileAcrossBoundaries() throws Exception {
        InMemoryOutputFile columnar = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(columnar, SCHEMA)) {
            writeRange(writer.columnWriter(), 0, 1_500);
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 1_500, 700);
            writer.endRowGroup();
            writeRange(writer.columnWriter(), 2_200, 2_100);
        }

        InMemoryOutputFile rows = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(rows, SCHEMA)) {
            writeRows(writer.rowWriter(), 0, 1_500);
            writer.endRowGroup();
            writeRows(writer.rowWriter(), 1_500, 700);
            writer.endRowGroup();
            writeRows(writer.rowWriter(), 2_200, 2_100);
        }

        try (ParquetFileReader reader = open(rows)) {
            assertThat(rowCounts(reader)).containsExactly(1_500L, 700L, 2_100L);
        }
        assertThat(InMemoryFiles.toByteArray(rows)).isEqualTo(InMemoryFiles.toByteArray(columnar));
    }

    /// A boundary placed while a record or batch is being filled would cut that record or batch
    /// off from the rows it belongs to, so it is rejected, and the exception out of the filler
    /// fails the writer.
    @Test
    void endingARowGroupFromInsideARecordFillerIsRejected() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            RowWriter rows = writer.rowWriter();
            writeRows(rows, 0, 5);
            assertThatThrownBy(() -> rows.writeRow(record -> {
                record.setInt("id", 5);
                try {
                    writer.endRowGroup();
                }
                catch (IOException e) {
                    throw new AssertionError(e);
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("endRowGroup() was called while a batch or record is being filled; "
                            + "call it between writes");
            assertThatThrownBy(() -> writeRows(rows, 5, 1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    @Test
    void endingARowGroupFromInsideABatchFillerIsRejected() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> {
                try {
                    writer.endRowGroup();
                }
                catch (IOException e) {
                    throw new AssertionError(e);
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("endRowGroup() was called while a batch or record is being filled; "
                            + "call it between writes");
        }

        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    @Test
    void closingFromInsideARecordFillerIsRejected() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            assertThatThrownBy(() -> writer.rowWriter().writeRow(record -> {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    throw new AssertionError(e);
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("close() was called while a batch or record is being filled; call it between writes");
        }

        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    @Test
    void endingARowGroupOnAClosedWriterIsRejected() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), SCHEMA);
        writer.close();

        assertThatThrownBy(writer::endRowGroup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Writer is closed");
    }

    private static void writeRange(ColumnWriter columns, int from, int count) throws IOException {
        int[] ids = ids(from, count);
        int[] values = new int[count];
        boolean[] nulls = new boolean[count];
        for (int i = 0; i < count; i++) {
            int row = from + i;
            nulls[i] = isNull(row);
            values[i] = nulls[i] ? 0 : value(row);
        }
        columns.writeBatch(batch -> batch.ints("id", ids).ints("v", values, nulls));
    }

    private static void writeRows(RowWriter rows, int from, int count) throws IOException {
        for (int row = from; row < from + count; row++) {
            int r = row;
            rows.writeRow(record -> {
                record.setInt("id", r);
                if (!isNull(r)) {
                    record.setInt("v", value(r));
                }
            });
        }
    }

    private static boolean isNull(int row) {
        return row % 7 == 0;
    }

    private static int value(int row) {
        return row % 13;
    }

    private static int[] ids(int from, int count) {
        int[] ids = new int[count];
        for (int i = 0; i < count; i++) {
            ids[i] = from + i;
        }
        return ids;
    }

    private static Integer[] nullableValues(int from, int count) {
        Integer[] values = new Integer[count];
        for (int i = 0; i < count; i++) {
            values[i] = isNull(from + i) ? null : value(from + i);
        }
        return values;
    }

    private static ParquetFileReader open(InMemoryOutputFile out) throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(InMemoryFiles.toByteArray(out))));
    }

    private static List<Long> rowCounts(ParquetFileReader reader) {
        return reader.getFileMetaData().rowGroups().stream().map(RowGroup::numRows).toList();
    }

}
