/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.writer.WriterTestSupport.expectedNullable;
import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static dev.hardwood.writer.WriterTestSupport.readInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static dev.hardwood.writer.WriterTestSupport.twoColumns;
import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip tests for a flat column: write with [ParquetFileWriter], read back with
/// [ParquetFileReader], and assert the values and null positions survive.
///
/// This is the values half only. What the columnar API refuses is in
/// [WriterBatchContractTest], how the bytes are laid out in [WriterLayoutTest], and the
/// encoding, codec, statistics and nested shapes in the classes named for them.
class WriterRoundTripTest {

    @Test
    void writesAndReadsBackTwoIntColumns() throws Exception {
        int[] a = { 1, 2, 3, 4, 5 };
        int[] b = { 10, 20, 30, 40, 50 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, twoColumns())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, a).ints(1, b));
        }

        ByteBuffer bytes = out.buffer();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(bytes))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(reader.getFileMetaData().createdBy()).isEqualTo(ParquetFileWriter.DEFAULT_CREATED_BY);
            assertThat(reader.getFileSchema().getColumnCount()).isEqualTo(2);
            assertThat(reader.getFileSchema().isFlatSchema()).isTrue();

            assertThat(readInts(reader, 0)).containsExactly(a);
            assertThat(readInts(reader, 1)).containsExactly(b);
        }
    }

    @Test
    void writesColumnsAddressedByName() throws Exception {
        int[] a = { 1, 2, 3 };
        int[] b = { 4, 5, 6 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, twoColumns())) {
            // Names may be given in any order; they resolve to the schema's columns.
            writer.columnWriter().writeBatch(batch -> batch.ints("b", b).ints("a", a));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(readInts(reader, 0)).containsExactly(a);
            assertThat(readInts(reader, 1)).containsExactly(b);
        }
    }

    @Test
    void writesToLocalFileWithAtomicRename(@TempDir Path dir) throws Exception {
        int[] ids = { 7, 8, 9 };

        Path file = dir.resolve("out.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, ids));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(readInts(reader, 0)).containsExactly(ids);
        }
    }

    @Test
    void multipleBatchesAccumulateIntoOneRowGroup() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2 }));
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 3, 4 }));
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 5 }));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            // Default 128 MiB target: the three small batches stay in one row group.
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            assertThat(readInts(reader, 0)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void emptyBatchProducesEmptyFile() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[0]));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(0);
            assertThat(reader.getFileMetaData().rowGroups()).isEmpty();
        }
    }

    @Test
    void writesAndReadsBackNullableColumn() throws Exception {
        // Interior and edge nulls, and both signed extremes at present positions.
        int[] values = { 7, 0, -3, 0, Integer.MIN_VALUE, 0, Integer.MAX_VALUE };
        boolean[] nulls = { false, true, false, true, false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(readNullable(reader, 0)).containsExactly(7, null, -3, null, Integer.MIN_VALUE, null, Integer.MAX_VALUE);
        }
    }

    @Test
    void allPresentOptionalColumnReadsBackWithoutNulls() throws Exception {
        // An OPTIONAL column supplied through the mask-less setter: every row is present,
        // so the def levels collapse to a single RLE run and the reader reports no nulls.
        int[] values = { 1, 2, 3, 4, 5 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(0)) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLeafValidity().hasNulls()).isFalse();
            assertThat(Arrays.copyOf(column.getInts(), column.getRecordCount())).containsExactly(values);
        }
    }

    @Test
    void writesAndReadsBackAllNullColumn() throws Exception {
        boolean[] nulls = { true, true, true, true };
        int[] values = new int[nulls.length];

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(4);
            assertThat(readNullable(reader, 0)).containsExactly(null, null, null, null);
        }
    }

    @Test
    void nullsSurvivePageBoundaries() throws Exception {
        // A tiny page target forces many pages; every third row is null, so nulls and
        // values straddle page boundaries.
        int n = 10_000;
        int[] values = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
            nulls[i] = i % 3 == 0;
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(256).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(readNullable(reader, 0)).isEqualTo(expectedNullable(values, nulls));
        }
    }

    @Test
    void rewritesNullableColumnFromFixtureByPassingValidityThrough() throws Exception {
        // Read a PyArrow-produced file whose OPTIONAL INT32 column has interior and trailing
        // nulls, then write that column back by handing the reader's Validity straight to the
        // writer — the read-to-write passthrough the Validity seam exists for. Reading the
        // rewritten file must reproduce the original values and null positions exactly.
        Path source = Path.of("src/test/resources/nullable_primitives_test.parquet");

        Integer[] original;
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source))) {
            int col = reader.getFileSchema().getColumn("nullable_int").columnIndex();
            assertThat(reader.getFileSchema().getColumn(col).type()).isEqualTo(PhysicalType.INT32);
            assertThat(reader.getFileSchema().getColumn(col).repetitionType()).isEqualTo(RepetitionType.OPTIONAL);

            original = new Integer[Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn());
                    ColumnReader column = reader.columnReader(col)) {
                while (column.nextBatch()) {
                    int count = column.getRecordCount();
                    int[] values = Arrays.copyOf(column.getInts(), count);
                    Validity validity = column.getLeafValidity();
                    for (int i = 0; i < count; i++) {
                        original[pos + i] = validity.isNull(i) ? null : values[i];
                    }
                    // Hand the reader's Validity straight to the writer, no re-derivation.
                    writer.columnWriter().writeBatch(batch -> batch.ints(0, values, validity));
                    pos += count;
                }
            }
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(readNullable(reader, 0)).containsExactly(original);
        }
    }

    @Test
    void nullsSurviveRowGroupBoundaries() throws Exception {
        int n = 5_000;
        int[] values = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = i * 2;
            nulls[i] = (i & 1) == 1;
        }

        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(4096).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            assertThat(readNullable(reader, 0)).isEqualTo(expectedNullable(values, nulls));
        }
    }

    @Test
    void writesNullableColumnViaValidity() throws Exception {
        // Drive the primary Validity overload directly with a hand-built dense present
        // bitmap (set-bit = present): rows 0 and 2 present, row 1 null.
        int[] values = { 10, 20, 30 };
        Validity nulls = Validity.of(new long[] { 0b101 });

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(readNullable(reader, 0)).containsExactly(10, null, 30);
        }
    }

    @Test
    void optionalColumnViaNoNullsValidityReadsBackWithoutNulls() throws Exception {
        int[] values = { 1, 2, 3 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, Validity.NO_NULLS));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(0)) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLeafValidity().hasNulls()).isFalse();
            assertThat(Arrays.copyOf(column.getInts(), column.getRecordCount())).containsExactly(values);
        }
    }
}
