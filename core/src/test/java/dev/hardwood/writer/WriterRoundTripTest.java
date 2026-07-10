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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Round-trip tests: write a flat `REQUIRED INT32` file with [ParquetFileWriter],
/// then read it back with [ParquetFileReader] and assert the values survive.
class WriterRoundTripTest {

    private static FileSchema twoColumns() {
        return FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static FileSchema oneOptionalColumn() {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
    }

    private static FileSchema oneColumn() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    @Test
    void writesAndReadsBackTwoIntColumns() throws Exception {
        int[] a = { 1, 2, 3, 4, 5 };
        int[] b = { 10, 20, 30, 40, 50 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, twoColumns())) {
            writer.writeBatch(batch -> batch.ints(0, a).ints(1, b));
        }

        ByteBuffer bytes = ByteBuffer.wrap(out.toByteArray());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(bytes))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(reader.getFileMetaData().createdBy()).isEqualTo("hardwood");
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

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, twoColumns())) {
            // Names may be given in any order; they resolve to the schema's columns.
            writer.writeBatch(batch -> batch.ints("b", b).ints("a", a));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(readInts(reader, 0)).containsExactly(a);
            assertThat(readInts(reader, 1)).containsExactly(b);
        }
    }

    @Test
    void writesToLocalFileWithAtomicRename(@TempDir Path dir) throws Exception {
        int[] ids = { 7, 8, 9 };

        Path file = dir.resolve("out.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.writeBatch(batch -> batch.ints(0, ids));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(readInts(reader, 0)).containsExactly(ids);
        }
    }

    @Test
    void multipleBatchesAccumulateIntoOneRowGroup() throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.writeBatch(batch -> batch.ints(0, new int[] { 1, 2 }));
            writer.writeBatch(batch -> batch.ints(0, new int[] { 3, 4 }));
            writer.writeBatch(batch -> batch.ints(0, new int[] { 5 }));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            // Default 128 MiB target: the three small batches stay in one row group.
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            assertThat(readInts(reader, 0)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void emptyBatchProducesEmptyFile() throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.writeBatch(batch -> batch.ints(0, new int[0]));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(0);
            assertThat(reader.getFileMetaData().rowGroups()).isEmpty();
        }
    }

    @Test
    void rejectsDuplicateColumnInBatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints(0, new int[] { 4, 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsSameColumnByIndexAndName() throws Exception {
        // The schema binding lets the batch see that "id" is column 0, so the collision
        // is caught eagerly rather than at write time.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints("id", new int[] { 4, 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsUnknownColumnName() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch.ints("nope", new int[] { 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsRaggedBatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), twoColumns())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }).ints(1, new int[] { 1, 2 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsBatchNotCoveringAllColumns() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), twoColumns())) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNonInt32Schema() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        assertThatThrownBy(() -> ParquetFileWriter.create(new ByteBufferOutputFile(), schema))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writesAndReadsBackNullableColumn() throws Exception {
        // Interior and edge nulls, and both signed extremes at present positions.
        int[] values = { 7, 0, -3, 0, Integer.MIN_VALUE, 0, Integer.MAX_VALUE };
        boolean[] nulls = { false, true, false, true, false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(readNullable(reader, 0)).containsExactly(7, null, -3, null, Integer.MIN_VALUE, null, Integer.MAX_VALUE);
        }
    }

    @Test
    void allPresentOptionalColumnReadsBackWithoutNulls() throws Exception {
        // An OPTIONAL column supplied through the mask-less setter: every row is present,
        // so the def levels collapse to a single RLE run and the reader reports no nulls.
        int[] values = { 1, 2, 3, 4, 5 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
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

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
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
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
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
        ByteBufferOutputFile out = new ByteBufferOutputFile();
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
                    writer.writeBatch(batch -> batch.ints(0, values, validity));
                    pos += count;
                }
            }
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
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

        WriterConfig config = WriterConfig.builder().rowGroupTargetBytes(4096).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
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

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(readNullable(reader, 0)).containsExactly(10, null, 30);
        }
    }

    @Test
    void optionalColumnViaNoNullsValidityReadsBackWithoutNulls() throws Exception {
        int[] values = { 1, 2, 3 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values, Validity.NO_NULLS));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                ColumnReader column = reader.columnReader(0)) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLeafValidity().hasNulls()).isFalse();
            assertThat(Arrays.copyOf(column.getInts(), column.getRecordCount())).containsExactly(values);
        }
    }

    @Test
    void rejectsNullMaskOnRequiredColumn() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2 }, new boolean[] { false, true })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsValidityOnRequiredColumn() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2 }, Validity.NO_NULLS)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullMaskLengthMismatch() throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneOptionalColumn())) {
            assertThatThrownBy(() -> writer.writeBatch(
                    batch -> batch.ints(0, new int[] { 1, 2, 3 }, new boolean[] { false, true })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsMutatingBatchAfterWrite() throws Exception {
        // A filler that stashes the batch and mutates it after writeBatch returns must
        // fail loudly rather than silently drop the values.
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn())) {
            ColumnBatch[] escaped = new ColumnBatch[1];
            writer.writeBatch(batch -> {
                escaped[0] = batch;
                batch.ints(0, new int[] { 1, 2, 3 });
            });
            assertThatThrownBy(() -> escaped[0].ints(0, new int[] { 4 })).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsUseAfterClose() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn());
        writer.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
        writer.close();
        assertThatThrownBy(() -> writer.writeBatch(batch -> batch.ints(0, new int[] { 4 })))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedCloseLeavesNoFileAtTarget(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partial.parquet");

        // Fail while writing the footer's trailing magic: the data pages and part of the
        // footer are on disk, so close() must discard rather than publish a broken file.
        FailOnSecondMagic out = new FailOnSecondMagic(OutputFile.of(file));
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn());
        writer.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));

        assertThatThrownBy(writer::close).isInstanceOf(IOException.class);
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void largeColumnIsSplitAcrossMultiplePages() throws Exception {
        // Comfortably more than one target page (262,144 INT32 values per 1 MiB page).
        int n = 600_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }
        byte[] bytes = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(n);
            // One 128 MiB row group; 600k values at 262,144 per 1 MiB page ⇒ exactly 3 pages.
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            assertThat(countDataPages(bytes, meta.dataPageOffset(), meta.numValues())).isEqualTo(3);
            // Arrays.equals over containsExactly: the latter is O(n) with per-element
            // boxing/description and is needlessly slow at 600k elements.
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void largeColumnIsSplitAcrossMultipleRowGroups() throws Exception {
        int n = 5_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        // 4 KiB target ⇒ 1024 rows per row group ⇒ the single batch spans several groups.
        WriterConfig config = WriterConfig.builder().rowGroupTargetBytes(4096).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(n);
            // 5000 rows at 1024 per group ⇒ four full groups and a 904-row tail, pinning
            // the cadence arithmetic rather than merely asserting "more than one".
            assertThat(reader.getFileMetaData().rowGroups().stream().map(RowGroup::numRows))
                    .containsExactly(1024L, 1024L, 1024L, 1024L, 904L);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void writesCorrectPageCrc() throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3, 4, 5 }));
        }
        byte[] bytes = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            int offset = Math.toIntExact(meta.dataPageOffset());
            ThriftCompactReader thrift = new ThriftCompactReader(ByteBuffer.wrap(bytes), offset);
            PageHeader header = PageHeaderReader.read(thrift);
            int bodyStart = offset + thrift.getBytesRead();

            assertThat(header.crc()).as("page crc must be written").isNotNull();
            CRC32 crc = new CRC32();
            crc.update(bytes, bodyStart, header.compressedPageSize());
            assertThat(header.crc().intValue()).isEqualTo((int) crc.getValue());
        }
    }

    /// Walks the column chunk's contiguous data pages from `startOffset`, returning
    /// how many pages it took to cover `totalValues`.
    private static int countDataPages(byte[] file, long startOffset, long totalValues) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(file);
        int offset = Math.toIntExact(startOffset);
        long seen = 0;
        int pages = 0;
        while (seen < totalValues) {
            ThriftCompactReader reader = new ThriftCompactReader(buf, offset);
            PageHeader header = PageHeaderReader.read(reader);
            pages++;
            seen += header.dataPageHeader().numValues();
            offset += reader.getBytesRead() + header.compressedPageSize();
        }
        return pages;
    }

    private static int[] readInts(ParquetFileReader reader, int columnIndex) {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            int[] result = new int[reader.getFileMetaData().numRows() < 0 ? 0
                    : Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                int[] batch = column.getInts();
                System.arraycopy(batch, 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }

    /// Reads a flat column back into a boxed array, `null` at each null row, so both the
    /// values and their null positions can be asserted in one comparison.
    private static Integer[] readNullable(ParquetFileReader reader, int columnIndex) {
        int rows = Math.toIntExact(reader.getFileMetaData().numRows());
        Integer[] result = new Integer[rows];
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getRecordCount();
                int[] batch = column.getInts();
                Validity validity = column.getLeafValidity();
                for (int i = 0; i < count; i++) {
                    result[pos + i] = validity.isNull(i) ? null : batch[i];
                }
                pos += count;
            }
        }
        return result;
    }

    @Test
    void writesAndReadsBackStructWithRequiredAndOptionalLeaves() throws Exception {
        // optional group address { required int32 street; optional int32 zip }
        // record 0: address null (street/zip absent); 1: present, zip null; 2,3: fully present.
        FileSchema schema = FileSchema.builder("schema")
                .struct("address", RepetitionType.OPTIONAL, s -> s
                        .addColumn("street", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        Validity addressNulls = Validity.ofNulls(new boolean[] { true, false, false, false });
        int[] street = { 0, 10, 20, 30 };
        int[] zip = { 0, 0, 200, 300 };
        boolean[] zipNulls = { false, true, false, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .struct("address", addressNulls)
                    .ints("address.street", street)
                    .ints("address.zip", zip, zipNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int streetIdx = reader.getFileSchema().getColumn("address.street").columnIndex();
            int zipIdx = reader.getFileSchema().getColumn("address.zip").columnIndex();

            // street is absent only where the struct is null; zip also where zip itself is null.
            assertThat(readNullable(reader, streetIdx)).containsExactly(null, 10, 20, 30);
            assertThat(readNullable(reader, zipIdx)).containsExactly(null, null, 200, 300);

            // The STRUCT layer distinguishes a null struct from a present struct with a null leaf.
            try (ColumnReader column = reader.columnReader(zipIdx)) {
                assertThat(column.nextBatch()).isTrue();
                assertThat(column.getLayerCount()).isEqualTo(1);
                assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
                Validity struct = column.getLayerValidity(0);
                assertThat(struct.isNull(0)).isTrue();
                assertThat(struct.isNull(1)).isFalse();
                assertThat(struct.isNull(2)).isFalse();
            }
        }
    }

    @Test
    void writesAndReadsBackNestedOptionalStructDepthTwo() throws Exception {
        // optional group a { optional int32 b } — definition levels span 0, 1 and 2.
        // record 0: a null (def 0); 1: a present, b null (def 1); 2: fully present (def 2).
        FileSchema schema = FileSchema.builder("schema")
                .struct("a", RepetitionType.OPTIONAL, s -> s
                        .addColumn("b", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        Validity aNulls = Validity.ofNulls(new boolean[] { true, false, false });
        int[] b = { 0, 0, 42 };
        boolean[] bNulls = { false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.struct("a", aNulls).ints("a.b", b, bNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int bIdx = reader.getFileSchema().getColumn("a.b").columnIndex();
            assertThat(reader.getFileSchema().getColumn(bIdx).maxDefinitionLevel()).isEqualTo(2);
            assertThat(readNullable(reader, bIdx)).containsExactly(null, null, 42);

            try (ColumnReader column = reader.columnReader(bIdx)) {
                assertThat(column.nextBatch()).isTrue();
                Validity struct = column.getLayerValidity(0);
                assertThat(struct.isNull(0)).isTrue();  // a null
                assertThat(struct.isNull(1)).isFalse(); // a present (b null)
                assertThat(struct.isNull(2)).isFalse();
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredStruct() throws Exception {
        // required group g { optional int32 x } — no STRUCT layer; x behaves like a flat
        // optional column, but the group nesting must still round-trip through the footer.
        FileSchema schema = FileSchema.builder("schema")
                .struct("g", RepetitionType.REQUIRED, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] x = { 1, 0, 3 };
        boolean[] xNulls = { false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.ints("g.x", x, xNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int xIdx = reader.getFileSchema().getColumn("g.x").columnIndex();
            assertThat(readNullable(reader, xIdx)).containsExactly(1, null, 3);
        }
    }

    @Test
    void writesAndReadsBackListOfInts() throws Exception {
        // optional group phones (LIST) { repeated group list { optional int32 element } }
        // record 0: [1,2]; 1: [] (empty); 2: null (absent list); 3: [3, null, 5].
        FileSchema schema = FileSchema.builder("schema")
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int leaf = reader.getFileSchema().getColumn("phones.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf))
                    .containsExactly(List.of(1, 2), List.of(), null, Arrays.asList(3, null, 5));
        }
    }

    @Test
    void writesAndReadsBackListOfLists() throws Exception {
        // optional [[optional int]] — two repetition levels.
        // record 0: [[1,2],[3]]; 1: []; 2: null; 3: [[]] (one empty inner); 4: [null] (one null inner).
        FileSchema schema = FileSchema.builder("schema")
                .list("m", RepetitionType.OPTIONAL,
                        el -> el.list(RepetitionType.OPTIONAL, inner -> inner.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        int[] outerOffsets = { 0, 2, 2, 2, 3, 4 };
        Validity outerNulls = Validity.ofNulls(new boolean[] { false, false, true, false, false });
        int[] innerOffsets = { 0, 2, 3, 3, 3 };
        Validity innerNulls = Validity.ofNulls(new boolean[] { false, false, false, true });
        int[] elements = { 1, 2, 3 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("m", outerOffsets, outerNulls)
                    .list("m.list.element", innerOffsets, innerNulls)
                    .ints("m.list.element.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("m.list.element.list.element").columnIndex())) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getRecordCount()).isEqualTo(5);
            assertThat(column.getLayerCount()).isEqualTo(2);

            List<List<List<Integer>>> actual = new ArrayList<>();
            int[] outer = column.getLayerOffsets(0);
            int[] inner = column.getLayerOffsets(1);
            Validity outerV = column.getLayerValidity(0);
            Validity innerV = column.getLayerValidity(1);
            int[] values = column.getInts();
            for (int r = 0; r < column.getRecordCount(); r++) {
                if (outerV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                List<List<Integer>> lists = new ArrayList<>();
                for (int i = outer[r]; i < outer[r + 1]; i++) {
                    if (innerV.isNull(i)) {
                        lists.add(null);
                        continue;
                    }
                    List<Integer> ints = new ArrayList<>();
                    for (int e = inner[i]; e < inner[i + 1]; e++) {
                        ints.add(values[e]);
                    }
                    lists.add(ints);
                }
                actual.add(lists);
            }
            assertThat(actual).containsExactly(
                    List.of(List.of(1, 2), List.of(3)),
                    List.of(),
                    null,
                    List.of(List.of()),
                    Arrays.asList((List<Integer>) null));
        }
    }

    @Test
    void writesAndReadsBackListOfStructs() throws Exception {
        // optional [ { required int32 x; optional int32 y } ]
        FileSchema schema = FileSchema.builder("schema")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("y", PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // record 0: [{1,10},{2,null}]; 1: [{3,30}].
        int[] offsets = { 0, 2, 3 };
        int[] x = { 1, 2, 3 };
        int[] y = { 10, 0, 30 };
        boolean[] yNulls = { false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("people", offsets)
                    .ints("people.list.element.x", x)
                    .ints("people.list.element.y", y, yNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int xIdx = reader.getFileSchema().getColumn("people.list.element.x").columnIndex();
            int yIdx = reader.getFileSchema().getColumn("people.list.element.y").columnIndex();
            try (ColumnReader xr = reader.columnReader(xIdx); ColumnReader yr = reader.columnReader(yIdx)) {
                assertThat(xr.nextBatch()).isTrue();
                assertThat(yr.nextBatch()).isTrue();
                assertThat(xr.getLayerOffsets(0)).containsExactly(0, 2, 3);
                assertThat(Arrays.copyOf(xr.getInts(), xr.getValueCount())).containsExactly(1, 2, 3);
                int[] ys = yr.getInts();
                Validity yv = yr.getLeafValidity();
                assertThat(yv.isNull(1)).isTrue();
                assertThat(ys[0]).isEqualTo(10);
                assertThat(ys[2]).isEqualTo(30);
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredList() throws Exception {
        // required list<required int32> — the list itself is never null, so there is no outer
        // optional level; def levels only distinguish an empty list from a present element.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int[] offsets = { 0, 2, 2, 3 }; // record 0: [1,2]; record 1: []; record 2: [3]
        int[] elements = { 1, 2, 3 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch.list("v", offsets).ints("v.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).containsExactly(List.of(1, 2), List.of(), List.of(3));
        }
    }

    @Test
    void writesAndReadsBackListOfStructWithNullStructElement() throws Exception {
        // optional list< optional struct { required int32 x } >, one record whose middle
        // element is a null struct — distinct from an absent list and from a null leaf.
        FileSchema schema = FileSchema.builder("schema")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        int[] offsets = { 0, 3 };
        Validity structNulls = Validity.ofNulls(new boolean[] { false, true, false });
        int[] x = { 1, 0, 3 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("people", offsets)
                    .struct("people.list.element", structNulls)
                    .ints("people.list.element.x", x));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("people.list.element.x").columnIndex())) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getRecordCount()).isEqualTo(1);
            // Three struct-element slots, the middle one an absent struct; the leaf is not
            // compacted, so its slot survives and reads back null with the two present values.
            assertThat(column.getLayerOffsets(0)).containsExactly(0, 3);
            assertThat(column.getValueCount()).isEqualTo(3);
            Validity structValidity = column.getLayerValidity(1);
            assertThat(structValidity.isNull(0)).isFalse();
            assertThat(structValidity.isNull(1)).isTrue();
            assertThat(structValidity.isNull(2)).isFalse();
            Validity leafValidity = column.getLeafValidity();
            assertThat(leafValidity.isNull(1)).isTrue();
            int[] xs = column.getInts();
            assertThat(xs[0]).isEqualTo(1);
            assertThat(xs[2]).isEqualTo(3);
        }
    }

    @Test
    void writesAndReadsBackMapOfIntToInt() throws Exception {
        // optional map<int32, optional int32> props — key/value share one REPEATED layer.
        // record 0: {1:10, 2:null}; 1: {} (empty); 2: null (absent map); 3: {3:30}.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 3 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] keys = { 1, 2, 3 };
        int[] values = { 10, 0, 30 };
        boolean[] valueNulls = { false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .map("props", offsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values, valueNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int valIdx = reader.getFileSchema().getColumn("props.key_value.value").columnIndex();
            assertThat(reader.getFileSchema().getColumn(valIdx).maxDefinitionLevel()).isEqualTo(3);
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader vr = reader.columnReader(valIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(vr.nextBatch()).isTrue();
                assertThat(vr.getRecordCount()).isEqualTo(4);
                assertThat(vr.getLayerCount()).isEqualTo(1);
                assertThat(vr.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
                assertThat(readMapOfInts(kr, vr)).containsExactly(
                        mapOf(1, 10, 2, null), Map.of(), null, mapOf(3, 30, null, null));
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredMap() throws Exception {
        // required map<int32, required int32> — the map itself is never null, so only an
        // empty map (zero-delta) and a present entry are distinguished.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.REQUIRED, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int[] offsets = { 0, 2, 2, 3 }; // record 0: {1:10, 2:20}; 1: {}; 2: {3:30}
        int[] keys = { 1, 2, 3 };
        int[] values = { 10, 20, 30 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .map("props", offsets)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int valIdx = reader.getFileSchema().getColumn("props.key_value.value").columnIndex();
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader vr = reader.columnReader(valIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(vr.nextBatch()).isTrue();
                assertThat(readMapOfInts(kr, vr)).containsExactly(
                        mapOf(1, 10, 2, 20), Map.of(), Map.of(3, 30));
            }
        }
    }

    @Test
    void writesAndReadsBackMapOfIntToListOfInts() throws Exception {
        // optional map<int32, required list<required int32>> — a REPEATED value layer nested
        // inside the MAP's REPEATED layer, two repetition levels driven by two offset arrays.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.list(RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        // record 0: {1:[10,20], 2:[30]}; 1: {} (empty); 2: null (absent map).
        int[] mapOffsets = { 0, 2, 2, 2 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true });
        int[] keys = { 1, 2 };
        int[] listOffsets = { 0, 2, 3 };
        int[] elements = { 10, 20, 30 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .map("props", mapOffsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .list("props.key_value.value", listOffsets)
                    .ints("props.key_value.value.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int elemIdx = reader.getFileSchema().getColumn("props.key_value.value.list.element").columnIndex();
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader er = reader.columnReader(elemIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(er.nextBatch()).isTrue();
                assertThat(er.getLayerCount()).isEqualTo(2);

                List<Map<Integer, List<Integer>>> actual = new ArrayList<>();
                int[] mapOffs = er.getLayerOffsets(0);
                Validity mapV = er.getLayerValidity(0);
                int[] listOffs = er.getLayerOffsets(1);
                int[] ks = kr.getInts();
                int[] vals = er.getInts();
                for (int r = 0; r < er.getRecordCount(); r++) {
                    if (mapV.isNull(r)) {
                        actual.add(null);
                        continue;
                    }
                    Map<Integer, List<Integer>> map = new LinkedHashMap<>();
                    for (int e = mapOffs[r]; e < mapOffs[r + 1]; e++) {
                        List<Integer> list = new ArrayList<>();
                        for (int i = listOffs[e]; i < listOffs[e + 1]; i++) {
                            list.add(vals[i]);
                        }
                        map.put(ks[e], list);
                    }
                    actual.add(map);
                }
                assertThat(actual).containsExactly(
                        Map.of(1, List.of(10, 20), 2, List.of(30)), Map.of(), null);
            }
        }
    }

    @Test
    void rejectsListVerbOnMapPath() throws Exception {
        // A map addressed with the list verb is a wrong-kind facet and must fail eagerly.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch.list("props", new int[] { 0, 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullMapWithNonEmptyOffsets() throws Exception {
        // A null map is absent, so its offset delta must be zero, mirroring the null-list rule.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .map("props", new int[] { 0, 1, 2 }, Validity.ofNulls(new boolean[] { true, false }))
                    .ints("props.key_value.key", new int[] { 99, 5 })
                    .ints("props.key_value.value", new int[] { 1, 2 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// Reconstructs a `MAP` of `INT32` to `INT32` as one map per record — `null` for an
    /// absent map, empty for an empty one, a `null` entry value for a null value. The key
    /// and value columns share the map's REPEATED layer offsets, so entries align by index.
    private static List<Map<Integer, Integer>> readMapOfInts(ColumnReader keys, ColumnReader values) {
        List<Map<Integer, Integer>> out = new ArrayList<>();
        int[] offsets = values.getLayerOffsets(0);
        Validity mapValidity = values.getLayerValidity(0);
        int[] ks = keys.getInts();
        int[] vs = values.getInts();
        Validity valueValidity = values.getLeafValidity();
        for (int r = 0; r < values.getRecordCount(); r++) {
            if (mapValidity.isNull(r)) {
                out.add(null);
                continue;
            }
            Map<Integer, Integer> map = new LinkedHashMap<>();
            for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                map.put(ks[e], valueValidity.isNull(e) ? null : vs[e]);
            }
            out.add(map);
        }
        return out;
    }

    /// Builds a small ordered map of up to two `Integer` entries, allowing a `null` value
    /// (which `Map.of` forbids). A `null` key marks the second entry absent.
    private static Map<Integer, Integer> mapOf(Integer k1, Integer v1, Integer k2, Integer v2) {
        Map<Integer, Integer> map = new LinkedHashMap<>();
        map.put(k1, v1);
        if (k2 != null) {
            map.put(k2, v2);
        }
        return map;
    }

    @Test
    void rejectsNullableStructEnclosingList() throws Exception {
        // A nullable struct directly enclosing a repeated field is not yet supported and must
        // be rejected eagerly rather than shredded into an unverified file.
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, sb -> sb
                        .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .list("s.phones", new int[] { 0, 1 })
                    .ints("s.phones.list.element", new int[] { 1 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNonMonotonicListOffsets() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .list("v", new int[] { 0, 2, 1 })
                    .ints("v.list.element", new int[] { 7 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsListOffsetsDisagreeingWithElementCount() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // offsets claim 2 elements, but only 3 are supplied.
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .list("v", new int[] { 0, 2 })
                    .ints("v.list.element", new int[] { 1, 2, 3 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullListWithNonEmptyOffsets() throws Exception {
        // A null list is absent, so its offset delta must be zero. A non-zero span
        // contradicts the null bit and would silently drop the stray element.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // record 0 is null yet its offsets span one element (99); record 1 is [5].
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .list("v", new int[] { 0, 1, 2 }, Validity.ofNulls(new boolean[] { true, false }))
                    .ints("v.list.element", new int[] { 99, 5 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsColumnsImplyingDifferentRecordCounts() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("r", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            // r has 3 records but v's offsets describe only 2.
            assertThatThrownBy(() -> writer.writeBatch(batch -> batch
                    .ints("r", new int[] { 0, 1, 2 })
                    .list("v", new int[] { 0, 1, 2 })
                    .ints("v.list.element", new int[] { 5, 6 })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void singleLargeListRecordSpansManyPages() throws Exception {
        // One record whose list is far larger than a page: streaming must seal pages part-way
        // through the record, and the reader must reassemble it across pages via rep levels.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int n = 5_000;
        int[] offsets = { 0, n };
        int[] elements = new int[n];
        List<Integer> expected = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            elements[i] = i;
            expected.add(i);
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.writeBatch(batch -> batch.list("v", offsets).ints("v.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(1);
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            assertThat(meta.numValues()).isEqualTo(n); // the single record's elements span multiple pages
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).containsExactly(expected);
        }
    }

    @Test
    void listsSurvivePageAndRowGroupBoundaries() throws Exception {
        // Many records with varying list lengths, absent lists, and interior null elements,
        // written with tiny page and row-group targets so lists straddle both boundaries.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int records = 2_000;
        List<List<Integer>> expected = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        List<Boolean> listNulls = new ArrayList<>();
        List<Integer> elements = new ArrayList<>();
        List<Boolean> elementNulls = new ArrayList<>();
        offsets.add(0);
        int element = 0;
        for (int r = 0; r < records; r++) {
            if (r % 7 == 0) {
                listNulls.add(true);
                expected.add(null);
            }
            else {
                listNulls.add(false);
                List<Integer> list = new ArrayList<>();
                int length = r % 4; // 0..3, so empty and non-empty both occur
                for (int k = 0; k < length; k++) {
                    boolean isNull = element % 5 == 0;
                    int value = r * 10 + k;
                    elements.add(value);
                    elementNulls.add(isNull);
                    list.add(isNull ? null : value);
                    element++;
                }
                expected.add(list);
            }
            offsets.add(elements.size());
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).rowGroupTargetBytes(256).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.writeBatch(batch -> batch
                    .list("v", toIntArray(offsets), Validity.ofNulls(toBooleanArray(listNulls)))
                    .ints("v.list.element", toIntArray(elements), toBooleanArray(elementNulls)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).isEqualTo(expected);
        }
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    private static boolean[] toBooleanArray(List<Boolean> list) {
        boolean[] array = new boolean[list.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    /// Reconstructs a `LIST` of `INT32` as one list per record — `null` for an absent list,
    /// empty for an empty one, a `null` entry for a null element.
    private static List<List<Integer>> readListOfInts(ParquetFileReader reader, int columnIndex) {
        List<List<Integer>> out = new ArrayList<>();
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            while (column.nextBatch()) {
                int records = column.getRecordCount();
                int[] offsets = column.getLayerOffsets(0);
                Validity listValidity = column.getLayerValidity(0);
                int[] values = column.getInts();
                Validity leafValidity = column.getLeafValidity();
                for (int r = 0; r < records; r++) {
                    if (listValidity.isNull(r)) {
                        out.add(null);
                        continue;
                    }
                    List<Integer> list = new ArrayList<>();
                    for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                        list.add(leafValidity.isNull(e) ? null : values[e]);
                    }
                    out.add(list);
                }
            }
        }
        return out;
    }

    private static Integer[] expectedNullable(int[] values, boolean[] nulls) {
        Integer[] expected = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            expected[i] = nulls[i] ? null : values[i];
        }
        return expected;
    }

    /// An [OutputFile] that throws while writing the second `PAR1` magic — the footer's
    /// trailing marker — leaving a file whose footer never completed.
    private static final class FailOnSecondMagic implements OutputFile {

        private final OutputFile delegate;
        private int magicWrites;

        FailOnSecondMagic(OutputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            if (isMagic(data) && ++magicWrites == 2) {
                throw new IOException("injected footer write failure");
            }
            delegate.write(data);
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void discard() throws IOException {
            delegate.discard();
        }

        private static boolean isMagic(ByteBuffer data) {
            if (data.remaining() != 4) {
                return false;
            }
            int p = data.position();
            return data.get(p) == 'P' && data.get(p + 1) == 'A' && data.get(p + 2) == 'R' && data.get(p + 3) == '1';
        }
    }
}
