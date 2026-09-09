/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// What the writer does at a scale the rest of the suite does not reach.
///
/// Everything else here writes thousands of rows, where every offset, page count and row count
/// fits comfortably in whatever type carries it. The defects this is for only appear once they
/// do not: a chunk offset past what an `int` holds, a page count past what the format's own
/// `i16` field holds in some readers, a row count accumulated across enough row groups to
/// disagree with the footer's own.
///
/// The multi-row-group case runs by default because it is quick. The two-gigabyte case is
/// opt-in, on the same switch as the read direction's `LargeFileReadTest`: it writes several
/// gigabytes to disk, which is not something every build should do.
class WriterLargeFileTest {

    private static final String COLUMN = "v";

    /// Values are a pure function of their row index, so a file of any size is verified without
    /// an expected array beside it.
    private static long valueAt(long row) {
        return row * 2_654_435_761L + (row % 97);
    }

    /// Enough rows to cross many row-group and page boundaries at small targets, and few enough
    /// to stay a second or two.
    private static final int ROWS = 2_000_000;

    private static final int BATCH = 8_192;

    /// A file banded into many row groups, each holding many pages, read back value by value.
    ///
    /// What this adds over the single-row-group cases is accumulation: every row group's offsets
    /// are recorded against a running position, every chunk's row count is summed into the
    /// footer's, and a defect in either only shows once there are enough of them for a drift to
    /// exceed a page.
    @Test
    void manyRowGroupsAndPagesReadBackIntact() throws Exception {
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(1L << 20)
                .pageTargetBytes(16 << 10)
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), config)) {
            writeRows(writer, ROWS);
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(out.buffer()))) {

            assertThat(reader.getFileMetaData().numRows()).as("footer row count").isEqualTo(ROWS);
            assertThat(reader.getFileMetaData().rowGroups().size())
                    .as("row groups at a 1 MiB target").isGreaterThan(8);
            assertRowGroupOffsetsAscend(reader);
            assertValues(reader, ROWS);
        }
    }

    /// A file past two gigabytes, where a column chunk's offset no longer fits in an `int`.
    ///
    /// The read direction proves it can map such a file; this proves the writer records the
    /// offsets that make it readable, which is the same question asked of the other end of the
    /// pipe.
    @Test
    @Tag("large")
    @EnabledIfSystemProperty(named = "hardwood.largeFileTests", matches = "true")
    void writesAndReadsBackAFileLargerThanTwoGigabytes(@TempDir Path dir) throws Exception {
        // An uncompressed PLAIN INT64 is eight bytes a row and nothing shrinks it afterwards, so
        // the file size follows from the row count rather than from how well the values happened
        // to compress. Both settings are load-bearing: the default codec would put a file this
        // shape back under the boundary the case exists to cross.
        long rows = (2L << 30) / Long.BYTES + (8L << 20);
        Path file = dir.resolve("large.parquet");

        WriterConfig config = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupBufferTargetBytes(128L << 20)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema(), config)) {
            writeRows(writer, rows);
        }

        assertThat(Files.size(file)).as("file size").isGreaterThan(2L << 30);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).as("footer row count").isEqualTo(rows);
            assertRowGroupOffsetsAscend(reader);
            assertThat(lastChunkOffset(reader))
                    .as("a chunk offset past what an int holds")
                    .isGreaterThan(Integer.MAX_VALUE);
            assertValues(reader, rows);
        }
    }

    /// A row group whose byte target asks for more rows than a column chunk can hold.
    ///
    /// The target is a byte target and a `BOOLEAN` value is charged the single bit it occupies,
    /// so a few hundred megabytes of it names billions of rows in one group. The buffers a chunk
    /// accumulates into are indexed by `int`, and a chunk that reaches their ceiling cannot take
    /// another value: the row group has to be cut before it gets there, since a boundary the
    /// writer declines to place is one the caller has no way to place either.
    ///
    /// Every column contributes at least one entry per record, and every entry costs its column
    /// either a stored value or a level byte, so the ceiling one of those buffers reaches first
    /// is what bounds the group. A column with more entries than records — a repeated one —
    /// reaches it earlier still, but only at a target in the tens of gigabytes, its entries
    /// costing a level byte each on the way.
    @Test
    @Tag("large")
    @EnabledIfSystemProperty(named = "hardwood.largeFileTests", matches = "true")
    void aRowGroupIsCutBeforeAChunkOutgrowsItsBuffers(@TempDir Path dir) throws Exception {
        long rows = (1L << 31) + (1 << 16);
        Path file = dir.resolve("boolean.parquet");

        FileSchema schema = FileSchema.builder("large")
                .addColumn(COLUMN, PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        // Half a gigabyte of charge at one bit a row is four billion rows: the row ceiling is
        // reached first, which is the point.
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(512L << 20)
                // Both configured targets put out of the way, so what cuts the group is the
                // ceiling this case exists to reach.
                .rowGroupTargetRows(Long.MAX_VALUE)
                .codec(CompressionCodec.UNCOMPRESSED)
                .build();

        boolean[] values = new boolean[BATCH];
        for (int i = 0; i < BATCH; i++) {
            values[i] = (i & 7) == 0;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            ColumnWriter columns = writer.columnWriter();
            for (long row = 0; row < rows; row += BATCH) {
                int count = (int) Math.min(BATCH, rows - row);
                boolean[] slice = count == BATCH ? values : Arrays.copyOf(values, count);
                columns.writeBatch(batch -> batch.booleans(COLUMN, slice));
            }
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).as("footer row count").isEqualTo(rows);
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("row groups, each holding what one chunk can carry")
                    .hasSizeGreaterThan(1)
                    .allSatisfy(group -> assertThat(group.numRows()).isPositive());
            assertThat(reader.getFileMetaData().rowGroups().stream().mapToLong(RowGroup::numRows).sum())
                    .as("rows across the groups agree with the footer")
                    .isEqualTo(rows);
        }
    }

    // ==================== Helpers ====================

    private static FileSchema schema() {
        return FileSchema.builder("large")
                .addColumn(COLUMN, PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
    }

    private static void writeRows(ParquetFileWriter writer, long rows) throws IOException {
        ColumnWriter columns = writer.columnWriter();
        long[] values = new long[BATCH];
        for (long row = 0; row < rows; row += BATCH) {
            int count = (int) Math.min(BATCH, rows - row);
            for (int i = 0; i < count; i++) {
                values[i] = valueAt(row + i);
            }
            long[] slice = count == BATCH ? values : Arrays.copyOf(values, count);
            columns.writeBatch(batch -> batch.longs(COLUMN, slice));
        }
    }

    /// Every value is the one its row index implies, compared as the batches arrive so that the
    /// file is never held in memory at once.
    ///
    /// The comparison is a plain `if` with an assertion inside rather than an assertion per
    /// value: at these row counts the assertion library's own per-call work dominates the read
    /// it is checking, and a mismatch still reports which row it was.
    private static void assertValues(ParquetFileReader reader, long rows) throws IOException {
        long row = 0;
        try (ColumnReader column = reader.columnReader(0)) {
            while (column.nextBatch()) {
                long[] batch = column.getLongs();
                int count = column.getValueCount();
                for (int i = 0; i < count; i++) {
                    if (batch[i] != valueAt(row + i)) {
                        assertThat(batch[i]).as("row %d", row + i).isEqualTo(valueAt(row + i));
                    }
                }
                row += count;
            }
        }
        assertThat(row).as("rows read back").isEqualTo(rows);
    }

    /// Row groups are written in order and never overlap, which is what a running position that
    /// drifted would break.
    private static void assertRowGroupOffsetsAscend(ParquetFileReader reader) {
        long previous = -1;
        for (RowGroup group : reader.getFileMetaData().rowGroups()) {
            long offset = group.columns().get(0).metaData().dataPageOffset();
            assertThat(offset).as("data page offset ascends").isGreaterThan(previous);
            previous = offset;
        }
    }

    private static long lastChunkOffset(ParquetFileReader reader) {
        List<RowGroup> groups = reader.getFileMetaData().rowGroups();
        return groups.get(groups.size() - 1).columns().get(0).metaData().dataPageOffset();
    }
}
