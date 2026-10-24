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
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The size statistics the writer produces: the chunk's `SizeStatistics`, the per-page level
/// histograms in its `ColumnIndex` and the per-page unencoded `BYTE_ARRAY` bytes in its
/// `OffsetIndex`. Read back from the file's own bytes.
class WriterSizeStatisticsTest {

    private static final WriterConfig TWO_ROW_PAGES = WriterConfig.builder()
            .codec(CompressionCodec.UNCOMPRESSED)
            .pageTargetRows(2)
            .build();

    /// `id`, `address.city` (maxDef 1), `address.zip` (maxDef 2) and `scores.list.element`
    /// (maxRep 1, maxDef 3), over four rows that put every level of each column in play, in two
    /// pages of two rows each.
    private static final FileSchema NESTED = FileSchema.builder("schema")
            .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
            .struct("address", RepetitionType.OPTIONAL, address -> address
                    .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                    .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
            .list("scores", RepetitionType.OPTIONAL,
                    element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
            .build();

    @Test
    void countsEveryLevelOfANestedColumnPerPageAndPerChunk() throws Exception {
        byte[] file = writeNested();

        // Page 0: [7, 9] at levels (0, 3), (1, 3); [] at (0, 1). Page 1: absent at (0, 0); [1, null]
        // at (0, 3), (1, 2).
        Index scores = index(file, 0, 3);
        assertThat(scores.chunk().metaData().sizeStatistics()).satisfies(statistics -> {
            assertThat(statistics.unencodedByteArrayDataBytes()).isNull();
            assertThat(statistics.repetitionLevelHistogram()).containsExactly(4, 2);
            assertThat(statistics.definitionLevelHistogram()).containsExactly(1, 1, 1, 3);
        });
        assertThat(scores.columns().repetitionLevelHistograms()).containsExactly(2, 1, 2, 1);
        assertThat(scores.columns().definitionLevelHistograms()).containsExactly(0, 1, 0, 2, 1, 0, 1, 1);
        assertThat(scores.offsets().unencodedByteArrayDataBytes()).isNull();
    }

    @Test
    void countsTheBytesOfAByteArrayColumnWithoutTheirLengthPrefixes() throws Exception {
        byte[] file = writeNested();

        // Page 0: "10115", then an absent address. Page 1: a null zip, then "0150".
        Index zip = index(file, 0, 2);
        assertThat(zip.chunk().metaData().sizeStatistics()).satisfies(statistics -> {
            assertThat(statistics.unencodedByteArrayDataBytes()).isEqualTo(9);
            assertThat(statistics.repetitionLevelHistogram()).isNull();
            assertThat(statistics.definitionLevelHistogram()).containsExactly(1, 1, 2);
        });
        assertThat(zip.columns().repetitionLevelHistograms()).isNull();
        assertThat(zip.columns().definitionLevelHistograms()).containsExactly(1, 0, 1, 0, 1, 1);
        assertThat(zip.offsets().unencodedByteArrayDataBytes()).containsExactly(5, 4);
    }

    /// A column with one definition level holds nulls and values only, which its null count
    /// already gives, so it carries no histogram; its bytes are still counted.
    @Test
    void omitsTheHistogramOfAColumnWithOneDefinitionLevel() throws Exception {
        byte[] file = writeNested();

        Index city = index(file, 0, 1);
        assertThat(city.chunk().metaData().sizeStatistics())
                .isEqualTo(new SizeStatistics(16L, null, null));
        assertThat(city.columns().repetitionLevelHistograms()).isNull();
        assertThat(city.columns().definitionLevelHistograms()).isNull();
        assertThat(city.offsets().unencodedByteArrayDataBytes()).containsExactly(6, 10);
    }

    @Test
    void writesNoSizeStatisticsForAColumnWithNothingToCount() throws Exception {
        byte[] file = writeNested();

        Index id = index(file, 0, 0);
        assertThat(id.chunk().metaData().sizeStatistics()).isNull();
        assertThat(id.columns().repetitionLevelHistograms()).isNull();
        assertThat(id.columns().definitionLevelHistograms()).isNull();
        assertThat(id.offsets().unencodedByteArrayDataBytes()).isNull();
    }

    /// A dictionary chunk's pages hold indices; the bytes counted are those of the values they
    /// name. A fixed-width binary column is not counted.
    @ParameterizedTest
    @EnumSource(value = ColumnEncoding.class, names = { "AUTO", "PLAIN", "DELTA_BYTE_ARRAY" })
    void countsByteArrayPageBytesUnderEveryEncoding(ColumnEncoding encoding) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .addColumn("f", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 3)
                .build();
        int rows = 1_000;
        byte[][] strings = new byte[rows][];
        boolean[] nulls = new boolean[rows];
        byte[][] fixed = new byte[rows][];
        long[] pageBytes = new long[rows / 100];
        for (int r = 0; r < rows; r++) {
            nulls[r] = r % 7 == 0;
            strings[r] = new byte[r % 5];
            fixed[r] = new byte[3];
            pageBytes[r / 100] += nulls[r] ? 0 : strings[r].length;
        }
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED)
                .pageTargetRows(100).encoding("s", encoding).build();
        byte[] file = write(schema, config, batch -> batch.bytes(0, strings, nulls).fixed(1, fixed));

        Index s = index(file, 0, 0);
        assertThat(s.chunk().metaData().dictionaryPageOffset() != null).isEqualTo(encoding == ColumnEncoding.AUTO);
        assertThat(s.offsets().unencodedByteArrayDataBytes()).containsExactly(pageBytes);
        assertThat(s.chunk().metaData().sizeStatistics().unencodedByteArrayDataBytes())
                .isEqualTo(sum(pageBytes));
        assertThat(index(file, 0, 1).chunk().metaData().sizeStatistics()).isNull();
    }

    /// The definition level histograms let Hardwood's reader prune a Hardwood-written file on a
    /// group's presence: the row group where `address` is present throughout is never read for
    /// `IS NULL`, and the one where it is absent throughout never for `IS NOT NULL`.
    @Test
    void letsTheReaderSkipRowGroupsAGroupNullPredicateRulesOut() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            // Present throughout, one with a null zip, which is still a present address.
            rows.writeRow(row -> row.setInt("id", 1).setStruct("address", a -> a.setString("zip", "10115")));
            rows.writeRow(row -> row.setInt("id", 2).setStruct("address", a -> a.setString("zip", null)));
            writer.endRowGroup();
            // Absent throughout.
            rows.writeRow(row -> row.setInt("id", 3));
            rows.writeRow(row -> row.setInt("id", 4));
            writer.endRowGroup();
            // Mixed.
            rows.writeRow(row -> row.setInt("id", 5).setStruct("address", a -> a.setString("zip", "0150")));
            rows.writeRow(row -> row.setInt("id", 6));
        }
        byte[] file = InMemoryFiles.toByteArray(out);
        List<RowGroup> rowGroups = rowGroups(file);
        assertThat(rowGroups).hasSize(3);

        CountingInputFile isNull = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(ids(isNull, FilterPredicate.isNull("address"))).containsExactly(3, 4, 6);
        assertThat(isNull.reads()).noneMatch(read -> touchesData(read, rowGroups.get(0)));

        CountingInputFile isNotNull = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(ids(isNotNull, FilterPredicate.isNotNull("address"))).containsExactly(1, 2, 5);
        assertThat(isNotNull.reads()).noneMatch(read -> touchesData(read, rowGroups.get(1)));
    }

    // ==================== Helpers ====================

    private record Index(ColumnChunk chunk, OffsetIndex offsets, ColumnIndex columns) {
    }

    private static byte[] writeNested() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, NESTED, TWO_ROW_PAGES)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1)
                    .setStruct("address", a -> a.setString("city", "Berlin").setString("zip", "10115"))
                    .setList("scores", scores -> scores.addInt(7).addInt(9)));
            rows.writeRow(row -> row.setInt("id", 2).setList("scores", scores -> { }));
            rows.writeRow(row -> row.setInt("id", 3)
                    .setStruct("address", a -> a.setString("city", "Aarhus")));
            rows.writeRow(row -> row.setInt("id", 4)
                    .setStruct("address", a -> a.setString("city", "Oslo").setString("zip", "0150"))
                    .setList("scores", scores -> scores.addInt(1).addNull()));
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static byte[] write(FileSchema schema, WriterConfig config, Consumer<ColumnBatch> filler) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(filler);
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static List<RowGroup> rowGroups(byte[] file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            return reader.getFileMetaData().rowGroups();
        }
    }

    /// Reads a chunk's page index from its byte ranges.
    private static Index index(byte[] file, int rowGroup, int column) throws Exception {
        ColumnChunk chunk = rowGroups(file).get(rowGroup).columns().get(column);
        OffsetIndex offsets = OffsetIndexReader.read(new ThriftCompactReader(
                slice(file, chunk.offsetIndexOffset(), chunk.offsetIndexLength())));
        ColumnIndex columns = ColumnIndexReader.read(new ThriftCompactReader(
                slice(file, chunk.columnIndexOffset(), chunk.columnIndexLength())));
        return new Index(chunk, offsets, columns);
    }

    private static ByteBuffer slice(byte[] file, long offset, int length) {
        return ByteBuffer.wrap(file, Math.toIntExact(offset), length).slice();
    }

    private static long sum(long[] values) {
        long total = 0;
        for (long value : values) {
            total += value;
        }
        return total;
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

    /// Whether `read` overlaps the pages of any column chunk of `rowGroup`.
    private static boolean touchesData(CountingInputFile.Read read, RowGroup rowGroup) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            long start = chunk.metaData().dictionaryPageOffset() != null
                    ? chunk.metaData().dictionaryPageOffset()
                    : chunk.metaData().dataPageOffset();
            long end = start + chunk.metaData().totalCompressedSize();
            if (read.offset() < end && read.end() > start) {
                return true;
            }
        }
        return false;
    }
}
