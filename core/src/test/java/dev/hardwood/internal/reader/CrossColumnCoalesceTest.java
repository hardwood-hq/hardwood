/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies cross-column ranged-GET coalescing within a row group (#374).
///
/// Adjacent column chunks within a row group are typically stored
/// back-to-back on disk, so a single ranged GET can cover many columns
/// at once. The pipeline coalesces such reads into [SharedRegion]s in
/// `RowGroupIterator.coalesceAcrossColumns`. These tests assert the
/// observable effect: the underlying `readRange` count drops compared to
/// what a per-column-per-chunk fetch path would produce.
class CrossColumnCoalesceTest {

    /// 20-column flat schema, single row group, no OffsetIndex →
    /// SequentialFetchPlan path. 20 leaf columns are stored back-to-back
    /// (zero gaps), all within ~2 KB.
    private static final Path SEQ_FILE = Path.of("src/test/resources/yellow_tripdata_sample.parquet");

    /// 3-column flat schema, single row group, OffsetIndex present →
    /// IndexedFetchPlan path. Columns are stored back-to-back.
    private static final Path INDEXED_FILE = Path.of("src/test/resources/page_index_test.parquet");

    @Test
    void sequentialPlanCoalescesFirstChunkOfEveryColumn() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(SEQ_FILE));
        countingFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(countingFile)) {
            int readsBefore = countingFile.readCount();
            try (RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            int readsForData = countingFile.readCount() - readsBefore;
            // The fixture has 20 leaf columns; a per-column fetch would
            // issue 20 readRange calls. With cross-column coalescing,
            // they collapse into one (all columns fit in <2 KB,
            // back-to-back).
            assertThat(readsForData)
                    .as("Data fetches should coalesce to a single ranged GET (was %d)", readsForData)
                    .isEqualTo(1);
        }
    }

    @Test
    void indexedPlanCoalescesFirstChunkOfEveryColumn() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(INDEXED_FILE));
        countingFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(countingFile)) {
            int readsBefore = countingFile.readCount();
            try (RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            int readsForData = countingFile.readCount() - readsBefore;
            // 3 columns, all back-to-back, no filter → all 3 first
            // chunks coalesce into one region. The pipeline also fetches
            // the per-RG index region (OffsetIndex / ColumnIndex), so
            // expect at most 2 reads (1 region + 1 index buffer).
            assertThat(readsForData)
                    .as("Data fetches should coalesce to ≤ 2 ranged GETs (was %d)", readsForData)
                    .isLessThanOrEqualTo(2);
        }
    }

    @Test
    void anUnprojectedColumnWithinTheGapLimitIsFetchedAcross() throws Exception {
        // `b` sits between the two projected columns: 100 000 plain longs, about 800 KB.
        assertThat(dataRequestsForOuterColumns(100_000))
                .as("`a` and `c` in one request, across `b`")
                .isEqualTo(1);
    }

    @Test
    void anUnprojectedColumnBeyondTheGapLimitSplitsTheRequest() throws Exception {
        // 160 000 plain longs, about 1.3 MB: more than the gap coalescing bridges.
        assertThat(dataRequestsForOuterColumns(160_000))
                .as("`a` and `c` in a request each")
                .isEqualTo(2);
    }

    @Test
    void aColumnFetchingOnItsOwnIsNotBridgedByItsNeighbours(@TempDir Path dir) throws Exception {
        // `b` is fetched in pieces of 64 KB, so its first fetch does not cover its ~800 KB chunk
        // and it is left out of cross-column coalescing, while `a` and `c` each fit in one fetch.
        // A region from `a` to `c` would take in all of `b`, which `b` then fetches again.
        Path file = dir.resolve("abc.parquet");
        Files.write(file, writeOuterAndInnerColumns(100_000));
        System.setProperty(SequentialFetchPlan.CHUNK_SIZE_PROPERTY, String.valueOf(64 * 1024));
        try {
            CountingInputFile counter = new CountingInputFile(InputFile.of(file));
            counter.open();
            try (ParquetFileReader reader = ParquetFileReader.open(counter);
                    RowReader rowReader = reader.buildRowReader()
                            .projection(ColumnProjection.columns("a", "b", "c"))
                            .build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
            IoBudget.of(file, List.of("a", "b", "c"), 0, 100_000).assertWithin(counter);
        }
        finally {
            System.clearProperty(SequentialFetchPlan.CHUNK_SIZE_PROPERTY);
        }
    }

    /// Writes `rows` rows of a `BOOLEAN` column `a`, a plain `INT64` column `b` and a `BOOLEAN`
    /// column `c`, one row group without a page index, and reads `a` and `c`: the number of
    /// requests their data takes.
    private static int dataRequestsForOuterColumns(int rows) throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(writeOuterAndInnerColumns(rows)));
        file.open();
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            int before = file.readCount();
            try (RowReader rowReader = reader.buildRowReader()
                    .projection(ColumnProjection.columns("a", "c"))
                    .build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
            return file.readCount() - before;
        }
    }

    /// `rows` rows of a `BOOLEAN` column `a`, a plain `INT64` column `b` and a `BOOLEAN` column
    /// `c`, in one row group without a page index.
    private static byte[] writeOuterAndInnerColumns(int rows) throws Exception {
        FileSchema schema = FileSchema.builder("gap")
                .addColumn("a", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("c", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding("b", ColumnEncoding.PLAIN)
                .rowGroupTargetRows(rows)
                .build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < rows; i++) {
                long value = i;
                rowWriter.writeRow(row -> row.setBoolean("a", value % 3 == 0).setLong("b", value).setBoolean("c", value % 5 == 0));
            }
        }
        return out.toByteArray();
    }

    @Test
    void filteredColumnIsNotOverCoalesced() throws Exception {
        // With a selective filter, the IndexedFetchPlan for `id` has page drops. Sweeping its
        // dropped pages into a shared region would fetch bytes no page needs, and fetch the
        // column's later page groups a second time; the budget rejects both. `id` runs from 1, so
        // `id < 1000` is the rows [0, 999).
        CountingInputFile counter = new CountingInputFile(InputFile.of(INDEXED_FILE));
        counter.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(counter);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("id", "value", "category"))
                        .filter(FilterPredicate.lt("id", 1000L))
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(999);
        IoBudget.of(INDEXED_FILE, List.of("id", "value", "category"), 0, 999).assertWithin(counter);
    }
}
