/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.plan.FetchPlanBuilder;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The row-group statistics of `id` (0–149) admit `id = 75`, but its Column Index rules out
/// both pages (0–49 and 100–149), so page filtering leaves no row range. The other columns
/// have no Page Index: `value` is flat and leaves the mask gate open, `tags` is nested with v1
/// pages and closes it. Either way no page is read.
class EmptyPageRangesUnindexedColumnTest {

    private static final Path FIXTURE =
            Path.of("src/test/resources/page_index_empty_ranges_unindexed_column.parquet");

    private static final ColumnProjection FLAT = ColumnProjection.columns("value", "id");

    @Test
    void theNestedV1ColumnClosesTheMaskGate() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            FileColumnOrdinals ordinals = FileColumnOrdinals.identity(
                    schema.getColumnCount(), null, BoundsReadability.ALL);
            RowGroup rowGroup = meta.rowGroups().get(0);

            assertThat(FetchPlanBuilder.masksApplicableForRowGroup(
                    ProjectedSchema.create(schema, FLAT), rowGroup, schema, ordinals, ByteSource.of(file)).get()).isTrue();
            assertThat(FetchPlanBuilder.masksApplicableForRowGroup(
                    ProjectedSchema.create(schema, ColumnProjection.all()), rowGroup, schema, ordinals, ByteSource.of(file)).get())
                    .isFalse();
        }
    }

    @Test
    void aFilterRulingOutEveryPageReadsNoPageWithTheGateOpen() throws Exception {
        assertNoRowAndNoPageRead(FLAT);
    }

    /// Empty row ranges are not promoted to all rows when the gate is closed, and the gate is
    /// not probed: the probe would read a page header of `tags`.
    @Test
    void aFilterRulingOutEveryPageReadsNoPageWithTheGateClosed() throws Exception {
        assertNoRowAndNoPageRead(ColumnProjection.all());
    }

    @Test
    void aFilterRulingOutEveryPageReadsNoPageThroughColumnReaders() throws Exception {
        assertNoPageRead(file -> {
            try (ColumnReaders columns = file.buildColumnReaders(ColumnProjection.all())
                    .filter(FilterPredicate.eq("id", 75)).build()) {
                assertThat(columns.nextBatch()).isFalse();
            }
        });
    }

    @Test
    void aFilterKeepingOnePageReturnsItsMatchWithTheGateOpenThroughColumnReaders() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders columns = file.buildColumnReaders(FLAT).filter(FilterPredicate.eq("id", 120)).build()) {
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getRecordCount()).isEqualTo(1);
            assertThat(columns.getColumnReader("id").getInts()[0]).isEqualTo(120);
            assertThat(columns.getColumnReader("value").getInts()[0]).isEqualTo(70);
            assertThat(columns.nextBatch()).isFalse();
        }
    }

    @Test
    void aFilterKeepingOnePageReturnsItsMatchWithTheGateClosedThroughColumnReaders() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders columns = file.buildColumnReaders(ColumnProjection.columns("id", "value", "tags"))
                     .filter(FilterPredicate.eq("id", 120)).build()) {
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getRecordCount()).isEqualTo(1);
            assertThat(columns.getColumnReader("id").getInts()[0]).isEqualTo(120);
            assertThat(columns.getColumnReader("value").getInts()[0]).isEqualTo(70);
            ColumnReader tags = columns.getColumnReader(2);
            int[] offsets = tags.getLayerOffsets(0);
            assertThat(Arrays.copyOfRange(tags.getInts(), offsets[0], offsets[1])).containsExactly(70, 70);
            assertThat(columns.nextBatch()).isFalse();
        }
    }

    @Test
    void aFilterKeepingOnePageReturnsItsMatchWithTheGateOpen() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.buildRowReader()
                     .projection(FLAT)
                     .filter(FilterPredicate.eq("id", 120))
                     .build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(120);
            assertThat(rows.getInt("value")).isEqualTo(70);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void aFilterKeepingOnePageReturnsItsMatchWithTheGateClosed() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.buildRowReader()
                     .filter(FilterPredicate.eq("id", 120))
                     .build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(120);
            assertThat(rows.getInt("value")).isEqualTo(70);
            PqList tags = rows.getList("tags");
            assertThat(tags.ints().toArray()).containsExactly(70, 70);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private static void assertNoRowAndNoPageRead(ColumnProjection projection) throws Exception {
        assertNoPageRead(file -> {
            try (RowReader rows = file.buildRowReader()
                    .projection(projection)
                    .filter(FilterPredicate.eq("id", 75))
                    .build()) {
                assertThat(rows.hasNext()).isFalse();
            }
        });
    }

    /// A read of the fixture, which asserts what it returns.
    private interface Read {
        void run(ParquetFileReader file) throws Exception;
    }

    private static void assertNoPageRead(Read fileRead) throws Exception {
        CountingInputFile input = new CountingInputFile(InputFile.of(FIXTURE));
        List<ColumnMetaData> chunks;
        try (ParquetFileReader file = ParquetFileReader.open(input)) {
            fileRead.run(file);
            chunks = file.getFileMetaData().rowGroups().get(0).columns().stream()
                    .map(chunk -> chunk.metaData())
                    .toList();
        }
        // Only the footer and the Page Index are read: no byte of any column chunk.
        for (CountingInputFile.Read read : input.reads()) {
            for (ColumnMetaData chunk : chunks) {
                long start = chunk.dictionaryPageOffset() != null
                        ? chunk.dictionaryPageOffset() : chunk.dataPageOffset();
                long end = start + chunk.totalCompressedSize();
                assertThat(read.offset() < end && read.end() > start)
                        .as("read %s overlaps column chunk %s", read, chunk.pathInSchema())
                        .isFalse();
            }
        }
    }
}
