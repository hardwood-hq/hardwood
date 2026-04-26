/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Exercises the fallback path for a nested column that has neither an
/// OffsetIndex nor a `DATA_PAGE_V2` shape — the rep-level prefix lives
/// inside the compressed area on a v1 page, so counting top-level records
/// would require decompression. The row-group-wide gate closes, every
/// `matchingRows` is promoted to `RowRanges.ALL`, and:
///
/// - filter pushdown still produces correct rows via the row reader's
///   residual-filter pass over every undropped row group;
/// - tail reading falls back to decode-and-discard of leading rows
///   instead of using per-page masks.
///
/// The fixture is the only one in the suite that closes the gate, so it
/// covers the dead-code-shaped branches (`MaskCapability.NO`, the
/// `IllegalStateException` guard inside
/// `SequentialFetchPlan.computeRecordsInPage`) that the other no-Page-Index
/// fixtures cannot reach.
class NestedV1NoIndexFallbackTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/nested_v1_no_index.parquet");
    private static final int TOTAL_ROWS = 2_000;

    @Test
    void testFixtureLacksOffsetIndex() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file, null, null);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                assertThat(rg.columns().get(c).offsetIndexOffset())
                        .as("column %d offsetIndexOffset", c)
                        .isNull();
            }
        }
    }

    @Test
    void testMaskGateClosesForV1NestedColumn() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file, null, null);
            RowGroup rg = meta.rowGroups().get(0);

            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());
            boolean applicable = RowGroupIterator.masksApplicableForRowGroup(
                    projected, rg, schema, file);

            // Gate must be closed: a nested column without an OffsetIndex
            // and with v1 pages can't be record-counted without
            // decompression. If this ever returns true on this fixture, the
            // probe is no longer detecting v1 pages and the
            // skip-without-decompress optimisation has been silently broken.
            assertThat(applicable)
                    .as("mask gate must close on nested-v1-no-index fixture")
                    .isFalse();
        }
    }

    @Test
    void testFilterReturnsCorrectRowsViaResidualPath() throws Exception {
        int lo = 400;
        int hi = 1600;
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("narrow", lo),
                FilterPredicate.lt("narrow", hi));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                PqList tags = rows.getList("tags");

                assertThat(narrow)
                        .as("row %d: narrow outside requested range", count)
                        .isGreaterThanOrEqualTo(lo)
                        .isLessThan(hi);
                assertThat(tags)
                        .as("row %d: tags should not be null", count)
                        .isNotNull();
                assertThat(tags.size()).as("row %d: tags size", count).isEqualTo(2);

                String firstTag = tags.strings().iterator().next();
                assertThat(firstTag)
                        .as("row %d: tag does not match narrow=%d", count, narrow)
                        .isEqualTo(String.format("row=%05d", narrow));
                count++;
            }
            assertThat(count)
                    .as("row count for range [%d, %d)", lo, hi)
                    .isEqualTo(hi - lo);
        }
    }

    @Test
    void testTailReadReturnsAlignedRowsViaDecodeAndDiscardFallback() throws Exception {
        int tailRows = 600;
        int firstExpectedRow = TOTAL_ROWS - tailRows;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            int expected = firstExpectedRow;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                PqList tags = rows.getList("tags");

                assertThat(narrow).as("row offset %d narrow", expected - firstExpectedRow)
                        .isEqualTo(expected);
                assertThat(tags)
                        .as("row offset %d tags", expected - firstExpectedRow)
                        .isNotNull();
                assertThat(tags.size()).isEqualTo(2);
                String firstTag = tags.strings().iterator().next();
                assertThat(firstTag)
                        .as("row offset %d tag", expected - firstExpectedRow)
                        .isEqualTo(String.format("row=%05d", expected));
                expected++;
            }
            assertThat(expected - firstExpectedRow)
                    .as("tail row count")
                    .isEqualTo(tailRows);
        }
    }

    @Test
    void testFullScanStillPairsColumnsCorrectly() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            int expected = 0;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                PqList tags = rows.getList("tags");
                assertThat(narrow).as("row %d narrow", expected).isEqualTo(expected);
                assertThat(tags).as("row %d tags", expected).isNotNull();
                String firstTag = tags.strings().iterator().next();
                assertThat(firstTag).as("row %d tag", expected)
                        .isEqualTo(String.format("row=%05d", expected));
                expected++;
            }
            assertThat(expected).isEqualTo(TOTAL_ROWS);
        }
    }
}
