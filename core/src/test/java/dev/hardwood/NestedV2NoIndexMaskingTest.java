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
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;

import static org.assertj.core.api.Assertions.assertThat;

/// Drives the rep-level walk path in
/// [dev.hardwood.internal.reader.SequentialFetchPlan] against a
/// `DATA_PAGE_V2` nested column without OffsetIndex. The fixture pairs a
/// flat `narrow` INT32 with a nested `tags` LIST&lt;STRING&gt; whose pages
/// flush at different rows than the flat column. Filter pushdown and
/// tail-mode tests verify cross-column row alignment.
class NestedV2NoIndexMaskingTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_nested_v2.parquet");
    private static final int TOTAL_ROWS = 2_000;

    @Test
    void testFixtureLacksOffsetIndex() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                assertThat(rg.columns().get(c).offsetIndexOffset())
                        .as("column %d offsetIndexOffset", c)
                        .isNull();
            }
        }
    }

    @Test
    void testFilterOnNarrowReturnsCorrectlyAlignedTags() throws Exception {
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
    void testTailReadReturnsAlignedRowsViaPerPageMaskFastPath() throws Exception {
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
