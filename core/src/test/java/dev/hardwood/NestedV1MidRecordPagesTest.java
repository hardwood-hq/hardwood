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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.decode.ReferencePageDecoder;
import dev.hardwood.internal.reader.fetch.PageWalks;
import dev.hardwood.internal.reader.fetch.SequentialFetchPlan;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Reads a nested `LIST<STRING>` column whose `V1` data pages begin inside a record: a page's
/// values before its first repetition level `0` belong to the previous page's last record.
///
/// The fixture's `tags` pages hold 7 level positions each, wherever records fall, over lists of
/// one to four elements, empty and null lists, and null elements. The build also runs this class
/// at a batch size of `1`, which puts every such join of two pages at a batch edge.
class NestedV1MidRecordPagesTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/nested_v1_mid_record_pages.parquet");
    private static final int TOTAL_ROWS = 300;

    @Test
    void fixtureHasPagesBeginningInsideRecord() throws Exception {
        InputFile file = InputFile.of(FIXTURE);
        file.open();
        HardwoodContextImpl context = HardwoodContextImpl.create();
        int pages = 0;
        int pagesBeginningInsideRecord = 0;
        try {
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            int tags = tagsColumnIndex(schema);
            ColumnSchema column = schema.getColumn(tags);
            for (RowGroup rowGroup : meta.rowGroups()) {
                assertThat(rowGroup.columns().get(tags).offsetIndexOffset()).as("offset index").isNull();
                PageWalks.Pages iterator = PageWalks.of(SequentialFetchPlan.build(file, column, rowGroup.columns().get(tags),
                        context, 0, file.name(), 0));
                while (iterator.hasNext()) {
                    PageInfo pageInfo = iterator.next();
                    ReferencePageDecoder.DecodedPage page = new ReferencePageDecoder(pageInfo.columnSchema(),
                            pageInfo.columnMetaData().codec(), context.decompressorFactory())
                            .decode(pageInfo.pageData(), pageInfo.dictionary());
                    pages++;
                    if (page.repetitionLevels()[0] != 0) {
                        pagesBeginningInsideRecord++;
                    }
                }
            }
        }
        finally {
            file.close();
            context.close();
        }
        assertThat(pages).as("tags pages").isGreaterThan(1);
        assertThat(pagesBeginningInsideRecord).as("tags pages beginning inside a record").isPositive();
    }

    @Test
    void fullScanJoinsRecordsAcrossPages() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            int expected = 0;
            while (rows.hasNext()) {
                rows.next();
                assertRow(rows, expected);
                expected++;
            }
            assertThat(expected).isEqualTo(TOTAL_ROWS);
        }
    }

    @Test
    void headEndsInsideAPage() throws Exception {
        int headRows = 101;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().head(headRows).build()) {
            int expected = 0;
            while (rows.hasNext()) {
                rows.next();
                assertRow(rows, expected);
                expected++;
            }
            assertThat(expected).isEqualTo(headRows);
        }
    }

    @Test
    void filterKeepsRecordsJoinedAcrossPages() throws Exception {
        int lo = 37;
        int hi = 211;
        FilterPredicate filter = FilterPredicate.and(FilterPredicate.gtEq("id", lo), FilterPredicate.lt("id", hi));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            int expected = lo;
            while (rows.hasNext()) {
                rows.next();
                assertRow(rows, expected);
                expected++;
            }
            assertThat(expected).isEqualTo(hi);
        }
    }

    @Test
    void columnReaderJoinsRecordsAcrossPages() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader tags = reader.columnReader("tags.list.element")) {
            int record = 0;
            while (tags.nextBatch()) {
                int[] offsets = tags.getLayerOffsets(0);
                Validity lists = tags.getLayerValidity(0);
                Validity elements = tags.getLeafValidity();
                String[] values = tags.getStrings();
                for (int r = 0; r < tags.getRecordCount(); r++) {
                    List<String> actual = null;
                    if (lists.isNotNull(r)) {
                        actual = new ArrayList<>();
                        for (int v = offsets[r]; v < offsets[r + 1]; v++) {
                            actual.add(elements.isNull(v) ? null : values[v]);
                        }
                    }
                    assertThat(actual).as("record %d", record).isEqualTo(expectedTags(record));
                    record++;
                }
            }
            assertThat(record).isEqualTo(TOTAL_ROWS);
        }
    }

    private static void assertRow(RowReader rows, int id) {
        assertThat(rows.getInt("id")).as("id").isEqualTo(id);
        List<String> expected = expectedTags(id);
        if (expected == null) {
            assertThat(rows.isNull("tags")).as("row %d tags null", id).isTrue();
            return;
        }
        PqList tags = rows.getList("tags");
        assertThat(tags).as("row %d tags", id).isNotNull();
        assertThat(new ArrayList<>(tags.strings())).as("row %d tags", id).isEqualTo(expected);
    }

    /// The `tags` value of row `i`, as `tools/simple-datagen.py` writes it.
    private static List<String> expectedTags(int i) {
        if (i % 13 == 5) {
            return null;
        }
        if (i % 11 == 4) {
            return List.of();
        }
        String[] tags = new String[i % 4 + 1];
        for (int j = 0; j < tags.length; j++) {
            tags[j] = (i + j) % 9 == 0 ? null : "r" + i + "-e" + j;
        }
        return Arrays.asList(tags);
    }

    private static int tagsColumnIndex(FileSchema schema) {
        for (int c = 0; c < schema.getColumnCount(); c++) {
            if (schema.getColumn(c).fieldPath().topLevelName().equals("tags")) {
                return c;
            }
        }
        throw new IllegalStateException("No tags column in " + FIXTURE);
    }
}
