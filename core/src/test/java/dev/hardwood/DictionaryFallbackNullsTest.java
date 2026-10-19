/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A dictionary chunk that falls back to `PLAIN` part-way through, over an optional string column
/// with nulls on page edges and inside pages: every value, null and dictionary id reads as written,
/// through the column readers at batch sizes that put page edges inside batches and batch edges
/// inside pages, through the row reader, and through filtered reads whose binary predicates run per
/// batch as the column completes its cells.
///
/// The fixture has one row group of 4,000 rows in pages of 300: `label` repeats three values over
/// the first 2,000 rows (dictionary pages) and is distinct after (`PLAIN` pages, once the
/// dictionary outgrew its limit); it is null every seventh row and at each page's first and last
/// row. `id` is the row number. A second fixture has the same fallback on a repeated leaf, two
/// optional strings per row.
class DictionaryFallbackNullsTest {

    private static final Path FILE = Path.of("src/test/resources/dict_plain_fallback_nulls.parquet");
    private static final int ROWS = 4_000;
    private static final Path NESTED_FILE = Path.of("src/test/resources/dict_plain_fallback_nested.parquet");
    private static final int NESTED_ROWS = 3_000;

    private static String label(int row) {
        if (row % 7 == 3 || row % 300 == 0 || row % 300 == 299) {
            return null;
        }
        return row < 2_000 ? new String[]{ "alpha", "bravo", "charlie" }[row % 3] : "distinct-%06d".formatted(row);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 7, 64, 299, 301, 1_000, 4_096 })
    void columnReadersReadEveryValueNullAndId(int batchSize) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label", "id"))
                     .batchSize(batchSize)
                     .build()) {
            ColumnReader labels = columns.getColumnReader("label");
            ColumnReader ids = columns.getColumnReader("id");
            int row = 0;
            int batchesWithIds = 0;
            while (columns.nextBatch()) {
                int[] dictionaryIds = labels.getDictionaryIds();
                if (dictionaryIds != null) {
                    batchesWithIds++;
                }
                byte[] bytes = labels.getBinaryValues();
                int[] starts = labels.getBinaryStarts();
                int[] ends = labels.getBinaryEnds();
                String[] strings = labels.getStrings();
                for (int i = 0; i < labels.getRecordCount(); i++, row++) {
                    String expected = label(row);
                    assertThat(ids.getLongs()[i]).isEqualTo(row);
                    assertThat(labels.getLeafValidity().isNull(i)).as("row %d null", row).isEqualTo(expected == null);
                    assertThat(strings[i]).as("row %d", row).isEqualTo(expected);
                    if (expected == null) {
                        continue;
                    }
                    assertThat(new String(bytes, starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
                            .as("row %d view", row).isEqualTo(expected);
                    if (dictionaryIds != null) {
                        assertThat(labels.getBinaryDictionary().getString(dictionaryIds[i]))
                                .as("row %d id", row).isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(ROWS);
            // The first 2,000 rows are dictionary-encoded, so a batch no larger has ids.
            assertThat(batchesWithIds > 0).as("batches whose values all come from the dictionary")
                    .isEqualTo(batchSize <= 2_000);
        }
    }

    /// A batch exposes ids exactly when it draws on the dictionary and every present value came
    /// from a dictionary page, whichever segments the batch's rows fall into. Which page each row
    /// came from is read off single-row batches: one from a dictionary page has ids, a null
    /// included, one from a `PLAIN` page has none. The sizes put the end of a batch on the first
    /// row of a `PLAIN` page, a null (2,100: ids, the batch's only absent entry is in a null slot),
    /// and on its second, a value (2,101: no ids, the absent entry is the batch's last value).
    @ParameterizedTest
    @ValueSource(ints = { 7, 11, 191, 301, 1_051, 2_101 })
    void aBatchHasIdsExactlyWhenEveryPresentValueCameFromTheDictionary(int batchSize) throws Exception {
        boolean[] fromDictionary = new boolean[ROWS];
        forEachBatch(1, (first, labels) -> fromDictionary[first] = labels.getDictionaryIds() != null);
        assertThat(fromDictionary[2_099]).as("last row of the dictionary pages").isTrue();
        assertThat(fromDictionary[2_100]).as("first row of the PLAIN pages").isFalse();

        int[] edgeBatches = new int[2];
        forEachBatch(batchSize, (first, labels) -> {
            int count = labels.getRecordCount();
            boolean drawsOnDictionary = false;
            boolean everyPresentValueHasAnId = true;
            for (int row = first; row < first + count; row++) {
                drawsOnDictionary |= fromDictionary[row];
                everyPresentValueHasAnId &= fromDictionary[row] || label(row) == null;
            }
            int[] ids = labels.getDictionaryIds();
            assertThat(ids != null).as("ids of the batch at row %d", first)
                    .isEqualTo(drawsOnDictionary && everyPresentValueHasAnId);
            int end = first + count;
            if (end == 2_101) {
                assertThat(ids).as("a batch whose only PLAIN row is a null").isNotNull();
                assertThat(ids[count - 1]).isEqualTo(-1);
                edgeBatches[0]++;
            }
            if (end == 2_102 && count > 2) {
                assertThat(ids).as("a batch whose last value is PLAIN").isNull();
                edgeBatches[1]++;
            }
        });
        if (batchSize == 11 || batchSize == 191 || batchSize == 2_101) {
            assertThat(edgeBatches[0]).as("batches ending on the PLAIN pages' first row").isEqualTo(1);
        }
        if (batchSize == 1_051) {
            assertThat(edgeBatches[1]).as("batches ending on the PLAIN pages' second row").isEqualTo(1);
        }
    }

    /// The same rule on a repeated leaf, whose batches end at record boundaries: `labels` holds two
    /// optional strings per row, dictionary-encoded up to some row and `PLAIN` after; both are null
    /// on each page's first row, so the first `PLAIN` record holds only nulls. One batch ends on
    /// that record (ids: its absent entries are in null slots), one on the record after (no ids),
    /// and two sizes put batch edges inside the pages.
    @Test
    void aNestedBatchHasIdsExactlyWhenEveryPresentValueCameFromTheDictionary() throws Exception {
        boolean[] fromDictionary = new boolean[NESTED_ROWS];
        forEachNestedBatch(1, (first, labels) -> fromDictionary[first] = labels.getDictionaryIds() != null);
        int firstPlain = 0;
        while (fromDictionary[firstPlain]) {
            firstPlain++;
        }
        assertThat(firstPlain % 150).as("the PLAIN pages start on a page's first row, %d", firstPlain).isZero();
        assertThat(firstPlain).isBetween(1_500, NESTED_ROWS - 2);

        for (int batchSize : new int[]{ 7, 301, firstPlain + 1, firstPlain + 2 }) {
            forEachNestedBatch(batchSize, (first, labels) -> {
                int count = labels.getRecordCount();
                boolean drawsOnDictionary = false;
                boolean everyPresentValueHasAnId = true;
                for (int row = first; row < first + count; row++) {
                    drawsOnDictionary |= fromDictionary[row];
                    everyPresentValueHasAnId &= fromDictionary[row]
                            || (nestedLabel(row, 0) == null && nestedLabel(row, 1) == null);
                }
                int[] ids = labels.getDictionaryIds();
                assertThat(ids != null).as("ids of the batch at row %d of %d", first, batchSize)
                        .isEqualTo(drawsOnDictionary && everyPresentValueHasAnId);
                if (ids != null) {
                    assertThat(ids).hasSize(labels.getValueCount());
                }
            });
        }
        int[] firstBatch = new int[2];
        forEachNestedBatch(firstPlain + 1, (first, labels) -> {
            if (first == 0) {
                firstBatch[0] = labels.getDictionaryIds() != null ? 1 : -1;
            }
        });
        forEachNestedBatch(firstPlain + 2, (first, labels) -> {
            if (first == 0) {
                firstBatch[1] = labels.getDictionaryIds() != null ? 1 : -1;
            }
        });
        assertThat(firstBatch).as("a batch ending on the null-only PLAIN record, and one past it")
                .containsExactly(1, -1);
    }

    private static String nestedLabel(int row, int element) {
        if (row % 150 == 0 || (element == 0 && row % 5 == 2)) {
            return null;
        }
        return row < 1_500
                ? new String[]{ "alpha", "bravo", "charlie" }[(row + element) % 3]
                : "distinct-%06d-%d".formatted(row, element);
    }

    private static void forEachNestedBatch(int batchSize, BatchCheck check) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE));
             ColumnReader labels = reader.buildColumnReader("labels.list.element").batchSize(batchSize).build()) {
            int row = 0;
            while (labels.nextBatch()) {
                check.check(row, labels);
                String[] strings = labels.getStrings();
                int[] offsets = labels.getLayerOffsets(0);
                for (int r = 0; r < labels.getRecordCount(); r++) {
                    for (int e = 0; e < 2; e++) {
                        assertThat(strings[offsets[r] + e]).as("row %d element %d", row + r, e)
                                .isEqualTo(nestedLabel(row + r, e));
                    }
                }
                row += labels.getRecordCount();
            }
            assertThat(row).isEqualTo(NESTED_ROWS);
        }
    }

    /// A filtered batch has ids exactly when every value it keeps came from the dictionary, though
    /// the batch it was filtered from mixes both encodings: one batch of 4,096 rows reads the
    /// whole file, and `id < 2000` keeps only rows of dictionary pages while `id > 1900` keeps
    /// rows on both sides of the fallback.
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void aFilteredBatchHasIdsExactlyWhenEveryKeptValueCameFromTheDictionary(boolean bothSides) throws Exception {
        boolean[] fromDictionary = new boolean[ROWS];
        forEachBatch(1, (first, labels) -> fromDictionary[first] = labels.getDictionaryIds() != null);
        FilterPredicate filter = bothSides ? FilterPredicate.gt("id", 1_900L) : FilterPredicate.lt("id", 2_000L);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label", "id"))
                     .filter(filter)
                     .batchSize(4_096)
                     .build()) {
            ColumnReader labels = columns.getColumnReader("label");
            ColumnReader ids = columns.getColumnReader("id");
            assertThat(columns.nextBatch()).isTrue();
            boolean everyKeptValueHasAnId = true;
            for (int i = 0; i < labels.getRecordCount(); i++) {
                int row = Math.toIntExact(ids.getLongs()[i]);
                everyKeptValueHasAnId &= fromDictionary[row] || label(row) == null;
            }
            assertThat(everyKeptValueHasAnId).isEqualTo(!bothSides);
            assertThat(labels.getDictionaryIds() != null).isEqualTo(!bothSides);
            assertThat(columns.nextBatch()).isFalse();
        }
    }

    private interface BatchCheck {
        void check(int firstRow, ColumnReader labels);
    }

    private static void forEachBatch(int batchSize, BatchCheck check) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label", "id"))
                     .batchSize(batchSize)
                     .build()) {
            ColumnReader labels = columns.getColumnReader("label");
            int row = 0;
            while (columns.nextBatch()) {
                check.check(row, labels);
                row += labels.getRecordCount();
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rowReaderReadsEveryValueAndNull(boolean asBytes) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {
            int row = 0;
            while (rows.hasNext()) {
                rows.next();
                String expected = label(row);
                assertThat(rows.isNull("label")).as("row %d null", row).isEqualTo(expected == null);
                if (expected != null) {
                    String actual = asBytes
                            ? new String(rows.getBinary("label"), StandardCharsets.UTF_8)
                            : rows.getString("label");
                    assertThat(actual).as("row %d", row).isEqualTo(expected);
                }
                assertThat(rows.getLong("id")).isEqualTo(row);
                row++;
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    /// Each predicate's matching rows, through the row reader and the column readers. `eq` and
    /// `inStrings` are decided by entry id on dictionary batches; `gt` and `notEq` read every
    /// value's bytes; the conjunction spans both columns.
    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3, 4, 5 })
    void filteredReadsKeepTheMatchingRows(int predicate) throws Exception {
        FilterPredicate filter = switch (predicate) {
            case 0 -> FilterPredicate.eq("label", "bravo");
            case 1 -> FilterPredicate.inStrings("label", "alpha", "distinct-002501", "distinct-003998");
            case 2 -> FilterPredicate.gt("label", "distinct-003500");
            case 3 -> FilterPredicate.notEq("label", "charlie");
            case 4 -> FilterPredicate.isNull("label");
            default -> FilterPredicate.and(FilterPredicate.lt("label", "c"), FilterPredicate.gt("id", 1_000L));
        };
        IntPredicate matches = switch (predicate) {
            case 0 -> row -> "bravo".equals(label(row));
            case 1 -> row -> label(row) != null
                    && List.of("alpha", "distinct-002501", "distinct-003998").contains(label(row));
            case 2 -> row -> label(row) != null && label(row).compareTo("distinct-003500") > 0;
            case 3 -> row -> label(row) != null && !label(row).equals("charlie");
            case 4 -> row -> label(row) == null;
            default -> row -> label(row) != null && label(row).compareTo("c") < 0 && row > 1_000;
        };
        List<Long> expected = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            if (matches.test(row)) {
                expected.add((long) row);
            }
        }

        List<Long> viaRows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                String expectedLabel = label(Math.toIntExact(id));
                assertThat(rows.isNull("label") ? null : rows.getString("label")).isEqualTo(expectedLabel);
                viaRows.add(id);
            }
        }
        assertThat(viaRows).isEqualTo(expected);

        List<Long> viaColumns = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label", "id"))
                     .filter(filter)
                     .batchSize(97)
                     .build()) {
            ColumnReader labels = columns.getColumnReader("label");
            ColumnReader ids = columns.getColumnReader("id");
            while (columns.nextBatch()) {
                String[] strings = labels.getStrings();
                for (int i = 0; i < ids.getRecordCount(); i++) {
                    long id = ids.getLongs()[i];
                    assertThat(strings[i]).isEqualTo(label(Math.toIntExact(id)));
                    viaColumns.add(id);
                }
            }
        }
        assertThat(viaColumns).isEqualTo(expected);
    }
}
