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
/// batch on the column's worker.
///
/// The fixture has one row group of 4,000 rows in pages of 300: `label` repeats three values over
/// the first 2,000 rows (dictionary pages) and is distinct after (`PLAIN` pages, once the
/// dictionary outgrew its limit); it is null every seventh row and at each page's first and last
/// row. `id` is the row number.
class DictionaryFallbackNullsTest {

    private static final Path FILE = Path.of("src/test/resources/dict_plain_fallback_nulls.parquet");
    private static final int ROWS = 4_000;

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
