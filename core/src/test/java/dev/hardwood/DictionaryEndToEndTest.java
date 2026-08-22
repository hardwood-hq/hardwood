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
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end dictionary pruning through the public reader APIs against
/// `column_index_pushdown_dict.parquet` (one row group, 10 000 rows; `id` INT64 `0..9999`,
/// `category` STRING cycling `"cat_0".."cat_9"`). The file's only row group is dropped entirely
/// when the predicate value falls inside the column's statistics range but is proven absent by the
/// dictionary, so no rows — and on remote backends, no data pages — are read.
///
/// `category` holds only `"cat_0".."cat_9"`: `eq("category", "cat_5x")` sorts inside
/// `["cat_0", "cat_9"]` (statistics keep) but was never written (dictionary drops);
/// `eq("category", "cat_5")` is present. Complements
/// [dev.hardwood.internal.predicate.DictionaryPushDownTest], which asserts the evaluator decision
/// directly; here the assertions run through `ColumnReader`, `ColumnReaders`, and `RowReader`.
class DictionaryEndToEndTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    private static final FilterPredicate ABSENT = FilterPredicate.eq("category", "cat_5x");
    private static final FilterPredicate PRESENT = FilterPredicate.eq("category", "cat_5");

    /// `category` cycles over ten values across 10 000 rows, so exactly a tenth match `"cat_5"`.
    private static final int PRESENT_ROWS = 1000;
    /// Repeating the 10 000-row fixture this many times exceeds three maximum
    /// two-column batch capacities, forcing the recycling exchange to reuse a
    /// batch holder after several dictionary changes.
    private static final int REPEATED_FILES = 80;

    @Test
    void fixtureIsDictionaryEncodedOnTheFilteredColumn() throws Exception {
        // Precondition for every prune assertion below: the filtered columns must actually be
        // dictionary-encoded.
        InputFile inputFile = InputFile.of(FIXTURE);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            assertIsDictionaryEncoded(reader, inputFile, "category");
        }
    }

    @Test
    void columnReaderPrunesRowGroupForAbsentValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (ColumnReader col = reader.buildColumnReader("category").filter(ABSENT).build()) {
                assertThat(countRows(col)).isZero();
            }
            try (ColumnReader col = reader.buildColumnReader("category").filter(PRESENT).build()) {
                assertThat(countRows(col)).isEqualTo(PRESENT_ROWS);
            }
        }
    }

    @Test
    void columnReadersProjectionPrunesRowGroupForAbsentValue() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("category", "id");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (ColumnReaders cols = reader.buildColumnReaders(projection).filter(ABSENT).build()) {
                assertThat(countRows(cols.getColumnReader("category"))).isZero();
            }
            try (ColumnReaders cols = reader.buildColumnReaders(projection).filter(PRESENT).build()) {
                ColumnReader category = cols.getColumnReader("category");
                ColumnReader id = cols.getColumnReader("id");
                assertThat(category.nextBatch() & id.nextBatch()).isTrue();
                assertThat(category.getStrings()[0]).isEqualTo("cat_5");
                // category is "cat_" + (id % 10), so the first match is id == 5.
                assertThat(id.getLongs()[0]).isEqualTo(5L);
            }
        }
    }

    @Test
    void rowReaderPrunesRowGroupForAbsentValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (RowReader rows = reader.buildRowReader().filter(ABSENT).build()) {
                assertThat(countRows(rows)).isZero();
            }
            try (RowReader rows = reader.buildRowReader().filter(PRESENT).build()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getString("category")).isEqualTo("cat_5");
            }
        }
    }

    @Test
    void rowReaderPrunesRowGroupOnlyWhenEveryInListValueIsAbsent() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            FilterPredicate allAbsent = FilterPredicate.inStrings("category", "cat_5x", "cat_7x");
            try (RowReader rows = reader.buildRowReader().filter(allAbsent).build()) {
                assertThat(countRows(rows)).isZero();
            }
            // One stored value keeps the row group; only its rows survive record-level filtering.
            FilterPredicate onePresent = FilterPredicate.inStrings("category", "cat_5x", "cat_3");
            try (RowReader rows = reader.buildRowReader().filter(onePresent).build()) {
                assertThat(countRows(rows)).isEqualTo(PRESENT_ROWS);
            }
        }
    }

    @Test
    void rowReaderKeepsDictionaryEqualityCorrectAcrossFilesAndRecycledBatches() throws Exception {
        List<InputFile> files = IntStream.range(0, REPEATED_FILES)
                .mapToObj(ignored -> InputFile.of(FIXTURE))
                .toList();
        try (ParquetFileReader reader = ParquetFileReader.openAll(files);
             RowReader rows = reader.buildRowReader().filter(PRESENT).build()) {
            assertThat(countRows(rows)).isEqualTo(PRESENT_ROWS * REPEATED_FILES);
        }
    }

    /// Asserts push-down can read a dictionary for `column` in the fixture's single row group —
    /// the chunk is fully dictionary-encoded and its page is locatable. The column is looked up by
    /// name so this does not depend on column ordering.
    private static void assertIsDictionaryEncoded(ParquetFileReader reader, InputFile inputFile,
            String column) {
        RowGroup rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        int columnIndex = IntStream.range(0, rowGroup.columns().size())
                .filter(i -> rowGroup.columns().get(i).metaData().pathInSchema().toString().equals(column))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column named '" + column + "' in the fixture"));
        FileSchema schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThat(new RowGroupDictionaryFilterSource(inputFile, rowGroup, schema, context)
                    .forColumn(columnIndex))
                    .as("dictionary for column '%s'", column)
                    .isNotNull();
        }
    }

    private static int countRows(ColumnReader reader) {
        int total = 0;
        while (reader.nextBatch()) {
            total += reader.getRecordCount();
        }
        return total;
    }

    private static int countRows(RowReader reader) {
        int total = 0;
        while (reader.hasNext()) {
            reader.next();
            total++;
        }
        return total;
    }
}
