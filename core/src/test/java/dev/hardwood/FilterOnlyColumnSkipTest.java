/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A column the predicate references and the projection does not is not read in a row group
/// statistics proved to match the filter in full, and it is not reachable through a reader's
/// accessors in any row group. See `_designs/FILTER_ONLY_COLUMN_SKIP.md`.
///
/// Both fixtures hold three row groups of 1000 rows, `id` running `0..2999`. The filter
/// `id < 1500` decides the first row group `ALWAYS_MATCHES`, leaves the second to the record
/// filter, and prunes the third. `id >= 1500` mirrors it: the fully matching row group is the
/// last, so a consumer that took a filter-only batch for one of its steps would find none left.
///
/// `500 <= id < 2500` places the fully matching row group between two undecided ones, where the
/// filter-only column's batches must close at the rows the payload columns' batches close at.
///
/// Whether a column was read in a row group is observed through `RowGroupScanned`, which the
/// fetch plan of a column that is read emits and a skipped column's does not. Each test reads
/// its own copy of a fixture, so an event a sibling test emitted late cannot stand in for one
/// this test expects, or against one it expects absent.
class FilterOnlyColumnSkipTest extends AbstractJfrRecorderTest {

    private static final int ROWS = 3000;
    private static final int ROWS_PER_GROUP = 1000;
    private static final long THRESHOLD = 1500;
    private static final String ROW_GROUP_SCANNED_EVENT = "dev.hardwood.RowGroupScanned";

    @TempDir
    static Path dir;

    private static Path flatFixture;
    private static Path nestedFixture;

    /// This test's copies of the fixtures.
    private Path flatFile;
    private Path nestedFile;

    @BeforeEach
    void copyFixtures(TestInfo test) throws IOException {
        String name = test.getTestMethod().orElseThrow().getName() + "_" + Math.abs(test.getDisplayName().hashCode());
        flatFile = Files.copy(flatFixture, dir.resolve(name + "_flat.parquet"));
        nestedFile = Files.copy(nestedFixture, dir.resolve(name + "_nested.parquet"));
    }

    @BeforeAll
    static void writeFixtures() throws IOException {
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(ROWS_PER_GROUP).build();

        flatFixture = dir.resolve("filter_only_flat.parquet");
        FileSchema flat = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("amount", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .addColumn("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(flatFixture), flat, config)) {
            for (int i = 0; i < ROWS; i++) {
                final long id = i;
                writer.rowWriter().writeRow(row -> {
                    row.setLong("id", id).setString("label", label(id));
                    if (amountIsNull(id)) {
                        row.setNull("amount");
                    }
                    else {
                        row.setDouble("amount", amount(id));
                    }
                });
            }
        }

        // `s` is optional, so a column reader decodes its leaves as nested.
        nestedFixture = dir.resolve("filter_only_nested.parquet");
        FileSchema nested = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("code", PhysicalType.INT64, RepetitionType.REQUIRED)
                        .addColumn("amount", PhysicalType.DOUBLE, RepetitionType.REQUIRED))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(nestedFixture), nested, config)) {
            for (int i = 0; i < ROWS; i++) {
                final long id = i;
                writer.rowWriter().writeRow(row -> row.setLong("id", id)
                        .setStruct("s", s -> s.setLong("code", id).setDouble("amount", amount(id))));
            }
        }
    }

    private static String label(long id) {
        return String.format("L%05d", id);
    }

    private static double amount(long id) {
        return id * 0.5;
    }

    private static boolean amountIsNull(long id) {
        return id % 7 == 0;
    }

    private static List<String> expectedLabels(long count) {
        return expectedLabels(0, count);
    }

    private static List<String> expectedLabels(long from, long to) {
        List<String> labels = new ArrayList<>();
        for (long id = from; id < to; id++) {
            labels.add(label(id));
        }
        return labels;
    }

    private static List<Double> expectedAmounts(long count, IntFunction<Boolean> isNull) {
        return expectedAmounts(0, count, isNull);
    }

    private static List<Double> expectedAmounts(long from, long to, IntFunction<Boolean> isNull) {
        List<Double> amounts = new ArrayList<>();
        for (int id = Math.toIntExact(from); id < to; id++) {
            amounts.add(isNull.apply(id) ? null : amount(id));
        }
        return amounts;
    }

    // ==================== Flat row reader ====================

    @Test
    void flatRowReaderSkipsTheFilterOnlyColumnInTheFullyMatchingRowGroup() throws Exception {
        assertFlatRowReaderReadsTheMatchingRows(FilterPredicate.lt("id", THRESHOLD));
        assertSkipped(flatFile, "id", "label");
    }

    @Test
    void flatRowReaderSkipsTheFilterOnlyColumnOnTheRecordMatcherPath() throws Exception {
        assertFlatRowReaderReadsTheMatchingRows(forRecordMatcher(
                FilterPredicate.lt("id", THRESHOLD), FilterPredicate.lt("id", -5L)));
        assertSkipped(flatFile, "id", "label");
    }

    @Test
    void flatRowReaderSkipsABinaryFilterOnlyColumn() throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.lt("label", label(THRESHOLD)))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).hasSize((int) THRESHOLD);
        assertThat(ids.get(0)).isZero();
        assertThat(ids.get(ids.size() - 1)).isEqualTo(THRESHOLD - 1);
        assertSkipped(flatFile, "label", "id");
    }

    @ParameterizedTest
    @ValueSource(ints = { 500, 1000, 1200, 5000 })
    void flatRowReaderHonoursHeadAcrossTheSkippedRowGroup(int head) throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("label"))
                     .filter(FilterPredicate.lt("id", THRESHOLD))
                     .head(head)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(Math.min(head, THRESHOLD)));
    }

    @Test
    void flatRowReaderDoesNotResolveTheFilterOnlyColumnInAnyRowGroup() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("label"))
                     .filter(FilterPredicate.lt("id", THRESHOLD))
                     .build()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
                assertThatThrownBy(() -> rows.getLong("id"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("[" + flatFile.getFileName() + "] Column not in projection: id");
                // The JVM's own bounds check raises it; its message is dropped once the
                // throw site is compiled, so only the type is pinned.
                assertThatThrownBy(() -> rows.getLong(1))
                        .isInstanceOf(IndexOutOfBoundsException.class);
            }
            assertThat(count).isEqualTo(THRESHOLD);
        }
    }

    private void assertFlatRowReaderReadsTheMatchingRows(FilterPredicate filter) throws IOException {
        List<String> labels = new ArrayList<>();
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("amount", "label"))
                     .filter(filter)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
                amounts.add(rows.isNull("amount") ? null : rows.getDouble("amount"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(THRESHOLD));
        assertThat(amounts).isEqualTo(expectedAmounts(THRESHOLD, id -> amountIsNull(id)));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void flatRowReaderUnderHeadCountsTheSkippedRowGroupWithoutEvaluatingIt(boolean recordMatcher)
            throws Exception {
        // Under a cap the reader counts matches row by row, in a fully matching batch too.
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("label"))
                     .filter(atLeastThreshold(recordMatcher))
                     .head(1200)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(THRESHOLD, THRESHOLD + 1200));
        assertSkipped(flatFile, "id", "label", 2);
    }

    /// `id >= 1500`, compiled drain-side, or through the record matcher when `recordMatcher`.
    private static FilterPredicate atLeastThreshold(boolean recordMatcher) {
        FilterPredicate atLeast = FilterPredicate.gtEq("id", THRESHOLD);
        return recordMatcher ? forRecordMatcher(atLeast, FilterPredicate.gt("id", 5000L)) : atLeast;
    }

    /// `matching OR never`, in a shape the drain side does not compile — two branches that
    /// each read both `id` and `label` — so the record matcher evaluates it. Every label is
    /// at least `"L"` and none is below `"A"`, so the result is the rows `matching` accepts,
    /// and statistics decide each row group as they decide `matching`.
    private static FilterPredicate forRecordMatcher(FilterPredicate matching, FilterPredicate never) {
        return FilterPredicate.or(
                FilterPredicate.and(matching, FilterPredicate.gtEq("label", "L")),
                FilterPredicate.and(never, FilterPredicate.lt("label", "A")));
    }

    // ==================== Column readers ====================

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void columnReadersAnswerTheSkippedRowGroupWithoutEvaluatingIt(boolean recordMatcher) throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("amount", "label"))
                     .filter(atLeastThreshold(recordMatcher))
                     .build()) {
            while (columns.nextBatch()) {
                String[] values = columns.getColumnReader("label").getStrings();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    labels.add(values[i]);
                }
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(THRESHOLD, ROWS));
        assertSkipped(flatFile, "id", "label", 2);
    }

    @Test
    void columnReaderSkipsTheFilterOnlyColumnInTheFullyMatchingRowGroup() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             ColumnReader column = reader.buildColumnReader("amount")
                     .filter(FilterPredicate.lt("id", THRESHOLD))
                     .build()) {
            while (column.nextBatch()) {
                double[] values = column.getDoubles();
                Validity validity = column.getLeafValidity();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    amounts.add(validity.isNull(i) ? null : values[i]);
                }
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(THRESHOLD, id -> amountIsNull(id)));
        assertSkipped(flatFile, "id", "amount");
    }

    @Test
    void columnReadersSkipTheFilterOnlyColumnOnTheRecordMatcherBackend() throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("amount", "label"))
                     .filter(forRecordMatcher(
                             FilterPredicate.lt("id", THRESHOLD), FilterPredicate.lt("id", -5L)))
                     .build()) {
            while (columns.nextBatch()) {
                String[] values = columns.getColumnReader("label").getStrings();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    labels.add(values[i]);
                }
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(THRESHOLD));
        assertSkipped(flatFile, "id", "label");
    }

    @Test
    void columnReadersRecycleTheFilterOnlyColumnsBatches() throws Exception {
        // 64-row batches cycle the filter-only column's batch pool many times over. With the
        // `IN` list every row group is evaluated, so each reused batch must hold its own values.
        List<String> scattered = readLabels(FilterPredicate.in("id", 3L, 700L, 1001L, 1234L, 1999L, 2500L, 2999L));
        assertThat(scattered).containsExactly(
                label(3), label(700), label(1001), label(1234), label(1999), label(2500), label(2999));

        // Here the filter-only column is not read in the first row group, so its cursor keeps
        // one batch across the proven steps before the second row group's cycle the pool.
        assertThat(readLabels(FilterPredicate.lt("id", THRESHOLD))).isEqualTo(expectedLabels(THRESHOLD));
    }

    private List<String> readLabels(FilterPredicate filter) throws IOException {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label"))
                     .filter(filter)
                     .batchSize(64)
                     .build()) {
            while (columns.nextBatch()) {
                String[] values = columns.getColumnReader("label").getStrings();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    labels.add(values[i]);
                }
            }
        }
        return labels;
    }

    @Test
    void nestedColumnReaderSkipsANestedFilterOnlyColumn() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             ColumnReader column = reader.buildColumnReader("s.amount")
                     .filter(FilterPredicate.lt("s.code", THRESHOLD))
                     .build()) {
            while (column.nextBatch()) {
                double[] values = column.getDoubles();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    amounts.add(values[i]);
                }
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(THRESHOLD, id -> false));
        assertSkipped(nestedFile, "code", "amount");
    }

    // ==================== Nested row reader ====================

    @Test
    void nestedRowReaderSkipsATopLevelFilterOnlyColumn() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("s"))
                     .filter(FilterPredicate.lt("id", THRESHOLD))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                amounts.add(rows.getStruct("s").getDouble("amount"));
                assertThatThrownBy(() -> rows.getLong("id"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Field 'id' not in projection");
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(THRESHOLD, id -> false));
        assertSkipped(nestedFile, "id", "amount");
    }

    @Test
    void nestedRowReaderSkipsAFilterOnlyLeafUnderAProjectedStruct() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("s.amount"))
                     .filter(FilterPredicate.lt("s.code", THRESHOLD))
                     .head(1200)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                PqStruct s = rows.getStruct("s");
                amounts.add(s.getDouble("amount"));
                assertThat(s.getFieldCount()).isEqualTo(1);
                assertThatThrownBy(() -> s.getLong("code"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Field not found: code");
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(1200, id -> false));
        assertSkipped(nestedFile, "code", "amount");
    }

    @ParameterizedTest
    @ValueSource(ints = { 500, 1000, 1200, 5000 })
    void nestedRowReaderHonoursHeadAcrossTheSkippedRowGroup(int head) throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("s"))
                     .filter(FilterPredicate.lt("id", THRESHOLD))
                     .head(head)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                amounts.add(rows.getStruct("s").getDouble("amount"));
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(Math.min(head, THRESHOLD), id -> false));
    }

    // ==================== Column index disagreeing with the chunk statistics ====================

    /// Row group 0's `id` chunk statistics claim `[500, 999]` while its column index keeps the
    /// real page bounds `0..999`, in pages of 100 rows. `500 <= id < 1250` is proven for row
    /// group 0 by the chunk statistics while page filtering drops its first five pages, and row
    /// group 1 (`1000..1999`) is left to the record filter.
    private static final Path INDEX_DISAGREES = Path.of("src/test/resources/filter_only_column_index_disagrees.parquet");

    private static FilterPredicate idFrom500To1250(boolean recordMatcher) {
        FilterPredicate matching = FilterPredicate.and(FilterPredicate.gtEq("id", 500L), FilterPredicate.lt("id", 1250L));
        return recordMatcher ? forRecordMatcher(matching, FilterPredicate.lt("id", -5L)) : matching;
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rowReaderSkipsTheFilterOnlyColumnInAProvenRowGroupThePageIndexNarrows(boolean recordMatcher)
            throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INDEX_DISAGREES));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("label"))
                     .filter(idFrom500To1250(recordMatcher))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(500, 1250));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void columnReadersSkipTheFilterOnlyColumnInAProvenRowGroupThePageIndexNarrows(boolean recordMatcher)
            throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INDEX_DISAGREES));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label"))
                     .filter(idFrom500To1250(recordMatcher))
                     .batchSize(64)
                     .build()) {
            while (columns.nextBatch()) {
                String[] values = columns.getColumnReader("label").getStrings();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    labels.add(values[i]);
                }
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(500, 1250));
    }

    // ==================== Fully matching row group between undecided ones ====================

    private static final long LOWER = 500;
    private static final long UPPER = 2500;

    /// `LOWER <= column < UPPER`: row groups 0 and 2 undecided, row group 1 fully matching.
    private static FilterPredicate between(String column) {
        return FilterPredicate.and(FilterPredicate.gtEq(column, LOWER), FilterPredicate.lt(column, UPPER));
    }

    /// [#between] on `id`, compiled drain-side, or through the record matcher when `recordMatcher`.
    private static FilterPredicate idBetween(boolean recordMatcher) {
        return recordMatcher
                ? forRecordMatcher(between("id"), FilterPredicate.lt("id", -5L))
                : between("id");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void flatRowReaderSkipsTheFilterOnlyColumnBetweenUndecidedRowGroups(boolean recordMatcher) throws Exception {
        List<String> labels = new ArrayList<>();
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("amount", "label"))
                     .filter(idBetween(recordMatcher))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
                amounts.add(rows.isNull("amount") ? null : rows.getDouble("amount"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(LOWER, UPPER));
        assertThat(amounts).isEqualTo(expectedAmounts(LOWER, UPPER, id -> amountIsNull(id)));
        assertSkippedBetween(flatFile, "id", "label");
    }

    @ParameterizedTest
    @CsvSource({ "1000, false", "1000, true", "1800, false", "1800, true" })
    void flatRowReaderHonoursHeadAroundTheSkippedRowGroup(int head, boolean recordMatcher) throws Exception {
        // 1000 matches end inside the fully matching row group, 1800 inside the last one.
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("label"))
                     .filter(idBetween(recordMatcher))
                     .head(head)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(LOWER, LOWER + head));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void columnReadersSkipTheFilterOnlyColumnBetweenUndecidedRowGroups(boolean recordMatcher) throws Exception {
        // 64-row batches end mid row group, so the batch in progress when the fully matching
        // row group starts is a short one on every column.
        List<String> labels = new ArrayList<>();
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(flatFile));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("amount", "label"))
                     .filter(idBetween(recordMatcher))
                     .batchSize(64)
                     .build()) {
            while (columns.nextBatch()) {
                String[] labelValues = columns.getColumnReader("label").getStrings();
                ColumnReader amount = columns.getColumnReader("amount");
                double[] amountValues = amount.getDoubles();
                Validity validity = amount.getLeafValidity();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    labels.add(labelValues[i]);
                    amounts.add(validity.isNull(i) ? null : amountValues[i]);
                }
            }
        }
        assertThat(labels).isEqualTo(expectedLabels(LOWER, UPPER));
        assertThat(amounts).isEqualTo(expectedAmounts(LOWER, UPPER, id -> amountIsNull(id)));
        assertSkippedBetween(flatFile, "id", "label");
    }

    @Test
    void nestedRowReaderSkipsTheFilterOnlyColumnBetweenUndecidedRowGroups() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("s.amount"))
                     .filter(between("s.code"))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                amounts.add(rows.getStruct("s").getDouble("amount"));
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(LOWER, UPPER, id -> false));
        assertSkippedBetween(nestedFile, "code", "amount");
    }

    @Test
    void nestedColumnReaderSkipsTheFilterOnlyColumnBetweenUndecidedRowGroups() throws Exception {
        List<Double> amounts = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nestedFile));
             ColumnReader column = reader.buildColumnReader("s.amount")
                     .filter(between("s.code"))
                     .batchSize(64)
                     .build()) {
            while (column.nextBatch()) {
                double[] values = column.getDoubles();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    amounts.add(values[i]);
                }
            }
        }
        assertThat(amounts).isEqualTo(expectedAmounts(LOWER, UPPER, id -> false));
        assertSkippedBetween(nestedFile, "code", "amount");
    }

    // ==================== Helpers ====================

    /// Asserts that `column` of `file` was not read in the first row group and was read in
    /// the second, which statistics left to the record filter. `payloadColumn`, read in both,
    /// shows the recording captured the first row group at all.
    private void assertSkipped(Path file, String column, String payloadColumn) {
        assertSkipped(file, column, payloadColumn, 0);
    }

    /// As [#assertSkipped(Path, String, String)], with the fully matching row group at
    /// `skippedRowGroup`; the undecided one is always row group 1.
    private void assertSkipped(Path file, String column, String payloadColumn, int skippedRowGroup) {
        awaitEvents();
        String fileName = file.getFileName().toString();
        assertThat(scanned(fileName, payloadColumn, skippedRowGroup))
                .as("payload column scanned in row group " + skippedRowGroup).isTrue();
        assertThat(scanned(fileName, column, skippedRowGroup))
                .as("filter-only column scanned in row group " + skippedRowGroup).isFalse();
        assertThat(scanned(fileName, column, 1)).as("filter-only column scanned in row group 1").isTrue();
    }

    /// Asserts that `column` of `file` was not read in row group 1, which statistics proved
    /// to match in full, and was read in row groups 0 and 2, which they left undecided.
    private void assertSkippedBetween(Path file, String column, String payloadColumn) {
        awaitEvents();
        String fileName = file.getFileName().toString();
        assertThat(scanned(fileName, payloadColumn, 1)).as("payload column scanned in row group 1").isTrue();
        assertThat(scanned(fileName, column, 1)).as("filter-only column scanned in row group 1").isFalse();
        assertThat(scanned(fileName, column, 0)).as("filter-only column scanned in row group 0").isTrue();
        assertThat(scanned(fileName, column, 2)).as("filter-only column scanned in row group 2").isTrue();
    }

    private boolean scanned(String fileName, String column, int rowGroupIndex) {
        return events(ROW_GROUP_SCANNED_EVENT)
                .anyMatch(e -> e.getString("file").equals(fileName)
                        && e.getString("column").equals(column)
                        && e.getInt("rowGroupIndex") == rowGroupIndex);
    }
}
