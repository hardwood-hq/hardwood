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
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential value comparison for the fixed-size-list read fast path: reads
/// fixed-width `LIST` columns through hardwood (which engages the fast path)
/// and through DuckDB (the oracle, which reads each `LIST` as a SQL array) and
/// asserts the vectors agree element for element.
///
/// The base fixture exercises the two fast-path decode routes and the fallback:
/// `vec_f32` is plain-encoded, `vec_i32` is dictionary-encoded (both fast path),
/// and `vec_null` is a nullable list whose null rows force the regular path.
/// `vec_i64`, `vec_f64`, and `vec_bool` add the remaining primitive element types
/// the fast path bulk-copies (INT64/DOUBLE/BOOLEAN), so every `copyValueRun` arm
/// is checked against the oracle.
/// Further fixtures push the fast path across the fallback-sensitive paths a
/// single uncompressed, single-page, single-row-group file cannot reach:
/// snappy/zstd compression, small pages and multiple row groups, a column mixing
/// fixed-width and null pages (the batch-homogeneity flush), and a row filter over a
/// fast-path column (`compactNestedBatch`). All are checked against an external
/// reader rather than only against hardwood's own reconstruction path.
@Tag("differential")
class DifferentialFixedSizeListTest {

    private static final Path DIR = Paths.get("src/test/resources/differential");
    private static final Path FILE = DIR.resolve("diff_fixed_size_list.parquet");
    private static final long EXPECTED_ROWS = 120;

    /// The fast path is opt-in (off by default), so it must be enabled explicitly
    /// for the oracle comparison to actually exercise it rather than falling back
    /// to the regular reconstruction path.
    private static final ReaderConfig FAST_PATH_ON =
            ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "true").build();

    @Test
    void fixedSizeListValuesMatchOracle() throws Exception {
        assertBaseCorpusMatchesOracle(FILE, null, EXPECTED_ROWS);
    }

    @ParameterizedTest
    @ValueSource(strings = {"snappy", "zstd"})
    void compressedFixedSizeListValuesMatchOracle(String codec) throws Exception {
        assertBaseCorpusMatchesOracle(
                DIR.resolve("diff_fixed_size_list_" + codec + ".parquet"), null, EXPECTED_ROWS);
    }

    /// A row filter over the fast-path columns forces the compaction path
    /// (`compactNestedBatch` with `fixedListK`); the surviving vectors must still
    /// match the oracle's `WHERE`-filtered result.
    @Test
    void filteredFixedSizeListValuesMatchOracle() throws Exception {
        assertBaseCorpusMatchesOracle(
                FILE, "__row__ >= 40", EXPECTED_ROWS - 40);
    }

    /// Small pages and row groups spread the fast-path column across many page and
    /// row-group boundaries; `vec_mixed` additionally interleaves fixed-width and null
    /// pages so a batch spanning both exercises the batch-homogeneity flush.
    @Test
    void pagedFixedSizeListValuesMatchOracle() throws Exception {
        Path file = DIR.resolve("diff_fixed_size_list_paged.parquet");
        String sql = "SELECT * FROM read_parquet('" + file.toAbsolutePath() + "') ORDER BY __row__";
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context, FAST_PATH_ON);
             RowReader rows = reader.buildRowReader().build();
             Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            long seen = 0;
            while (rs.next()) {
                assertThat(rows.hasNext()).as("hardwood has a row for oracle row %d", seen).isTrue();
                rows.next();
                long row = rows.getLong("__row__");
                assertThat(row).as("row order").isEqualTo(rs.getLong("__row__"));

                assertFloatVector(rows, "vec_clean", oracleArray(rs, "vec_clean"), row);
                assertNullableFloatVector(rows, "vec_mixed", oracleArray(rs, "vec_mixed"), row);

                seen++;
            }
            assertThat(rows.hasNext()).as("hardwood exhausted with oracle").isFalse();
            assertThat(seen).as("row count").isEqualTo(400);
        }
    }

    private static void assertBaseCorpusMatchesOracle(Path file, String where, long expectedRows)
            throws Exception {
        String sql = "SELECT * FROM read_parquet('" + file.toAbsolutePath() + "')"
                + (where == null ? "" : " WHERE " + where) + " ORDER BY __row__";
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context, FAST_PATH_ON);
             RowReader rows = openRows(reader, where);
             Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            long seen = 0;
            while (rs.next()) {
                assertThat(rows.hasNext()).as("hardwood has a row for oracle row %d", seen).isTrue();
                rows.next();
                long row = rows.getLong("__row__");
                assertThat(row).as("row order").isEqualTo(rs.getLong("__row__"));

                assertFloatVector(rows, "vec_f32", oracleArray(rs, "vec_f32"), row);
                assertIntVector(rows, "vec_i32", oracleArray(rs, "vec_i32"), row);
                assertLongVector(rows, "vec_i64", oracleArray(rs, "vec_i64"), row);
                assertDoubleVector(rows, "vec_f64", oracleArray(rs, "vec_f64"), row);
                assertBooleanVector(rows, "vec_bool", oracleArray(rs, "vec_bool"), row);
                assertNullableFloatVector(rows, "vec_null", oracleArray(rs, "vec_null"), row);

                seen++;
            }
            assertThat(rows.hasNext()).as("hardwood exhausted with oracle").isFalse();
            assertThat(seen).as("row count").isEqualTo(expectedRows);
        }
    }

    private static RowReader openRows(ParquetFileReader reader, String where) {
        RowReaderBuilder builder = reader.buildRowReader();
        if (where != null) {
            builder.filter(FilterPredicate.gtEq("__row__", 40L));
        }
        return builder.build();
    }

    private static Object[] oracleArray(ResultSet rs, String column) throws Exception {
        Array array = rs.getArray(column);
        return array == null ? null : (Object[]) array.getArray();
    }

    private static void assertFloatVector(RowReader rows, String column, Object[] oracle, long row) {
        List<Float> actual = rows.getList(column).floats();
        assertThat(actual).as("%s length @%d", column, row).hasSize(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat(actual.get(i).floatValue())
                    .as("%s[%d] @%d", column, i, row)
                    .isEqualTo(((Number) oracle[i]).floatValue());
        }
    }

    private static void assertIntVector(RowReader rows, String column, Object[] oracle, long row) {
        List<Object> actual = rows.getList(column).values();
        assertThat(actual).as("%s length @%d", column, row).hasSize(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat(((Number) actual.get(i)).intValue())
                    .as("%s[%d] @%d", column, i, row)
                    .isEqualTo(((Number) oracle[i]).intValue());
        }
    }

    private static void assertLongVector(RowReader rows, String column, Object[] oracle, long row) {
        List<Object> actual = rows.getList(column).values();
        assertThat(actual).as("%s length @%d", column, row).hasSize(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat(((Number) actual.get(i)).longValue())
                    .as("%s[%d] @%d", column, i, row)
                    .isEqualTo(((Number) oracle[i]).longValue());
        }
    }

    private static void assertDoubleVector(RowReader rows, String column, Object[] oracle, long row) {
        List<Object> actual = rows.getList(column).values();
        assertThat(actual).as("%s length @%d", column, row).hasSize(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat(((Number) actual.get(i)).doubleValue())
                    .as("%s[%d] @%d", column, i, row)
                    .isEqualTo(((Number) oracle[i]).doubleValue());
        }
    }

    private static void assertBooleanVector(RowReader rows, String column, Object[] oracle, long row) {
        List<Object> actual = rows.getList(column).values();
        assertThat(actual).as("%s length @%d", column, row).hasSize(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat((Boolean) actual.get(i))
                    .as("%s[%d] @%d", column, i, row)
                    .isEqualTo((Boolean) oracle[i]);
        }
    }

    private static void assertNullableFloatVector(RowReader rows, String column,
                                                  Object[] oracle, long row) {
        if (oracle == null) {
            assertThat(rows.isNull(column)).as("%s null @%d", column, row).isTrue();
        }
        else {
            assertThat(rows.isNull(column)).as("%s present @%d", column, row).isFalse();
            assertFloatVector(rows, column, oracle, row);
        }
    }
}
