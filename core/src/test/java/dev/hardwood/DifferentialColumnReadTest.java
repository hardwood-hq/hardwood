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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.ColumnReaderBuilder;
import dev.hardwood.reader.ParquetFileReader.ColumnReadersBuilder;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential test of the column readers' row selection: `head`, `skip` and `rowRange`, alone
/// and with a filter, run through a [ColumnReaders] and a single [ColumnReader] and through DuckDB
/// over the same file, the two required to return the same ordered `__row__` values.
///
/// `head` and `skip` map to SQL as on the row reader ([DifferentialReadTest]). `rowRange(a, b)`
/// bounds the physical position, `__row__ >= a AND __row__ < b`, ahead of the filter, so a filter
/// narrows the rows inside the range rather than shifting it.
@Tag("differential")
class DifferentialColumnReadTest {

    private static final Path RES = Paths.get("src/test/resources/differential");

    record Fixture(String name, Path path) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static final Fixture SINGLE = new Fixture("single-rg", RES.resolve("diff_numbers_single.parquet"));
    private static final Fixture MULTI = new Fixture("multi-rg", RES.resolve("diff_numbers_multi.parquet"));

    /// One query, rendered to both builders and to SQL. `null` fields are omitted on every side;
    /// `filter` and `where` state the same predicate on `id`, which is `__row__ + 1`.
    record Query(String name, FilterPredicate filter, String where, Long skip, Long head, long[] range) {

        void applyTo(ColumnReadersBuilder b) {
            if (filter != null) {
                b.filter(filter);
            }
            if (skip != null) {
                b.skip(skip);
            }
            if (head != null) {
                b.head(head);
            }
            if (range != null) {
                b.rowRange(range[0], range[1]);
            }
        }

        void applyTo(ColumnReaderBuilder b) {
            if (filter != null) {
                b.filter(filter);
            }
            if (skip != null) {
                b.skip(skip);
            }
            if (head != null) {
                b.head(head);
            }
            if (range != null) {
                b.rowRange(range[0], range[1]);
            }
        }

        String sql(Path file) {
            List<String> conditions = new ArrayList<>();
            if (range != null) {
                conditions.add("__row__ >= " + range[0] + " AND __row__ < " + range[1]);
            }
            if (where != null) {
                conditions.add(where);
            }
            String whereClause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
            String limit = head != null ? String.valueOf(head) : "ALL";
            long offset = skip != null ? skip : 0L;
            return "SELECT __row__ FROM read_parquet('" + file.toAbsolutePath() + "')" + whereClause
                    + " ORDER BY __row__ LIMIT " + limit + " OFFSET " + offset;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static Query positioned(String name, Long skip, Long head) {
        return new Query(name, null, null, skip, head, null);
    }

    private static Query filtered(String name, FilterPredicate filter, String where, Long skip, Long head) {
        return new Query(name, filter, where, skip, head, null);
    }

    private static Query ranged(String name, long start, long end) {
        return new Query(name, null, null, null, null, new long[]{ start, end });
    }

    private static Query rangedFiltered(String name, FilterPredicate filter, String where, long start, long end) {
        return new Query(name, filter, where, null, null, new long[]{ start, end });
    }

    static List<Query> queries() {
        return List.of(
                // head and skip as on the row reader
                positioned("head 40", null, 40L),
                positioned("skip 40", 40L, null),
                positioned("skip 30 + head 40", 30L, 40L),
                positioned("skip 45 + head 10 across a row-group boundary", 45L, 10L),
                positioned("skip == total", 250L, null),
                positioned("skip beyond total", 300L, null),
                filtered("filter + head", FilterPredicate.gt("id", 100L), "id > 100", null, 20L),
                filtered("filter + skip", FilterPredicate.gt("id", 100L), "id > 100", 30L, null),
                filtered("filter + skip + head", FilterPredicate.gt("id", 100L), "id > 100", 10L, 20L),
                filtered("filter + skip beyond matches", FilterPredicate.gt("id", 100L), "id > 100", 300L, null),
                // a physical row range
                ranged("range [30, 70)", 30, 70),
                ranged("range [45, 55) across a row-group boundary", 45, 55),
                ranged("range [50, 100) of whole row groups", 50, 100),
                ranged("range [0, 250) of every row", 0, 250),
                ranged("range [100, 100) empty", 100, 100),
                ranged("range [240, 300) past the end", 240, 300),
                ranged("range [250, 260) at the end", 250, 260),
                ranged("range [300, 400) beyond the end", 300, 400),
                // a physical row range, narrowed by a filter
                rangedFiltered("filter id>100 + range [80, 160)", FilterPredicate.gt("id", 100L), "id > 100", 80, 160),
                rangedFiltered("filter id<=60 + range [30, 130)", FilterPredicate.ltEq("id", 60L), "id <= 60", 30, 130),
                rangedFiltered("filter id==100 + range [0, 50) no match", FilterPredicate.eq("id", 100L), "id = 100",
                        0, 50),
                rangedFiltered("filter id>125 + range [0, 250)", FilterPredicate.gt("id", 125L), "id > 125", 0, 250),
                rangedFiltered("filter id<>120 + range [115, 125)", FilterPredicate.notEq("id", 120L), "id <> 120",
                        115, 125));
    }

    static Stream<Arguments> cases() {
        return Stream.of(SINGLE, MULTI)
                .flatMap(fixture -> queries().stream().map(query -> Arguments.of(fixture, query)));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("cases")
    void columnReaders(Fixture fixture, Query query) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture.path()))) {
            ColumnReadersBuilder builder = reader.buildColumnReaders(ColumnProjection.columns("__row__", "score"));
            query.applyTo(builder);
            try (ColumnReaders columns = builder.build()) {
                while (columns.nextBatch()) {
                    long[] values = columns.getColumnReader("__row__").getLongs();
                    double[] scores = columns.getColumnReader("score").getDoubles();
                    for (int i = 0; i < columns.getRecordCount(); i++) {
                        assertThat(scores[i]).as("score of row %d", values[i]).isEqualTo(values[i] % 100 + 0.25);
                        rows.add(values[i]);
                    }
                }
            }
        }
        assertThat(rows).as("%s / %s", fixture, query).isEqualTo(oracle(fixture.path(), query));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("cases")
    void columnReader(Fixture fixture, Query query) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture.path()))) {
            ColumnReaderBuilder builder = reader.buildColumnReader("__row__");
            query.applyTo(builder);
            try (ColumnReader column = builder.build()) {
                while (column.nextBatch()) {
                    long[] values = column.getLongs();
                    for (int i = 0; i < column.getRecordCount(); i++) {
                        rows.add(values[i]);
                    }
                }
            }
        }
        assertThat(rows).as("%s / %s", fixture, query).isEqualTo(oracle(fixture.path(), query));
    }

    private static List<Long> oracle(Path file, Query query) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query.sql(file))) {
            while (rs.next()) {
                rows.add(rs.getLong("__row__"));
            }
        }
        return rows;
    }
}
