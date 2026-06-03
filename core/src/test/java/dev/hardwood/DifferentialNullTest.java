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
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential **null-semantics** check (P2): pins that a comparison filter on a *nullable*
/// column drops the null rows, matching SQL three-valued logic.
///
/// The corpus `diff_nulls.parquet` has `val` null on every third row. DuckDB's `WHERE val <op> x`
/// excludes nulls (3VL); this asserts hardwood's `FilterPredicate` on the same column yields the
/// exact same matched rows. A divergence here is a real semantic finding (a filter that lets
/// nulls through, or rejects the combination), not noise.
@Tag("differential")
class DifferentialNullTest {

    private static final Path FILE = Paths.get("src/test/resources/differential/diff_nulls.parquet");

    enum Op {
        GT(">"), GE(">="), LT("<"), LE("<="), EQ("="), NE("<>");

        final String sql;

        Op(String sql) {
            this.sql = sql;
        }
    }

    record Case(String name, Op op, long value) {
        FilterPredicate hardwood() {
            return switch (op) {
                case GT -> FilterPredicate.gt("val", value);
                case GE -> FilterPredicate.gtEq("val", value);
                case LT -> FilterPredicate.lt("val", value);
                case LE -> FilterPredicate.ltEq("val", value);
                case EQ -> FilterPredicate.eq("val", value);
                case NE -> FilterPredicate.notEq("val", value);
            };
        }

        String sql() {
            return "val " + op.sql + " " + value;
        }

        @Override public String toString() {
            return name;
        }
    }

    static Stream<Case> cases() {
        return Stream.of(
                new Case("val > 100", Op.GT, 100),
                new Case("val >= 200", Op.GE, 200),
                new Case("val < 50", Op.LT, 50),
                new Case("val <= 40", Op.LE, 40),
                new Case("val == 62", Op.EQ, 62),
                new Case("val != 100", Op.NE, 100));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void filterDropsNullsLikeSql(Case c) throws Exception {
        List<Long> expected = oracle(c);
        List<Long> actual = hardwood(c);
        assertThat(actual).as("%s — must exclude the null rows", c).isEqualTo(expected);
    }

    private static List<Long> hardwood(Case c) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader r = reader.buildRowReader().filter(c.hardwood()).build()) {
            while (r.hasNext()) {
                r.next();
                rows.add(r.getLong("__row__"));
            }
        }
        return rows;
    }

    private static List<Long> oracle(Case c) throws Exception {
        String sql = "SELECT __row__ FROM read_parquet('" + FILE.toAbsolutePath() + "') WHERE "
                + c.sql() + " ORDER BY __row__";
        List<Long> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(rs.getLong("__row__"));
            }
        }
        return rows;
    }
}
