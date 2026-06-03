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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential test: every query is run through hardwood and through DuckDB over the
/// **same** Parquet file, and the two results are required to agree.
///
/// DuckDB is the oracle because its SQL `WHERE` / `LIMIT` / `OFFSET` semantics are exactly the
/// logical model in `_designs/ROW_SELECTION_SEMANTICS.md`. The corpus
/// (`src/test/resources/differential/`) carries a synthetic `__row__` column equal to the
/// physical row position, so file order — which hardwood's `head`/`tail`/`skip` operate in —
/// is recoverable as `ORDER BY __row__`, and the comparison reduces to checking that both
/// sides return the same ordered list of `__row__` values.
///
/// This is P1 (design: `_designs/DIFFERENTIAL_TESTING.md`, tracked by #548): row-identity only,
/// filters on a required column. Per-column value comparison and `byteRange` translation are P2.
/// Combinations that are known-wrong today are marked `pending(<issue>)` and skipped via
/// [Assumptions]; clearing the marker once the bug is fixed turns this into the fix's verifier.
@Tag("differential")
class DifferentialReadTest {

    private static final Path RES = Paths.get("src/test/resources/differential");

    record Fixture(String name, Path path) {
        @Override public String toString() {
            return name;
        }
    }

    private static final Fixture SINGLE = new Fixture("single-rg", RES.resolve("diff_numbers_single.parquet"));
    private static final Fixture MULTI = new Fixture("multi-rg", RES.resolve("diff_numbers_multi.parquet"));

    enum Op {
        GT(">"), GE(">="), LT("<"), LE("<="), EQ("="), NE("<>");

        final String sql;

        Op(String sql) {
            this.sql = sql;
        }
    }

    /// A predicate over the required `long` column `id`. Carries both representations so the
    /// hardwood and SQL sides cannot drift apart.
    record Pred(Op op, long value) {
        FilterPredicate hardwood() {
            return switch (op) {
                case GT -> FilterPredicate.gt("id", value);
                case GE -> FilterPredicate.gtEq("id", value);
                case LT -> FilterPredicate.lt("id", value);
                case LE -> FilterPredicate.ltEq("id", value);
                case EQ -> FilterPredicate.eq("id", value);
                case NE -> FilterPredicate.notEq("id", value);
            };
        }

        String sql() {
            return "id " + op.sql + " " + value;
        }
    }

    /// One row-selection query, expressed once and rendered to both a [RowReaderBuilder]
    /// configuration and an equivalent SQL statement. `null` fields are omitted on both sides.
    record Query(String name, List<String> projection, Pred pred,
                 Integer skip, Integer head, Integer tail, String pending) {

        void applyTo(RowReaderBuilder b) {
            if (projection != null) {
                List<String> cols = new ArrayList<>(projection);
                if (!cols.contains("__row__")) {
                    cols.add("__row__");
                }
                b.projection(ColumnProjection.columns(cols.toArray(new String[0])));
            }
            if (pred != null) {
                b.filter(pred.hardwood());
            }
            if (skip != null) {
                b.skip(skip);
            }
            if (head != null) {
                b.head(head);
            }
            if (tail != null) {
                b.tail(tail);
            }
        }

        String sql(Path file) {
            String from = "read_parquet('" + file.toAbsolutePath() + "')";
            String where = pred != null ? " WHERE " + pred.sql() : "";
            if (tail != null) {
                String inner = "SELECT __row__ FROM " + from + where + " ORDER BY __row__ DESC LIMIT " + tail;
                return "SELECT __row__ FROM (" + inner + ") ORDER BY __row__";
            }
            String limit = head != null ? String.valueOf(head) : "ALL";
            long offset = skip != null ? skip : 0L;
            return "SELECT __row__ FROM " + from + where + " ORDER BY __row__ LIMIT " + limit + " OFFSET " + offset;
        }

        @Override public String toString() {
            return name;
        }
    }

    private static Query builds(String name, List<String> projection, Pred pred,
                                Integer skip, Integer head, Integer tail) {
        return new Query(name, projection, pred, skip, head, tail, null);
    }

    private static Query pending(String issue, String name, Pred pred,
                                 Integer skip, Integer head, Integer tail) {
        return new Query(name, null, pred, skip, head, tail, issue);
    }

    static List<Query> queries() {
        return List.of(
                // --- combinations that should already agree with the oracle ---
                builds("projection", List.of("id", "score"), null, null, null, null),
                builds("filter id>125", null, new Pred(Op.GT, 125), null, null, null),
                builds("filter id<=60", null, new Pred(Op.LE, 60), null, null, null),
                builds("filter id==100", null, new Pred(Op.EQ, 100), null, null, null),
                builds("head 40", null, null, null, 40, null),
                builds("tail 40", null, null, null, null, 40),
                builds("skip 40", null, null, 40, null, null),
                builds("skip 30 + head 40", null, null, 30, 40, null),
                builds("projection + filter id>200", List.of("id"), new Pred(Op.GT, 200), null, null, null),
                builds("projection + head 25", List.of("id"), null, null, 25, null),

                builds("filter + head", null, new Pred(Op.GT, 100), null, 20, null),

                // --- known-wrong today; clearing the marker turns this into the fix's verifier ---
                pending("#541", "filter + skip + head", new Pred(Op.GT, 100), 10, 20, null));
    }

    static Stream<Arguments> cases() {
        return Stream.of(SINGLE, MULTI)
                .flatMap(fixture -> queries().stream().map(query -> Arguments.of(fixture, query)));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("cases")
    void differential(Fixture fixture, Query query) throws Exception {
        if (query.pending() != null) {
            Assumptions.abort("pending " + query.pending() + " — clear the marker once fixed");
        }

        List<Long> expected = oracle(fixture.path(), query);
        List<Long> actual = hardwood(fixture.path(), query);

        assertThat(actual)
                .as("%s / %s", fixture, query)
                .isEqualTo(expected);
    }

    private static List<Long> hardwood(Path file, Query query) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            RowReaderBuilder builder = reader.buildRowReader();
            query.applyTo(builder);
            try (RowReader r = builder.build()) {
                while (r.hasNext()) {
                    r.next();
                    rows.add(r.getLong("__row__"));
                }
            }
        }
        return rows;
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
