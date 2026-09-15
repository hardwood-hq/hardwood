/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.tools.predicateaudit.Oracle.In;
import dev.hardwood.tools.predicateaudit.Oracle.IsNull;
import dev.hardwood.tools.predicateaudit.Oracle.Leaf;
import dev.hardwood.tools.predicateaudit.Oracle.Not;
import dev.hardwood.tools.predicateaudit.Oracle.Op;
import dev.hardwood.tools.predicateaudit.Oracle.P;
import dev.hardwood.tools.predicateaudit.Oracle.Rows;

/// Puts predicates each engine can express to Hardwood, parquet-java and DuckDB over the same files.
///
/// A predicate has a Hardwood form, which the oracle also evaluates, and where the engine has a
/// spelling for it a parquet-java `filter2` filter and a DuckDB `WHERE` clause. The report lists
/// every engine's rows next to the rule's, so a difference shows with its data.
final class EngineComparison {

    /// One predicate: an id, the fixture group it reads, and its form in each engine (`null` where
    /// an engine has none).
    record Entry(String id, String group, P predicate, Supplier<FilterCompat.Filter> parquetJava, String duckDb) {
    }

    record Result(int predicates, int differing) {
    }

    private EngineComparison() {
    }

    static Result run(Path fixtures, Path report) throws Exception {
        Map<String, Rows> groups = Map.of("flat", Matrix.flatRows(Columns.flat()), "legacy", Matrix.flatRows(Columns.legacy()),
                "nested", Matrix.nestedRows());
        int differing = 0;
        List<Entry> entries = entries();
        try (Connection duckDb = DriverManager.getConnection("jdbc:duckdb:");
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(report.resolve("engines.tsv")))) {
            try (Statement statement = duckDb.createStatement()) {
                statement.execute("SET TimeZone='UTC'");
            }
            out.println("id\tpredicate\trule\thardwood\tparquet-java\tduckdb\tdiffers");
            for (Entry entry : entries) {
                Rows rows = groups.get(entry.group());
                String rule = Oracle.refusal(rows, entry.predicate()) != null ? "REFUSE" : expected(rows, entry.predicate());
                String hardwood = perLayout(fixtures, entry.group(), layout -> hardwood(layout, entry));
                String parquetJava = entry.parquetJava() == null ? "n/a" : perLayout(fixtures, entry.group(), layout -> parquetJava(layout, entry));
                String duck = entry.duckDb() == null ? "n/a"
                        : perLayout(fixtures, entry.group(), layout -> duckDb(duckDb, layout, entry.duckDb()));
                boolean differs = !(hardwood.equals(rule) || rule.equals("REFUSE") && hardwood.startsWith("THROW"))
                        || !(parquetJava.equals("n/a") || parquetJava.equals(rule))
                        || !(duck.equals("n/a") || duck.equals(rule));
                if (differs) {
                    differing++;
                }
                out.println(entry.id() + "\t" + Oracle.show(entry.predicate()) + "\t" + rule + "\t" + hardwood + "\t" + parquetJava
                        + "\t" + duck + "\t" + (differs ? "yes" : ""));
            }
            Result probes = nanAndNanosecondProbe(fixtures.resolve("pyarrow_nan.parquet"), duckDb, out);
            return new Result(entries.size() + probes.predicates(), differing + probes.differing());
        }
    }

    @FunctionalInterface
    private interface LayoutQuery {
        String rows(Path file) throws Exception;
    }

    /// The answer over each layout of the group, collapsed to one where every layout agrees.
    private static String perLayout(Path fixtures, String group, LayoutQuery query) {
        Map<String, String> answers = new LinkedHashMap<>();
        for (String layout : group.equals("nested") ? List.of("single", "multi", "dict") : List.of("single", "multi", "dict", "bloom")) {
            answers.put(layout, answer(query, fixtures.resolve(group + "_" + layout + ".parquet")));
        }
        if (answers.values().stream().distinct().count() == 1) {
            return answers.values().iterator().next();
        }
        StringBuilder rendered = new StringBuilder();
        answers.forEach((layout, answer) -> rendered.append(layout).append(": ").append(answer).append(" / "));
        return rendered.toString();
    }

    /// The rows `query` returns for `file`, or the first line of the exception it throws.
    private static String answer(LayoutQuery query, Path file) {
        try {
            return query.rows(file);
        }
        catch (Exception e) {
            return "THROW " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).lines().findFirst().orElse("");
        }
    }

    private static String expected(Rows rows, P predicate) {
        List<Long> matching = new ArrayList<>();
        for (int row = 0; row < Columns.ROWS; row++) {
            if (Oracle.eval(rows, predicate, row) == Oracle.Tri.T) {
                matching.add((long) row);
            }
        }
        return ReadPath.show(matching);
    }

    private static String hardwood(Path file, Entry entry) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return ReadPath.ROW_READER.read(reader, Predicates.of(entry.predicate()));
        }
    }

    private static String parquetJava(Path file, Entry entry) throws Exception {
        return ReadPath.show(ParquetJavaReader.rows(file.toUri(), entry.parquetJava().get()));
    }

    private static String duckDb(Connection connection, Path file, String where) throws Exception {
        List<Long> rows = new ArrayList<>();
        String query = "SELECT __row__ FROM read_parquet('" + file.toAbsolutePath() + "') WHERE " + where + " ORDER BY __row__";
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(query)) {
            while (result.next()) {
                rows.add(result.getLong(1));
            }
        }
        return ReadPath.show(rows);
    }

    // ==================== The predicates ====================

    private static Binary binary(byte[] bytes) {
        return Binary.fromConstantByteArray(bytes);
    }

    private static Supplier<FilterCompat.Filter> filter(Supplier<FilterPredicate> predicate) {
        return () -> FilterCompat.get(predicate.get());
    }

    private static String timestamp(long epochNanos) {
        return LocalDateTime.ofInstant(Instant.EPOCH.plusNanos(epochNanos), ZoneOffset.UTC).toString().replace('T', ' ');
    }

    private static String blob(byte[] bytes) {
        StringBuilder literal = new StringBuilder("'");
        for (byte b : bytes) {
            literal.append(String.format("\\x%02X", b));
        }
        return literal.append("'::BLOB").toString();
    }

    static List<Entry> entries() {
        List<Entry> e = new ArrayList<>();
        int u32AtProbe = (Integer) Columns.flat().stream().filter(c -> c.name().equals("u32")).findFirst().orElseThrow()
                .at(Columns.PROBE_ROW);
        e.add(new Entry("int.gt", "flat", new Leaf("i32", Op.GT, 20), filter(() -> FilterApi.gt(FilterApi.intColumn("i32"), 20)), "i32 > 20"));
        e.add(new Entry("int.noteq-nulls", "flat", new Leaf("i32", Op.NE, 20),
                filter(() -> FilterApi.notEq(FilterApi.intColumn("i32"), 20)), "i32 <> 20"));
        e.add(new Entry("int.notin-one", "flat", new Not(new In("i32", List.of(20))),
                filter(() -> FilterApi.notIn(FilterApi.intColumn("i32"), new HashSet<>(List.of(20)))), "i32 NOT IN (20)"));
        e.add(new Entry("int.notin-two", "flat", new Not(new In("i32", List.of(20, 22))),
                filter(() -> FilterApi.notIn(FilterApi.intColumn("i32"), new HashSet<>(List.of(20, 22)))), "i32 NOT IN (20, 22)"));
        e.add(new Entry("int8.past-annotation", "flat", new Leaf("i8", Op.EQ, 1000),
                filter(() -> FilterApi.eq(FilterApi.intColumn("i8"), 1000)), "i8 = 1000"));
        e.add(new Entry("uint32.lt-minus-one", "flat", new Leaf("u32", Op.LT, -1),
                filter(() -> FilterApi.lt(FilterApi.intColumn("u32"), -1)), "u32 < -1"));
        e.add(new Entry("uint32.eq-above-2^31", "flat", new Leaf("u32", Op.EQ, u32AtProbe),
                filter(() -> FilterApi.eq(FilterApi.intColumn("u32"), u32AtProbe)), "u32 = " + Integer.toUnsignedLong(u32AtProbe)));
        e.add(new Entry("uint64.eq-2^63", "flat", new Leaf("u64", Op.EQ, Long.MIN_VALUE),
                filter(() -> FilterApi.eq(FilterApi.longColumn("u64"), Long.MIN_VALUE)), "u64 = 9223372036854775808"));
        e.add(new Entry("float.gt", "flat", new Leaf("f32", Op.GT, 10.0f),
                filter(() -> FilterApi.gt(FilterApi.floatColumn("f32"), 10.0f)), "f32 > 10.0"));
        e.add(new Entry("float.eq-nan", "flat", new Leaf("f32", Op.EQ, Float.NaN),
                filter(() -> FilterApi.eq(FilterApi.floatColumn("f32"), Float.NaN)), "f32 = 'NaN'::FLOAT"));
        e.add(new Entry("float.eq-negative-zero", "flat", new Leaf("f32", Op.EQ, -0.0f),
                filter(() -> FilterApi.eq(FilterApi.floatColumn("f32"), -0.0f)), "f32 = '-0.0'::FLOAT"));
        e.add(new Entry("float.lt-zero", "flat", new Leaf("f32", Op.LT, 0.0f),
                filter(() -> FilterApi.lt(FilterApi.floatColumn("f32"), 0.0f)), "f32 < 0.0"));
        e.add(new Entry("double.gteq-nan", "flat", new Leaf("f64", Op.GE, Double.NaN),
                filter(() -> FilterApi.gtEq(FilterApi.doubleColumn("f64"), Double.NaN)), "f64 >= 'NaN'::DOUBLE"));
        e.add(new Entry("float16.gt", "flat", new Leaf("f16", Op.GT, 1.0f),
                filter(() -> FilterApi.gt(FilterApi.binaryColumn("f16"), binary(new byte[] { 0x00, 0x3C }))), "f16 > 1.0"));
        e.add(new Entry("float16.eq-nan-bytes", "flat", new Leaf("f16", Op.EQ, new byte[] { 0x00, 0x7E }),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("f16"), binary(new byte[] { 0x00, 0x7E }))), null));
        e.add(new Entry("bool.lt-true", "flat", new Leaf("bool", Op.LT, true), null, "bool < true"));
        e.add(new Entry("date.gt", "flat", new Leaf("date", Op.GT, LocalDate.ofEpochDay(19020)),
                filter(() -> FilterApi.gt(FilterApi.intColumn("date"), 19020)), "date > DATE '" + LocalDate.ofEpochDay(19020) + "'"));
        e.add(new Entry("time_ms.eq", "flat", new Leaf("time_ms", Op.EQ, LocalTime.of(6, 10)),
                filter(() -> FilterApi.eq(FilterApi.intColumn("time_ms"), 22_200_000)), "time_ms = TIME '06:10:00'"));
        long micros = Columns.BASE_MS * 1000 + 10 * 3_600_000_000L + 310;
        e.add(new Entry("ts_us_utc.gt", "flat", new Leaf("ts_us_utc", Op.GT, Instant.EPOCH.plusNanos(micros * 1000)),
                filter(() -> FilterApi.gt(FilterApi.longColumn("ts_us_utc"), micros)),
                "ts_us_utc > TIMESTAMPTZ '" + timestamp(micros * 1000) + "+00'"));
        e.add(new Entry("ts_us_local.eq", "flat",
                new Leaf("ts_us_local", Op.EQ, LocalDateTime.ofInstant(Instant.EPOCH.plusNanos(micros * 1000), ZoneOffset.UTC)),
                filter(() -> FilterApi.eq(FilterApi.longColumn("ts_us_local"), micros)),
                "ts_us_local = TIMESTAMP '" + timestamp(micros * 1000) + "'"));
        Instant nonCanonical = Instant.EPOCH.plusNanos(Columns.int96Nanos(6));
        e.add(new Entry("int96.eq-instant", "flat", new Leaf("ts96", Op.EQ, nonCanonical), null,
                "ts96 = TIMESTAMP '" + timestamp(Columns.int96Nanos(6)) + "'"));
        e.add(new Entry("int96.eq-bytes", "flat", new Leaf("ts96", Op.EQ, Columns.int96(Columns.int96Nanos(6), true)),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("ts96"), binary(Columns.int96(Columns.int96Nanos(6), true)))), null));
        e.add(new Entry("int96.gt", "flat", new Leaf("ts96", Op.GT, Instant.EPOCH.plusNanos(Columns.int96Nanos(Columns.PROBE_ROW))), null,
                "ts96 > TIMESTAMP '" + timestamp(Columns.int96Nanos(Columns.PROBE_ROW)) + "'"));
        e.add(new Entry("dec_i32.eq-int", "flat", new Leaf("dec_i32", Op.EQ, 21),
                filter(() -> FilterApi.eq(FilterApi.intColumn("dec_i32"), 21)), "dec_i32 = 21"));
        e.add(new Entry("dec_i32.eq-past-scale", "flat", new Leaf("dec_i32", Op.EQ, new BigDecimal("0.215")), null, "dec_i32 = 0.215"));
        e.add(new Entry("dec_i32.lt-past-scale", "flat", new Leaf("dec_i32", Op.LT, new BigDecimal("0.215")), null, "dec_i32 < 0.215"));
        e.add(new Entry("dec_flba.eq", "flat", new Leaf("dec_flba", Op.EQ, new BigDecimal("0.20")),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("dec_flba"), binary(Columns.fixedDecimal(BigInteger.valueOf(20), 9)))),
                "dec_flba = 0.20"));
        e.add(new Entry("dec_flba.eq-one-byte", "flat", new Leaf("dec_flba", Op.EQ, new byte[] { 0x14 }),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("dec_flba"), binary(new byte[] { 0x14 }))), null));
        e.add(new Entry("dec_ba.eq-minimal-bytes-of-padded", "flat", new Leaf("dec_ba", Op.EQ, new byte[] { 0x02 }),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("dec_ba"), binary(new byte[] { 0x02 }))), null));
        e.add(new Entry("dec_ba.eq-value", "flat", new Leaf("dec_ba", Op.EQ, new BigDecimal("0.002")), null, "dec_ba = 0.002"));
        e.add(new Entry("dec_ba.eq-zero", "flat", new Leaf("dec_ba", Op.EQ, BigDecimal.ZERO),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("dec_ba"), binary(new byte[] { 0x00 }))), "dec_ba = 0"));
        e.add(new Entry("str.lt-fullwidth", "flat", new Leaf("str", Op.LT, "～"),
                filter(() -> FilterApi.lt(FilterApi.binaryColumn("str"), Binary.fromString("～"))), "str < '～'"));
        e.add(new Entry("json.eq", "flat", new Leaf("json", Op.EQ, "{\"a\":310}"),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("json"), Binary.fromString("{\"a\":310}"))), "json::VARCHAR = '{\"a\":310}'"));
        e.add(new Entry("ba.gt", "flat", new Leaf("ba", Op.GT, new byte[] { 0x79, 0x18 }),
                filter(() -> FilterApi.gt(FilterApi.binaryColumn("ba"), binary(new byte[] { 0x79, 0x18 }))),
                "ba > " + blob(new byte[] { 0x79, 0x18 })));
        byte[] uuidAtProbe = Columns.uuidAt(Columns.PROBE_ROW);
        ByteBuffer uuidBuffer = ByteBuffer.wrap(uuidAtProbe);
        UUID uuid = new UUID(uuidBuffer.getLong(), uuidBuffer.getLong());
        UUID high = new UUID(0x8000000000000000L, 0);
        e.add(new Entry("uuid.eq", "flat", new Leaf("uuid", Op.EQ, uuid),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("uuid"), binary(uuidAtProbe))), "uuid = '" + uuid + "'"));
        e.add(new Entry("uuid.lt", "flat", new Leaf("uuid", Op.LT, high),
                filter(() -> FilterApi.lt(FilterApi.binaryColumn("uuid"), binary(Oracle.uuidBytes(high)))), "uuid < '" + high + "'"));
        e.add(new Entry("interval.eq", "legacy", new Leaf("itv", Op.EQ, new PqInterval(310, 0, 310_000)),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("itv"), binary(Columns.intervalAt(310)))),
                "itv = INTERVAL '310 months 310 seconds'"));
        e.add(new Entry("interval.eq-normalised", "legacy", new Leaf("itv", Op.EQ, new PqInterval(0, 9300, 310_000)), null,
                "itv = INTERVAL '9300 days 310 seconds'"));
        e.add(new Entry("interval.parquet-java-encoding", "flat", new Leaf("itv", Op.EQ, new PqInterval(310, 0, 310_000)),
                filter(() -> FilterApi.eq(FilterApi.binaryColumn("itv"), binary(Columns.intervalAt(310)))),
                "itv = INTERVAL '310 months 310 seconds'"));
        e.add(new Entry("nested.isnull-leaf", "nested", new IsNull("s.x"),
                filter(() -> FilterApi.eq(FilterApi.intColumn("s.x"), (Integer) null)), "s.x IS NULL"));
        e.add(new Entry("nested.gt", "nested", new Leaf("s.x", Op.GT, -601),
                filter(() -> FilterApi.gt(FilterApi.intColumn("s.x"), -601)), "s.x > -601"));
        e.add(new Entry("nested.not-gt", "nested", new Not(new Leaf("s.x", Op.GT, 20)),
                filter(() -> FilterApi.not(FilterApi.gt(FilterApi.intColumn("s.x"), 20))), "NOT (s.x > 20)"));
        return e;
    }

    /// A PyArrow file whose float bounds exclude `NaN`, with signed zeros across row groups and a
    /// `TIMESTAMP(NANOS)` column: the differences that need a writer other than parquet-java.
    private static Result nanAndNanosecondProbe(Path file, Connection duckDb, PrintWriter out) {
        long base = 1_700_000_000_000_000_000L;
        List<Entry> probes = List.of(
                new Entry("pyarrow.eq-nan", "pyarrow", new Leaf("d", Op.EQ, Double.NaN),
                        filter(() -> FilterApi.eq(FilterApi.doubleColumn("d"), Double.NaN)), "d = 'NaN'::DOUBLE"),
                new Entry("pyarrow.gt-past-bounds", "pyarrow", new Leaf("d", Op.GT, 5.0),
                        filter(() -> FilterApi.gt(FilterApi.doubleColumn("d"), 5.0)), "d > 5.0"),
                new Entry("pyarrow.eq-zero", "pyarrow", new Leaf("z", Op.EQ, 0.0),
                        filter(() -> FilterApi.eq(FilterApi.doubleColumn("z"), 0.0)), "z = 0.0"),
                new Entry("pyarrow.lt-zero", "pyarrow", new Leaf("z", Op.LT, 0.0),
                        filter(() -> FilterApi.lt(FilterApi.doubleColumn("z"), 0.0)), "z < 0.0"),
                new Entry("pyarrow.eq-nanosecond", "pyarrow", new Leaf("tsn", Op.EQ, Instant.EPOCH.plusNanos(base + 4)),
                        filter(() -> FilterApi.eq(FilterApi.longColumn("tsn"), base + 4)),
                        "tsn = '2023-11-14 22:13:20.000000004+00'::TIMESTAMPTZ"),
                new Entry("pyarrow.gt-nanosecond", "pyarrow", new Leaf("tsn", Op.GT, Instant.EPOCH.plusNanos(base + 4)),
                        filter(() -> FilterApi.gt(FilterApi.longColumn("tsn"), base + 4)),
                        "tsn > '2023-11-14 22:13:20.000000004+00'::TIMESTAMPTZ"));
        int differing = 0;
        for (Entry entry : probes) {
            String hardwood = answer(probed -> hardwood(probed, entry), file);
            String parquetJava = answer(probed -> parquetJava(probed, entry), file);
            String duck = answer(probed -> duckDb(duckDb, probed, entry.duckDb()), file);
            boolean differs = !(hardwood.equals(parquetJava) && hardwood.equals(duck));
            if (differs) {
                differing++;
            }
            out.println(entry.id() + "\t" + Oracle.show(entry.predicate()) + "\t(see pyarrow_nan in derive_fixtures.py)\t" + hardwood
                    + "\t" + parquetJava + "\t" + duck + "\t" + (differs ? "yes" : ""));
        }
        return new Result(probes.size(), differing);
    }
}
