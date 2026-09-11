/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Differential **column-order** check: filters on columns whose sort order is not the order of
/// their stored bits or bytes, answered by hardwood and by DuckDB over the same file.
///
/// The corpus `diff_order_*.parquet` holds four such columns, each rising with `__row__` across
/// the point where the two orders part:
///
/// - `u32` (`UINT_32`) and `u64` (`UINT_64`) order by unsigned magnitude and cross 2^31 and 2^63,
///   where the stored bit pattern turns negative.
/// - `h` (`FLOAT16`) is two little-endian bytes, which order nothing like the half they encode.
/// - `dec` (`DECIMAL(9, 2)` as `FIXED_LEN_BYTE_ARRAY`) is big-endian two's complement, whose bytes
///   order every negative value above every positive one.
///
/// The single-row-group, multi-row-group and dictionary-encoded layouts bring the statistics, the
/// page index and the dictionary into play alongside the record-level comparison, so a bound or a
/// dictionary read in the wrong order shows up as a missing or extra row.
///
/// An unsigned literal is the stored bit pattern, the form the accessors return. Binary literals
/// go through the public [FilterPredicate.BinaryColumnPredicate] and
/// [FilterPredicate.BinaryInPredicate] records, the form a filter converted from parquet-java
/// takes.
@Tag("differential")
class DifferentialColumnOrderTest {

    private static final Path RES = Paths.get("src/test/resources/differential");

    /// Width of `dec`: the fewest bytes that hold a precision-9 unscaled value.
    private static final int DECIMAL_WIDTH = 4;

    record Fixture(String name, Path path) {
        @Override public String toString() {
            return name;
        }
    }

    private static final Fixture SINGLE = new Fixture("single-rg", RES.resolve("diff_order_single.parquet"));
    private static final Fixture MULTI = new Fixture("multi-rg", RES.resolve("diff_order_multi.parquet"));
    private static final Fixture DICTIONARY = new Fixture("dictionary", RES.resolve("diff_order_dict.parquet"));

    /// One filter and its SQL counterpart, stated side by side so the two cannot drift apart.
    record Case(String name, FilterPredicate hardwood, String sql) {
        @Override public String toString() {
            return name;
        }
    }

    static List<Case> cases() {
        return List.of(
                // --- UINT_32: unsigned magnitude, crossing 2^31 ---
                new Case("u32 > 3e9", FilterPredicate.gt("u32", u32("3000000000")), "u32 > 3000000000"),
                new Case("u32 < 3e9", FilterPredicate.lt("u32", u32("3000000000")), "u32 < 3000000000"),
                new Case("u32 >= 2^31", FilterPredicate.gtEq("u32", u32("2147483648")), "u32 >= 2147483648"),
                new Case("u32 <= 1e9", FilterPredicate.ltEq("u32", u32("1000000000")), "u32 <= 1000000000"),
                new Case("u32 == row 150", FilterPredicate.eq("u32", u32("3221225400")), "u32 = 3221225400"),
                new Case("u32 != row 150", FilterPredicate.notEq("u32", u32("3221225400")), "u32 <> 3221225400"),
                new Case("u32 in rows 7, 150",
                        FilterPredicate.in("u32", u32("150323852"), u32("3221225400")),
                        "u32 IN (150323852, 3221225400)"),
                new Case("not u32 in rows 7, 150",
                        FilterPredicate.not(FilterPredicate.in("u32", u32("150323852"), u32("3221225400"))),
                        "u32 NOT IN (150323852, 3221225400)"),
                new Case("not u32 > 3e9",
                        FilterPredicate.not(FilterPredicate.gt("u32", u32("3000000000"))),
                        "NOT (u32 > 3000000000)"),

                // --- UINT_64: unsigned magnitude, crossing 2^63 ---
                new Case("u64 > 1e19", FilterPredicate.gt("u64", u64("10000000000000000000")),
                        "u64 > 10000000000000000000"),
                new Case("u64 < 1e19", FilterPredicate.lt("u64", u64("10000000000000000000")),
                        "u64 < 10000000000000000000"),
                new Case("u64 == row 150", FilterPredicate.eq("u64", u64("13835058055282163700")),
                        "u64 = 13835058055282163700"),
                new Case("u64 in rows 7, 150",
                        FilterPredicate.in("u64", u64("645636042579834306"), u64("13835058055282163700")),
                        "u64 IN (645636042579834306, 13835058055282163700)"),
                new Case("not u64 in rows 7, 150",
                        FilterPredicate.not(FilterPredicate.in("u64",
                                u64("645636042579834306"), u64("13835058055282163700"))),
                        "u64 NOT IN (645636042579834306, 13835058055282163700)"),

                // --- FLOAT16: numeric, not the order of its little-endian bytes ---
                new Case("h > 1.5", FilterPredicate.gt("h", 1.5f), "h > 1.5"),
                new Case("h < -2.0", FilterPredicate.lt("h", -2.0f), "h < -2.0"),
                new Case("h == 0.375", FilterPredicate.eq("h", 0.375f), "h = 0.375"),
                new Case("h in -0.5, 1.125", FilterPredicate.in("h", -0.5, 1.125), "h IN (-0.5, 1.125)"),
                // 1.1 has no half representation, so it matches nothing and drops out of not(in).
                new Case("h in 1.1", FilterPredicate.in("h", 1.1), "h IN (1.1)"),
                new Case("not h in 1.1", FilterPredicate.not(FilterPredicate.in("h", 1.1)), "h NOT IN (1.1)"),
                new Case("not h in -0.5, 1.125",
                        FilterPredicate.not(FilterPredicate.in("h", -0.5, 1.125)), "h NOT IN (-0.5, 1.125)"),
                new Case("h < bytes of 1.5", bytes("h", Operator.LT, half(1.5f)), "h < 1.5"),
                new Case("h >= bytes of -2.0", bytes("h", Operator.GT_EQ, half(-2.0f)), "h >= -2.0"),
                new Case("h == bytes of 0.375", bytes("h", Operator.EQ, half(0.375f)), "h = 0.375"),
                new Case("h in bytes of -0.5, 1.125",
                        bytesIn("h", half(-0.5f), half(1.125f)), "h IN (-0.5, 1.125)"),

                // --- DECIMAL as FIXED_LEN_BYTE_ARRAY: signed, crossing zero ---
                new Case("dec > 0", FilterPredicate.gt("dec", new BigDecimal("0")), "dec > 0"),
                new Case("dec < -100", FilterPredicate.lt("dec", new BigDecimal("-100.00")), "dec < -100.00"),
                new Case("dec > bytes of 1.25", bytes("dec", Operator.GT, decimal("1.25")), "dec > 1.25"),
                new Case("dec < bytes of -1.25", bytes("dec", Operator.LT, decimal("-1.25")), "dec < -1.25"),
                new Case("dec in bytes of -1.25, 2.50",
                        bytesIn("dec", decimal("-1.25"), decimal("2.50")), "dec IN (-1.25, 2.50)"),
                new Case("not dec in bytes of -1.25",
                        FilterPredicate.not(bytesIn("dec", decimal("-1.25"))), "dec NOT IN (-1.25)"),
                // A literal of any width stands for its value; the dictionary layout probes exact bytes.
                new Case("dec == bytes of 1.25", bytes("dec", Operator.EQ, decimal("1.25")), "dec = 1.25"),
                new Case("dec == minimal bytes of 1.25", bytes("dec", Operator.EQ, minimalDecimal("1.25")), "dec = 1.25"),
                new Case("dec == minimal bytes of -1.25", bytes("dec", Operator.EQ, minimalDecimal("-1.25")), "dec = -1.25"),
                new Case("dec == wide bytes of 1.25", bytes("dec", Operator.EQ, wideDecimal("1.25")), "dec = 1.25"),
                new Case("dec > minimal bytes of 1.25", bytes("dec", Operator.GT, minimalDecimal("1.25")), "dec > 1.25"),
                new Case("dec in minimal bytes of -1.25, 2.50",
                        bytesIn("dec", minimalDecimal("-1.25"), minimalDecimal("2.50")), "dec IN (-1.25, 2.50)"),
                new Case("not dec in minimal bytes of -1.25",
                        FilterPredicate.not(bytesIn("dec", minimalDecimal("-1.25"))), "dec NOT IN (-1.25)"));
    }

    static Stream<Arguments> fixtureCases() {
        return Stream.of(SINGLE, MULTI, DICTIONARY)
                .flatMap(fixture -> cases().stream().map(c -> Arguments.of(fixture, c)));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("fixtureCases")
    void filterAgreesWithOracle(Fixture fixture, Case c) throws Exception {
        List<Long> expected = oracle(fixture.path(), c);
        List<Long> actual = hardwood(fixture.path(), c);

        assertThat(actual)
                .as("%s / %s", fixture, c)
                .isEqualTo(expected);
    }

    private static List<Long> hardwood(Path file, Case c) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader r = reader.buildRowReader().filter(c.hardwood()).build()) {
            while (r.hasNext()) {
                r.next();
                rows.add(r.getLong("__row__"));
            }
        }
        return rows;
    }

    private static List<Long> oracle(Path file, Case c) throws Exception {
        String sql = "SELECT __row__ FROM read_parquet('" + file.toAbsolutePath() + "') WHERE "
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

    // ==================== Literals ====================

    /// An unsigned `INT32` literal: the stored bit pattern of the value written out in decimal.
    private static int u32(String unsigned) {
        return Integer.parseUnsignedInt(unsigned);
    }

    /// An unsigned `INT64` literal: the stored bit pattern of the value written out in decimal.
    private static long u64(String unsigned) {
        return Long.parseUnsignedLong(unsigned);
    }

    /// The two little-endian bytes a `FLOAT16` column stores for `value`.
    private static byte[] half(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[] { (byte) bits, (byte) (bits >> 8) };
    }

    /// The `DECIMAL_WIDTH` big-endian two's complement bytes `dec` stores for `value`.
    private static byte[] decimal(String value) {
        BigInteger unscaled = new BigDecimal(value).setScale(2).unscaledValue();
        byte[] minimal = unscaled.toByteArray();
        byte[] padded = new byte[DECIMAL_WIDTH];
        Arrays.fill(padded, 0, DECIMAL_WIDTH - minimal.length, unscaled.signum() < 0 ? (byte) 0xFF : 0);
        System.arraycopy(minimal, 0, padded, DECIMAL_WIDTH - minimal.length, minimal.length);
        return padded;
    }

    /// The fewest big-endian two's complement bytes that hold `value`'s unscaled form — narrower
    /// than `dec` for every value in the corpus.
    private static byte[] minimalDecimal(String value) {
        return new BigDecimal(value).setScale(2).unscaledValue().toByteArray();
    }

    /// `value` as `dec` stores it, behind one more sign-extension byte — wider than the column.
    private static byte[] wideDecimal(String value) {
        byte[] padded = decimal(value);
        byte[] wide = new byte[DECIMAL_WIDTH + 1];
        wide[0] = padded[0] < 0 ? (byte) 0xFF : 0;
        System.arraycopy(padded, 0, wide, 1, DECIMAL_WIDTH);
        return wide;
    }

    private static FilterPredicate bytes(String column, Operator op, byte[] value) {
        return new FilterPredicate.BinaryColumnPredicate(column, op, value);
    }

    private static FilterPredicate bytesIn(String column, byte[]... values) {
        return new FilterPredicate.BinaryInPredicate(column, values);
    }
}
