/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/// Every predicate of `_designs/PREDICATE_LITERALS.md`, answered through every read path and
/// checked against a Java oracle of the rule.
///
/// The oracle reads the filtered column unfiltered and applies the rule to each value: a
/// predicate matches the rows whose value it denotes, in the order the column's type defines, and
/// never a null row. Disagreement is therefore always the reader's, and it shows up as the row
/// numbers that differ rather than as a count.
///
/// **Read paths.** A predicate takes a different route through the reader depending on how it is
/// built and configured, and a literal resolved wrongly can show on one route only:
/// - the `RowReader` by default, which compiles the filter into batch matchers where it can
/// - the `RowReader` forced onto its record-level path, by `or`-ing in a binary comparison that
///   never matches and that no batch matcher takes
/// - the `RowReader` with `hardwood.metadata-filtering=false`, which decodes every row group and
///   page and answers from values alone
/// - the `ColumnReader`, with and without metadata filtering
///
/// **Layouts.** `predicate_single`, `predicate_multi` and `predicate_dict` hold the same 400 rows
/// in one row group, in four, and dictionary-encoded, so the row-group bounds, the page index and
/// the dictionary each decide a predicate that the record-level comparison decides again.
/// `predicate_nested` adds a struct whose leaf is null under a present struct, and a leaf below a
/// repeated path. `predicate_opaque` carries the `BSON` and `INTERVAL` columns, which DuckDB
/// cannot open and which therefore stay out of the corpus the differential tests read.
class PredicatePathAgreementTest {

    private static final Path RES = Paths.get("src/test/resources/predicate");

    /// A comparison no batch matcher takes, on a required column whose every value is `z`, so it
    /// matches no row and pushes the filter it is `or`-ed into onto the record-level path.
    private static final FilterPredicate NEVER = FilterPredicate.lt("zz", new byte[0]);

    private static final ReaderConfig METADATA_FILTERING_OFF = ReaderConfig.builder()
            .option("hardwood.metadata-filtering", "false")
            .build();

    private static HardwoodContext context;

    /// Values of one column of one file, read unfiltered: the logical value an accessor returns
    /// and the physical one, indexed by row.
    private static final Map<String, List<Object[]>> VALUES = new HashMap<>();

    /// The schema of each layout, read once: every case of every path asks it for the column it
    /// filters.
    private static final Map<String, FileSchema> SCHEMAS = new HashMap<>();

    @BeforeAll
    static void openContext() {
        context = HardwoodContext.create();
    }

    @AfterAll
    static void closeContext() {
        context.close();
        VALUES.clear();
        SCHEMAS.clear();
    }

    // ==================== Layouts ====================

    record Layout(String name, Path path) {
        @Override public String toString() {
            return name;
        }
    }

    private static final Layout SINGLE = new Layout("single-rg", RES.resolve("predicate_single.parquet"));
    private static final Layout MULTI = new Layout("multi-rg", RES.resolve("predicate_multi.parquet"));
    private static final Layout DICTIONARY = new Layout("dictionary", RES.resolve("predicate_dict.parquet"));
    private static final Layout NESTED = new Layout("nested", RES.resolve("predicate_nested_single.parquet"));
    private static final Layout NESTED_MULTI = new Layout("nested-multi-rg",
            RES.resolve("predicate_nested_multi.parquet"));
    private static final Layout OPAQUE = new Layout("opaque", RES.resolve("predicate_opaque_single.parquet"));
    private static final Layout OPAQUE_DICTIONARY = new Layout("opaque-dictionary",
            RES.resolve("predicate_opaque_dict.parquet"));

    private static final List<Layout> LAYOUTS =
            List.of(SINGLE, MULTI, DICTIONARY, NESTED, NESTED_MULTI, OPAQUE, OPAQUE_DICTIONARY);

    // ==================== Read paths ====================

    enum ReadPath {
        ROW_READER(false, true, false),
        ROW_READER_RECORD_PATH(false, true, true),
        ROW_READER_NO_METADATA(false, false, false),
        COLUMN_READER(true, true, false),
        COLUMN_READER_NO_METADATA(true, false, false);

        private final boolean columnReader;
        private final boolean metadataFiltering;
        private final boolean forceRecordPath;

        ReadPath(boolean columnReader, boolean metadataFiltering, boolean forceRecordPath) {
            this.columnReader = columnReader;
            this.metadataFiltering = metadataFiltering;
            this.forceRecordPath = forceRecordPath;
        }
    }

    // ==================== The rule ====================

    /// What the rule says a predicate answers.
    sealed interface Expect {
    }

    /// The predicate matches the rows whose value `test` accepts. A null value never matches.
    record Matching(ValueTest test) implements Expect {
    }

    /// The predicate matches exactly the rows where the named node is null.
    record MatchingNulls() implements Expect {
    }

    /// The rule does not admit the predicate: building the reader throws with this message.
    record Rejected(String message) implements Expect {
    }

    @FunctionalInterface
    interface ValueTest {
        /// @param column the column filtered, which fixes the order values compare in
        /// @param logical the value the column's logical accessor returns for the row
        /// @param physical the value its physical accessor returns
        boolean matches(ColumnSchema column, Object logical, Object physical);
    }

    record Case(String column, String name, FilterPredicate predicate, Expect expect) {
        String id() {
            return column + ": " + name;
        }

        @Override public String toString() {
            return id();
        }
    }

    /// Cells the reader does not yet answer by the rule, each naming the issue that removes it.
    ///
    /// An excluded case is still run, and is asserted to *disagree*: the entry has to go in the
    /// same change that makes the case pass, so the list cannot outlive the defect it records.
    /// A [Rejected] case disagrees by being answered rather than refused, which is how a
    /// predicate the rule does not admit but a factory still builds is recorded.
    private static final Map<String, String> EXCLUDED = Map.of(
            "bool: lt(true)", "#1183",
            "bool: ltEq(false)", "#1183",
            "bool: gt(false)", "#1183",
            "bool: gtEq(true)", "#1183",
            "bool: not(gt(false))", "#1183",
            "f16: in(0.1), a double on a FLOAT16 column", "#1195");

    // ==================== Cases ====================

    static List<Case> cases() {
        List<Case> cases = new ArrayList<>();

        // --- BOOLEAN: false before true ---
        cases.add(new Case("bool", "eq(true)", FilterPredicate.eq("bool", true), matching(eq(true))));
        cases.add(new Case("bool", "notEq(true)", FilterPredicate.notEq("bool", true), matching(notEq(true))));
        cases.add(new Case("bool", "lt(true)", new FilterPredicate.BooleanColumnPredicate("bool", Operator.LT, true), matching(cmp(Operator.LT, true))));
        cases.add(new Case("bool", "ltEq(false)", new FilterPredicate.BooleanColumnPredicate("bool", Operator.LT_EQ, false),
                matching(cmp(Operator.LT_EQ, false))));
        cases.add(new Case("bool", "gt(false)", new FilterPredicate.BooleanColumnPredicate("bool", Operator.GT, false), matching(cmp(Operator.GT, false))));
        cases.add(new Case("bool", "gtEq(true)", new FilterPredicate.BooleanColumnPredicate("bool", Operator.GT_EQ, true),
                matching(cmp(Operator.GT_EQ, true))));
        cases.add(new Case("bool", "not(gt(false))", FilterPredicate.not(new FilterPredicate.BooleanColumnPredicate("bool", Operator.GT, false)),
                matching(cmp(Operator.LT_EQ, false))));
        cases.add(new Case("bool", "isNull", FilterPredicate.isNull("bool"), new MatchingNulls()));
        cases.add(new Case("bool", "isNotNull", FilterPredicate.isNotNull("bool"), matching(everyNonNullRow())));

        // --- INT32 / INT64, signed and unsigned ---
        intCases(cases, "i32", 0);
        intCases(cases, "i8", -20);
        intCases(cases, "u8", 128);
        intCases(cases, "u32", Integer.MIN_VALUE + 100_000_000);
        longCases(cases, "i64", 0L);
        longCases(cases, "u64", Long.MIN_VALUE + 100_000_000_000_000L);

        // An INT(8) holds -128..127, but the accessors return what the file stores, so a literal
        // past the annotation is compared as given rather than refused.
        cases.add(new Case("i8", "eq(1000), past the annotation",
                FilterPredicate.eq("i8", 1000), matching(eq(1000))));
        cases.add(new Case("i8", "lt(1000), past the annotation",
                FilterPredicate.lt("i8", 1000), matching(cmp(Operator.LT, 1000))));

        // --- FLOAT, DOUBLE, FLOAT16 ---
        floatCases(cases, "f32", 12.5f);
        doubleCases(cases, "f64", 12.5);
        floatCases(cases, "f16", 6.25f);
        cases.add(new Case("f32", "eq(NaN)", FilterPredicate.eq("f32", Float.NaN), matching(eq(Float.NaN))));
        cases.add(new Case("f32", "gt(NaN)", FilterPredicate.gt("f32", Float.NaN),
                matching(cmp(Operator.GT, Float.NaN))));
        cases.add(new Case("f32", "lt(+inf)", FilterPredicate.lt("f32", Float.POSITIVE_INFINITY),
                matching(cmp(Operator.LT, Float.POSITIVE_INFINITY))));
        cases.add(new Case("f32", "eq(-0.0)", FilterPredicate.eq("f32", -0.0f), matching(eq(-0.0f))));
        cases.add(new Case("f64", "eq(NaN)", FilterPredicate.eq("f64", Double.NaN), matching(eq(Double.NaN))));
        cases.add(new Case("f64", "eq(-0.0)", FilterPredicate.eq("f64", -0.0), matching(eq(-0.0))));
        cases.add(new Case("f16", "eq(bytes of 6.25)",
                binary("f16", Operator.EQ, half(6.25f)), matching(eqPhysical(half(6.25f)))));
        cases.add(new Case("f16", "lt(bytes of 6.25)",
                binary("f16", Operator.LT, half(6.25f)), matching(cmpPhysical(Operator.LT, half(6.25f)))));

        // --- DATE, TIME, TIMESTAMP ---
        cases.add(new Case("date", "eq(row 200)", FilterPredicate.eq("date", LocalDate.ofEpochDay(19000)),
                matching(eq(LocalDate.ofEpochDay(19000)))));
        cases.add(new Case("date", "lt(row 200)", FilterPredicate.lt("date", LocalDate.ofEpochDay(19000)),
                matching(cmp(Operator.LT, LocalDate.ofEpochDay(19000)))));
        cases.add(new Case("date", "eq(epoch day 19000 as int)", FilterPredicate.eq("date", 19000),
                matching(eqPhysical(19000))));

        timeCases(cases, "time_ms", LocalTime.ofNanoOfDay(3_600_000_000_000L + 200 * 60_000_000_000L));
        timeCases(cases, "time_us", LocalTime.ofNanoOfDay(200 * 60_000_000_000L + 200_000L));
        timeCases(cases, "time_ns", LocalTime.ofNanoOfDay(200 * 60_000_000_000L + 200L));
        instantCases(cases, "ts_ms_utc", Instant.ofEpochMilli(1_700_000_000_000L));
        instantCases(cases, "ts_us_utc", Instant.ofEpochSecond(1_700_000_000L, 200_000L));
        instantCases(cases, "ts_ns_utc", Instant.ofEpochSecond(1_700_000_000L, 200L));

        // --- DECIMAL over INT32, INT64 and FIXED_LEN_BYTE_ARRAY ---
        decimalCases(cases, "dec_i32", new BigDecimal("0.00"));
        decimalCases(cases, "dec_i64", new BigDecimal("0.00"));
        decimalCases(cases, "dec_flba", new BigDecimal("0.00"));
        cases.add(new Case("dec_i32", "eq(unscaled 125 as int)", FilterPredicate.eq("dec_i32", 125),
                matching(eqPhysical(125))));

        // --- Text and binary ---
        stringCases(cases, "str", "k0200");
        stringCases(cases, "json", "{\"k\":\"k0200\"}");
        stringCases(cases, "enum", "E3");
        binaryCases(cases, "ba", new byte[] { 0, (byte) 200 });
        binaryCases(cases, "flba5", new byte[] { 100, 0, 0, 0, (byte) 200 });
        binaryCases(cases, "bson", new byte[] { 0, (byte) 200, 0 });
        intervalCases(cases, "iv", interval(200 % 13, 200 % 29, 200 * 1000));
        cases.add(new Case("str", "eq(emoji at row 399)", FilterPredicate.eq("str", "😀"),
                matching(eq("😀"))));
        cases.add(new Case("str", "gt(full-width tilde at row 398)", FilterPredicate.gt("str", "～"),
                matching(cmp(Operator.GT, "～"))));

        // --- UUID ---
        UUID uuidAt200 = uuidOfRow(200);
        cases.add(new Case("uuid", "eq(row 200)", FilterPredicate.eq("uuid", uuidAt200), matching(eq(uuidAt200))));
        cases.add(new Case("uuid", "gt(row 200)", FilterPredicate.gt("uuid", uuidAt200),
                matching(cmp(Operator.GT, uuidAt200))));

        // --- Null tests, and a group that is absent on some rows ---
        cases.add(new Case("str", "isNull", FilterPredicate.isNull("str"), new MatchingNulls()));
        cases.add(new Case("s.x", "isNull", FilterPredicate.isNull("s.x"), new MatchingNulls()));
        cases.add(new Case("s.x", "isNotNull", FilterPredicate.isNotNull("s.x"), matching(everyNonNullRow())));
        intCases(cases, "s.x", 0);
        stringCases(cases, "s.name", "n0200");
        decimalCases(cases, "s.dec", new BigDecimal("0.00"));

        // --- Rejected by the rule ---
        cases.add(new Case("l.list.element", "eq(1) below a repeated path",
                FilterPredicate.eq("l.list.element", 1),
                new Rejected("Filter predicates do not support repeated columns. "
                        + "Column 'l.list.element' is repeated.")));
        cases.add(new Case("s", "eq(1) on a group", FilterPredicate.eq("s", 1),
                new Rejected("Filter predicates require a leaf column. Column 's' is a group.")));
        cases.add(new Case("i32", "eq(0L), a long on an INT32 column", FilterPredicate.eq("i32", 0L),
                new Rejected("Column 'i32' has physical type INT32; "
                        + "given filter predicate type INT64 is incompatible")));
        // A FLOAT16 column's literals are a `float` and two bytes, so `in(double...)` does not
        // take it. The factory still builds one, and the reader answers it, which the exclusion
        // records; `in(float...)`, which the rule does give a FLOAT16 column, does not exist yet.
        // The `not(not(in(0.1)))` cases below go through the same shape and stay as they are:
        // they prove the nulls a negated constant used to return, which is what #1193 fixed.
        cases.add(new Case("f16", "in(0.1), a double on a FLOAT16 column",
                FilterPredicate.in("f16", 0.1),
                new Rejected("Column 'f16' has physical type FIXED_LEN_BYTE_ARRAY; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible")));

        stringLiteralsOnlyWhereGetStringReads(cases);
        literalsTheColumnCannotHold(cases);
        return cases;
    }

    /// A `String` is the literal where `getString` reads the column, and a `byte[]` is the
    /// literal of every binary column. On a column whose annotation reads the stored bytes as
    /// something other than text, a `String` says nothing about the values: it would be taken as
    /// the bytes it encodes rather than as the text a caller wrote.
    private static void stringLiteralsOnlyWhereGetStringReads(List<Case> cases) {
        cases.add(new Case("str", "eq(the bytes of k0200)",
                binary("str", Operator.EQ, "k0200".getBytes(StandardCharsets.UTF_8)),
                matching(eqPhysical("k0200".getBytes(StandardCharsets.UTF_8)))));
        cases.add(new Case("enum", "gt(the bytes of E3)",
                binary("enum", Operator.GT, "E3".getBytes(StandardCharsets.UTF_8)),
                matching(cmpPhysical(Operator.GT, "E3".getBytes(StandardCharsets.UTF_8)))));

        cases.add(new Case("dec_flba", "eq(\"1.25\"), a String on a DECIMAL column",
                FilterPredicate.eq("dec_flba", "1.25"),
                new Rejected("Column 'dec_flba' is annotated DECIMAL(20, 2), "
                        + "which takes BigDecimal and byte[] literals, not a String")));
        cases.add(new Case("f16", "lt(\"6.25\"), a String on a FLOAT16 column",
                FilterPredicate.lt("f16", "6.25"),
                new Rejected("Column 'f16' is annotated FLOAT16, "
                        + "which takes float and byte[] literals, not a String")));
        cases.add(new Case("uuid", "eq(a String on a UUID column)",
                FilterPredicate.eq("uuid", "0123456789abcdef"),
                new Rejected("Column 'uuid' is annotated UUID, "
                        + "which takes UUID and byte[] literals, not a String")));
        cases.add(new Case("flba5", "eq(a String on an unannotated FIXED_LEN_BYTE_ARRAY)",
                FilterPredicate.eq("flba5", "abcde"),
                new Rejected("Column 'flba5' is an unannotated FIXED_LEN_BYTE_ARRAY, "
                        + "which takes byte[] literals, not a String")));
        cases.add(new Case("bson", "inStrings(a String on a BSON column)",
                FilterPredicate.inStrings("bson", "abc"),
                new Rejected("Column 'bson' is annotated BSON, "
                        + "which takes byte[] literals, not a String")));
        cases.add(new Case("iv", "eq(a String on an INTERVAL column)",
                FilterPredicate.eq("iv", "months days ms"),
                new Rejected("Column 'iv' is annotated INTERVAL, "
                        + "which takes byte[] literals, not a String")));
    }

    /// A literal of the column's literal type that the column cannot hold: finer than its time
    /// unit, past its scale, or outside the range or width of its physical carrier. Equality asks
    /// whether a stored value *is* such a value and is refused; an order asks where it sits among
    /// the column's values and is answered exactly, which for a literal past the carrier's range
    /// means every non-null row or none.
    private static void literalsTheColumnCannotHold(List<Case> cases) {
        Instant subMicrosecond = Instant.ofEpochSecond(1_700_000_000L, 200_500L);
        cases.add(new Case("ts_us_utc", "eq(a sub-microsecond instant)",
                FilterPredicate.eq("ts_us_utc", subMicrosecond),
                new Rejected("Column 'ts_us_utc' holds a whole number of microseconds within the INT64 range; "
                        + "the equality literal 2023-11-14T22:13:20.000200500Z is not a value it can hold")));
        cases.add(new Case("ts_us_utc", "lt(a sub-microsecond instant)",
                FilterPredicate.lt("ts_us_utc", subMicrosecond), matching(cmp(Operator.LT, subMicrosecond))));
        cases.add(new Case("ts_us_utc", "gt(a sub-microsecond instant)",
                FilterPredicate.gt("ts_us_utc", subMicrosecond), matching(cmp(Operator.GT, subMicrosecond))));

        LocalTime subMillisecond = LocalTime.ofNanoOfDay(3_600_000_000_000L + 200 * 60_000_000_000L + 1);
        cases.add(new Case("time_ms", "eq(a sub-millisecond time)",
                FilterPredicate.eq("time_ms", subMillisecond),
                new Rejected("Column 'time_ms' holds a whole number of milliseconds; "
                        + "the equality literal 04:20:00.000000001 is not a value it can hold")));
        cases.add(new Case("time_ms", "lt(a sub-millisecond time)",
                FilterPredicate.lt("time_ms", subMillisecond), matching(cmp(Operator.LT, subMillisecond))));

        BigDecimal pastTheScale = new BigDecimal("1.255");
        cases.add(new Case("dec_i32", "eq(1.255), past the scale",
                FilterPredicate.eq("dec_i32", pastTheScale),
                new Rejected("Column 'dec_i32' holds a DECIMAL of scale 2 within the INT32 range; "
                        + "the equality literal 1.255 is not a value it can hold")));
        cases.add(new Case("dec_i32", "lt(1.255), past the scale",
                FilterPredicate.lt("dec_i32", pastTheScale), matching(cmp(Operator.LT, pastTheScale))));
        cases.add(new Case("dec_i32", "gtEq(1.255), past the scale",
                FilterPredicate.gtEq("dec_i32", pastTheScale), matching(cmp(Operator.GT_EQ, pastTheScale))));
        cases.add(new Case("dec_flba", "eq(1.255), past the scale",
                FilterPredicate.eq("dec_flba", pastTheScale),
                new Rejected("Column 'dec_flba' holds a DECIMAL of scale 2 within 9 bytes; "
                        + "the equality literal 1.255 is not a value it can hold")));

        BigDecimal pastTheCarrier = new BigDecimal("99999999999.00");
        cases.add(new Case("dec_i32", "eq(99999999999.00), past the INT32 range",
                FilterPredicate.eq("dec_i32", pastTheCarrier),
                new Rejected("Column 'dec_i32' holds a DECIMAL of scale 2 within the INT32 range; "
                        + "the equality literal 99999999999.00 is not a value it can hold")));
        cases.add(new Case("dec_i32", "lt(99999999999.00), past the INT32 range",
                FilterPredicate.lt("dec_i32", pastTheCarrier), matching(everyNonNullRow())));
        cases.add(new Case("dec_i32", "gt(99999999999.00), past the INT32 range",
                FilterPredicate.gt("dec_i32", pastTheCarrier), matching(never())));

        byte[] tooWide = HEX.parseHex("01000000000000000000");
        cases.add(new Case("dec_flba", "eq(a literal wider than the column)",
                binary("dec_flba", Operator.EQ, tooWide),
                new Rejected("Column 'dec_flba' holds a DECIMAL within 9 bytes; "
                        + "the equality literal 01000000000000000000 is not a value it can hold")));
        cases.add(new Case("dec_flba", "lt(a literal wider than the column)",
                binary("dec_flba", Operator.LT, tooWide), matching(everyNonNullRow())));

        byte[] threeBytes = HEX.parseHex("010203");
        cases.add(new Case("uuid", "eq(a three-byte literal)",
                binary("uuid", Operator.EQ, threeBytes),
                new Rejected("Column 'uuid' holds a byte string of 16 bytes; "
                        + "the equality literal 010203 (3 bytes) is not a value it can hold")));
        cases.add(new Case("uuid", "lt(a three-byte literal)",
                binary("uuid", Operator.LT, threeBytes), matching(cmpPhysical(Operator.LT, threeBytes))));

        cases.add(new Case("f16", "eq(0.1), no half is 0.1",
                FilterPredicate.eq("f16", 0.1f),
                new Rejected("Column 'f16' holds a value an IEEE half represents; "
                        + "the equality literal 0.1 is not a value it can hold")));
        cases.add(new Case("f16", "gtEq(0.1), no half is 0.1",
                FilterPredicate.gtEq("f16", 0.1f), matching(cmp(Operator.GT_EQ, 0.1f))));

        cases.add(new Case("date", "eq(LocalDate.MAX), past the INT32 range",
                FilterPredicate.eq("date", LocalDate.MAX),
                new Rejected("Column 'date' holds an epoch day within the INT32 range; "
                        + "the equality literal +999999999-12-31 is not a value it can hold")));
        cases.add(new Case("date", "lt(LocalDate.MAX)", FilterPredicate.lt("date", LocalDate.MAX),
                matching(everyNonNullRow())));
        cases.add(new Case("date", "gt(LocalDate.MAX)", FilterPredicate.gt("date", LocalDate.MAX),
                matching(never())));

        // A predicate past the carrier's range is a constant, and `not` over a constant is the
        // other constant: neither returns the rows the comparison leaves unknown for being null.
        cases.add(new Case("ts_us_utc", "lt(Instant.MIN)", FilterPredicate.lt("ts_us_utc", Instant.MIN),
                matching(never())));
        cases.add(new Case("ts_us_utc", "gtEq(Instant.MIN)", FilterPredicate.gtEq("ts_us_utc", Instant.MIN),
                matching(everyNonNullRow())));
        cases.add(new Case("ts_us_utc", "not(lt(Instant.MIN))",
                FilterPredicate.not(FilterPredicate.lt("ts_us_utc", Instant.MIN)), matching(everyNonNullRow())));
        cases.add(new Case("ts_us_utc", "not(not(lt(Instant.MIN)))",
                FilterPredicate.not(FilterPredicate.not(FilterPredicate.lt("ts_us_utc", Instant.MIN))),
                matching(never())));
        cases.add(new Case("f32", "not(in(0.1)), no float is 0.1",
                FilterPredicate.not(FilterPredicate.in("f32", 0.1)), matching(everyNonNullRow())));
        cases.add(new Case("f32", "not(not(in(0.1)))",
                FilterPredicate.not(FilterPredicate.not(FilterPredicate.in("f32", 0.1))), matching(never())));
        cases.add(new Case("f16", "not(not(in(0.1)))",
                FilterPredicate.not(FilterPredicate.not(FilterPredicate.in("f16", 0.1))), matching(never())));
    }

    /// The comparison cases every column takes: each operator once, membership and its negation,
    /// and `not` over an ordered operator, which has to agree with the operator it inverts.
    private static void intCases(List<Case> cases, String column, int literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "notEq(" + literal + ")", FilterPredicate.notEq(column, literal),
                matching(notEq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "ltEq(" + literal + ")", FilterPredicate.ltEq(column, literal),
                matching(cmp(Operator.LT_EQ, literal))));
        cases.add(new Case(column, "gt(" + literal + ")", FilterPredicate.gt(column, literal),
                matching(cmp(Operator.GT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(gt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.gt(column, literal)), matching(cmp(Operator.LT_EQ, literal))));
        cases.add(new Case(column, "in(" + literal + ", " + (literal + 2) + ")",
                FilterPredicate.in(column, literal, literal + 2), matching(oneOf(literal, literal + 2))));
        cases.add(new Case(column, "not(in(" + literal + ", " + (literal + 2) + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal, literal + 2)),
                matching(noneOf(literal, literal + 2))));
    }

    private static void longCases(List<Case> cases, String column, long literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "notEq(" + literal + ")", FilterPredicate.notEq(column, literal),
                matching(notEq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)), matching(noneOf(literal))));
    }

    private static void floatCases(List<Case> cases, String column, float literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "notEq(" + literal + ")", FilterPredicate.notEq(column, literal),
                matching(notEq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
    }

    private static void doubleCases(List<Case> cases, String column, double literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)), matching(noneOf(literal))));
    }

    private static void timeCases(List<Case> cases, String column, LocalTime literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
    }

    private static void instantCases(List<Case> cases, String column, Instant literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
    }

    private static void decimalCases(List<Case> cases, String column, BigDecimal literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
    }

    private static void stringCases(List<Case> cases, String column, String literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
        cases.add(new Case(column, "notEq(" + literal + ")", FilterPredicate.notEq(column, literal),
                matching(notEq(literal))));
        cases.add(new Case(column, "lt(" + literal + ")", FilterPredicate.lt(column, literal),
                matching(cmp(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + literal + ")", FilterPredicate.gtEq(column, literal),
                matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "inStrings(" + literal + ")", FilterPredicate.inStrings(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(inStrings(" + literal + "))",
                FilterPredicate.not(FilterPredicate.inStrings(column, literal)), matching(noneOf(literal))));
    }

    private static void binaryCases(List<Case> cases, String column, byte[] literal) {
        String shown = HEX.formatHex(literal);
        cases.add(new Case(column, "eq(" + shown + ")", binary(column, Operator.EQ, literal),
                matching(eqPhysical(literal))));
        cases.add(new Case(column, "notEq(" + shown + ")", binary(column, Operator.NOT_EQ, literal),
                matching(notEqPhysical(literal))));
        cases.add(new Case(column, "lt(" + shown + ")", binary(column, Operator.LT, literal),
                matching(cmpPhysical(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + shown + ")", binary(column, Operator.GT_EQ, literal),
                matching(cmpPhysical(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "in(" + shown + ")", FilterPredicate.in(column, literal),
                matching(eqPhysical(literal))));
        cases.add(new Case(column, "not(in(" + shown + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)),
                matching(notEqPhysical(literal))));
    }

    /// An `INTERVAL` defines no order, so it takes equality and the set form only.
    private static void intervalCases(List<Case> cases, String column, byte[] literal) {
        String shown = HEX.formatHex(literal);
        cases.add(new Case(column, "eq(" + shown + ")", binary(column, Operator.EQ, literal),
                matching(eqPhysical(literal))));
        cases.add(new Case(column, "notEq(" + shown + ")", binary(column, Operator.NOT_EQ, literal),
                matching(notEqPhysical(literal))));
        cases.add(new Case(column, "in(" + shown + ")", FilterPredicate.in(column, literal),
                matching(eqPhysical(literal))));
        cases.add(new Case(column, "not(in(" + shown + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)),
                matching(notEqPhysical(literal))));
    }

    // ==================== The test ====================

    static Stream<Arguments> cells() {
        List<Case> cases = cases();
        return LAYOUTS.stream().flatMap(layout -> {
            List<String> columns = columnsOf(layout);
            return cases.stream()
                    .filter(c -> columns.contains(c.column()) || isGroup(columns, c.column()))
                    .flatMap(c -> Stream.of(ReadPath.values()).map(path -> Arguments.of(layout, c, path)));
        });
    }

    @ParameterizedTest(name = "{0} / {1} / {2}")
    @MethodSource("cells")
    void answersByTheRule(Layout layout, Case testCase, ReadPath path) throws Exception {
        String excludedBy = EXCLUDED.get(testCase.id());

        if (testCase.expect() instanceof Rejected rejected) {
            Throwable thrown = catchThrowable(() -> read(layout, testCase.predicate(), path));
            if (excludedBy == null) {
                assertThat(thrown).as("%s / %s / %s", layout, testCase, path)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(rejected.message());
            }
            else {
                // Any rejection at all, not only the message declared above: the exclusion
                // records that the reader accepts a predicate the rule refuses, and the change
                // that refuses it settles what it says.
                assertThat(thrown)
                        .as("%s / %s / %s is excluded by %s, and now is rejected as the rule says: "
                                + "delete the exclusion", layout, testCase, path, excludedBy)
                        .isNull();
            }
            return;
        }

        List<Long> expected = expectedRows(layout, testCase);
        List<Long> actual = read(layout, testCase.predicate(), path);

        if (excludedBy == null) {
            assertThat(actual).as("%s / %s / %s", layout, testCase, path).isEqualTo(expected);
        }
        else {
            assertThat(actual)
                    .as("%s / %s / %s is excluded by %s, and now agrees with the rule: "
                            + "delete the exclusion", layout, testCase, path, excludedBy)
                    .isNotEqualTo(expected);
        }
    }

    /// A constant predicate inside a conjunction, negated. `not(in("f32", 0.1))` matches every
    /// non-null `f32` row, since no `float` is `0.1`; negating the conjunction pushes its
    /// negation down to the leaves, and the branch that comes back must match no row rather than
    /// the rows `f32` is null on.
    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("flatLayoutsAndPaths")
    void negatingAConstantInsideAConjunctionKeepsNullsOut(Layout layout, ReadPath path) throws Exception {
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.not(FilterPredicate.in("f32", 0.1)),
                FilterPredicate.gt("i32", 0)));

        List<Object[]> values = values(layout, "i32");
        List<Long> expected = new ArrayList<>();
        for (int row = 0; row < values.size(); row++) {
            if (values.get(row)[0] instanceof Integer value && value <= 0) {
                expected.add((long) row);
            }
        }

        assertThat(read(layout, filter, path)).as("%s / %s", layout, path).isEqualTo(expected);
    }

    static Stream<Arguments> flatLayoutsAndPaths() {
        return Stream.of(SINGLE, MULTI, DICTIONARY)
                .flatMap(layout -> Stream.of(ReadPath.values()).map(path -> Arguments.of(layout, path)));
    }

    // ==================== Reading ====================

    private static List<Long> read(Layout layout, FilterPredicate predicate, ReadPath path) throws IOException {
        FilterPredicate filter = path.forceRecordPath ? FilterPredicate.or(predicate, NEVER) : predicate;
        ReaderConfig config = path.metadataFiltering ? ReaderConfig.defaults() : METADATA_FILTERING_OFF;
        List<Long> rows = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context, config)) {
            if (path.columnReader) {
                try (ColumnReader columns = reader.buildColumnReader("__row__").filter(filter).build()) {
                    while (columns.nextBatch()) {
                        long[] values = columns.getLongs();
                        for (int i = 0; i < columns.getRecordCount(); i++) {
                            rows.add(values[i]);
                        }
                    }
                }
            }
            else {
                try (RowReader reader1 = reader.buildRowReader().filter(filter).build()) {
                    while (reader1.hasNext()) {
                        reader1.next();
                        rows.add(reader1.getLong("__row__"));
                    }
                }
            }
        }
        return rows;
    }

    /// The rows the rule says the predicate matches, from the column read unfiltered.
    private static List<Long> expectedRows(Layout layout, Case testCase) {
        List<Object[]> values = values(layout, testCase.column());
        ColumnSchema column = schemaOf(layout, testCase.column());
        List<Long> rows = new ArrayList<>();

        for (int row = 0; row < values.size(); row++) {
            Object logical = values.get(row)[0];
            Object physical = values.get(row)[1];
            boolean matches = switch (testCase.expect()) {
                case MatchingNulls ignored -> logical == null;
                case Matching m -> logical != null && m.test().matches(column, logical, physical);
                case Rejected ignored -> throw new IllegalStateException("rejected cases answer no rows");
            };
            if (matches) {
                rows.add((long) row);
            }
        }
        return rows;
    }

    private static List<Object[]> values(Layout layout, String column) {
        return VALUES.computeIfAbsent(layout.name() + "#" + column, key -> {
            List<Object[]> values = new ArrayList<>();
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context,
                    ReaderConfig.defaults());
                 RowReader rows = reader.buildRowReader().build()) {
                while (rows.hasNext()) {
                    rows.next();
                    values.add(valueOf(rows, column));
                }
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot read column '" + column + "' of " + layout.path(), e);
            }
            return values;
        });
    }

    /// The logical and physical value of `column` in the row the reader stands on, or two nulls
    /// where the leaf or an enclosing group is absent.
    private static Object[] valueOf(RowReader rows, String column) {
        int dot = column.indexOf('.');
        if (dot < 0) {
            return rows.isNull(column)
                    ? new Object[2]
                    : new Object[] { rows.getValue(column), rows.getRawValue(column) };
        }
        String group = column.substring(0, dot);
        String field = column.substring(dot + 1);
        if (rows.isNull(group) || !(rows.getValue(group) instanceof PqStruct struct) || struct.isNull(field)) {
            return new Object[2];
        }
        return new Object[] { struct.getValue(field), struct.getRawValue(field) };
    }

    private static ColumnSchema schemaOf(Layout layout, String column) {
        return schemaOf(layout).getColumn(column);
    }

    private static FileSchema schemaOf(Layout layout) {
        return SCHEMAS.computeIfAbsent(layout.name(), key -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context,
                    ReaderConfig.defaults())) {
                return reader.getFileSchema();
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot open " + layout.path(), e);
            }
        });
    }

    private static List<String> columnsOf(Layout layout) {
        return schemaOf(layout).getColumns().stream()
                .map(c -> c.fieldPath().toString())
                .toList();
    }

    /// Whether `name` denotes a group, which only the rejection cases name.
    private static boolean isGroup(List<String> columns, String name) {
        return columns.stream().anyMatch(c -> c.startsWith(name + "."));
    }

    // ==================== The rule, in Java ====================

    private static Expect matching(ValueTest test) {
        return new Matching(test);
    }

    private static ValueTest never() {
        return (column, logical, physical) -> false;
    }

    private static ValueTest everyNonNullRow() {
        return (column, logical, physical) -> true;
    }

    private static ValueTest eq(Object literal) {
        return cmp(Operator.EQ, literal);
    }

    private static ValueTest notEq(Object literal) {
        return cmp(Operator.NOT_EQ, literal);
    }

    private static ValueTest cmp(Operator op, Object literal) {
        return (column, logical, physical) -> holds(op, compare(column, logical, literal));
    }

    private static ValueTest eqPhysical(Object literal) {
        return cmpPhysical(Operator.EQ, literal);
    }

    private static ValueTest notEqPhysical(Object literal) {
        return cmpPhysical(Operator.NOT_EQ, literal);
    }

    private static ValueTest cmpPhysical(Operator op, Object literal) {
        return (column, logical, physical) -> holds(op, compare(column, physical, literal));
    }

    private static ValueTest oneOf(Object... literals) {
        return (column, logical, physical) -> Arrays.stream(literals)
                .anyMatch(literal -> compare(column, logical, literal) == 0);
    }

    private static ValueTest noneOf(Object... literals) {
        return (column, logical, physical) -> Arrays.stream(literals)
                .noneMatch(literal -> compare(column, logical, literal) == 0);
    }

    private static boolean holds(Operator op, int comparison) {
        return switch (op) {
            case EQ -> comparison == 0;
            case NOT_EQ -> comparison != 0;
            case LT -> comparison < 0;
            case LT_EQ -> comparison <= 0;
            case GT -> comparison > 0;
            case GT_EQ -> comparison >= 0;
        };
    }

    /// `value` against `literal` in the order the column's type defines.
    private static int compare(ColumnSchema column, Object value, Object literal) {
        return switch (literal) {
            case Integer l -> unsigned(column)
                    ? Integer.compareUnsigned(((Number) value).intValue(), l)
                    : Integer.compare(((Number) value).intValue(), l);
            case Long l -> unsigned(column)
                    ? Long.compareUnsigned(((Number) value).longValue(), l)
                    : Long.compare(((Number) value).longValue(), l);
            case Float l -> Float.compare(((Number) value).floatValue(), l);
            case Double l -> Double.compare(((Number) value).doubleValue(), l);
            case Boolean l -> Boolean.compare((Boolean) value, l);
            case String l -> Arrays.compareUnsigned(utf8(value), l.getBytes(StandardCharsets.UTF_8));
            case byte[] l -> compareBytes(column, (byte[]) value, l);
            case LocalDate l -> ((LocalDate) value).compareTo(l);
            case Instant l -> ((Instant) value).compareTo(l);
            case LocalTime l -> ((LocalTime) value).compareTo(l);
            case BigDecimal l -> ((BigDecimal) value).compareTo(l);
            case UUID l -> Arrays.compareUnsigned(uuidBytes((UUID) value), uuidBytes(l));
            default -> throw new IllegalArgumentException("No rule for a literal of type "
                    + literal.getClass().getName());
        };
    }

    /// A byte literal stands for what the column's annotation decodes it to: a number for a
    /// `DECIMAL`, a half for a `FLOAT16`, and the bytes themselves everywhere else.
    private static int compareBytes(ColumnSchema column, byte[] value, byte[] literal) {
        if (column.logicalType() instanceof LogicalType.DecimalType) {
            return unscaled(value).compareTo(unscaled(literal));
        }
        if (column.logicalType() instanceof LogicalType.Float16Type) {
            return Float.compare(half(value), half(literal));
        }
        return Arrays.compareUnsigned(value, literal);
    }

    private static boolean unsigned(ColumnSchema column) {
        return column.logicalType() instanceof LogicalType.IntType intType && !intType.isSigned();
    }

    private static byte[] utf8(Object value) {
        return value instanceof String text ? text.getBytes(StandardCharsets.UTF_8) : (byte[]) value;
    }

    private static BigInteger unscaled(byte[] bytes) {
        return bytes.length == 0 ? BigInteger.ZERO : new BigInteger(bytes);
    }

    private static float half(byte[] bytes) {
        return Float.float16ToFloat((short) ((bytes[1] & 0xFF) << 8 | bytes[0] & 0xFF));
    }

    // ==================== Literals ====================

    private static final HexFormat HEX = HexFormat.of();

    private static FilterPredicate binary(String column, Operator op, byte[] value) {
        return switch (op) {
            case EQ -> FilterPredicate.eq(column, value);
            case NOT_EQ -> FilterPredicate.notEq(column, value);
            case LT -> FilterPredicate.lt(column, value);
            case LT_EQ -> FilterPredicate.ltEq(column, value);
            case GT -> FilterPredicate.gt(column, value);
            case GT_EQ -> FilterPredicate.gtEq(column, value);
        };
    }

    /// The two little-endian bytes a `FLOAT16` column stores for `value`.
    private static byte[] half(float value) {
        short bits = Float.floatToFloat16(value);
        return new byte[] { (byte) bits, (byte) (bits >> 8) };
    }

    /// The twelve bytes an `INTERVAL` column stores: months, days and milliseconds, each an
    /// unsigned little-endian 32-bit value.
    private static byte[] interval(int months, int days, int millis) {
        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(months).putInt(days).putInt(millis);
        return bytes;
    }

    /// The `uuid` column's value in `row`, as the fixture writes it.
    private static UUID uuidOfRow(int row) {
        byte[] bytes = new byte[16];
        bytes[0] = (byte) (row / 2);
        bytes[15] = (byte) (row & 0xFF);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static byte[] uuidBytes(UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
