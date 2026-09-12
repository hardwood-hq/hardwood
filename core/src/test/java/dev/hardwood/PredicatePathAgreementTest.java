/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
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
/// **Values.** The oracle is only as right as the values it reads, so [#readsTheValuesTheFixtureHolds]
/// first holds those values to what `tools/simple-datagen.py` wrote: each corpus has a
/// `<corpus>.values.tsv.gz` beside its files, with every row's logical and physical value. A decoding
/// defect fails there, once, rather than passing on both sides of every predicate.
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
/// **Layouts.** `predicate_single`, `predicate_multi`, `predicate_dict` and `predicate_bloom` hold
/// the same 400 rows in one row group, in four, dictionary-encoded, and in four with a Bloom filter
/// on every column that takes one, so the row-group bounds, the page index, the dictionary and the
/// Bloom filter each decide a predicate that the record-level comparison decides again.
/// `predicate_nested` adds a struct whose leaf is null under a present struct, and a leaf below a
/// repeated path. `predicate_opaque` carries, in the single, dictionary and Bloom layouts, the
/// columns that stay out of the corpus the differential tests read: `BSON`, which DuckDB cannot
/// open, `INTERVAL`, a `DECIMAL` over `BYTE_ARRAY` holding a padded and an empty encoding, a `NULL`
/// column and `GEOMETRY`.
/// `predicate_int96` carries `INT96` columns in the same three layouts, one value stored under a
/// non-canonical encoding, and bounds recorded in byte order rather than in time order.
/// `predicate_ts12` carries `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)` of every unit, in the
/// single, multi, dictionary and Bloom layouts, with values past the `INT64` nanosecond range on both
/// sides of the epoch and bounds recorded in the order of the values. Its `pages` layout holds one
/// value per page under a page index, so the page index decides the predicates too.
class PredicatePathAgreementTest {

    private static final Path RES = Paths.get("src/test/resources/predicate");

    /// Matches no row — nothing sorts below the empty string, and `__row__` is never negative — and
    /// reads `zz` and `__row__` in both conjunctions, which the batch compiler cannot give one bitmap
    /// per column for. `or`-ed into any predicate, it puts the filter on the record-level path.
    private static final FilterPredicate NEVER = FilterPredicate.or(
            FilterPredicate.and(FilterPredicate.lt("zz", new byte[0]), FilterPredicate.gtEq("__row__", 0L)),
            FilterPredicate.and(FilterPredicate.lt("zz", new byte[0]), FilterPredicate.lt("__row__", 0L)));

    private static final ReaderConfig METADATA_FILTERING_OFF = ReaderConfig.builder()
            .option("hardwood.metadata-filtering", "false")
            .build();

    private static HardwoodContext context;

    /// Values of one column of one file, read unfiltered: the logical value an accessor returns
    /// and the physical one, indexed by row.
    private static final Map<String, List<Object[]>> VALUES = new HashMap<>();

    /// The values `tools/simple-datagen.py` wrote to each corpus, read once.
    private static final Map<String, Map<String, List<String[]>>> WRITTEN = new HashMap<>();

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
        WRITTEN.clear();
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
    private static final Layout BLOOM = new Layout("bloom", RES.resolve("predicate_bloom.parquet"));
    private static final Layout NESTED = new Layout("nested", RES.resolve("predicate_nested_single.parquet"));
    private static final Layout NESTED_MULTI = new Layout("nested-multi-rg",
            RES.resolve("predicate_nested_multi.parquet"));
    private static final Layout OPAQUE = new Layout("opaque", RES.resolve("predicate_opaque_single.parquet"));
    private static final Layout OPAQUE_DICTIONARY = new Layout("opaque-dictionary",
            RES.resolve("predicate_opaque_dict.parquet"));
    private static final Layout OPAQUE_BLOOM = new Layout("opaque-bloom",
            RES.resolve("predicate_opaque_bloom.parquet"));

    private static final Layout INT96_SINGLE = new Layout("int96", RES.resolve("predicate_int96_single.parquet"));
    private static final Layout INT96_MULTI = new Layout("int96-multi-rg",
            RES.resolve("predicate_int96_multi.parquet"));
    private static final Layout INT96_DICTIONARY = new Layout("int96-dictionary",
            RES.resolve("predicate_int96_dict.parquet"));

    private static final Layout TS12_SINGLE = new Layout("ts12", RES.resolve("predicate_ts12_single.parquet"));
    private static final Layout TS12_MULTI = new Layout("ts12-multi-rg", RES.resolve("predicate_ts12_multi.parquet"));
    private static final Layout TS12_DICTIONARY = new Layout("ts12-dictionary",
            RES.resolve("predicate_ts12_dict.parquet"));
    private static final Layout TS12_BLOOM = new Layout("ts12-bloom", RES.resolve("predicate_ts12_bloom.parquet"));
    private static final Layout TS12_PAGES = new Layout("ts12-pages", RES.resolve("predicate_ts12_pages.parquet"));

    private static final List<Layout> LAYOUTS = List.of(SINGLE, MULTI, DICTIONARY, BLOOM, NESTED, NESTED_MULTI,
            OPAQUE, OPAQUE_DICTIONARY, OPAQUE_BLOOM, INT96_SINGLE, INT96_MULTI, INT96_DICTIONARY,
            TS12_SINGLE, TS12_MULTI, TS12_DICTIONARY, TS12_BLOOM, TS12_PAGES);

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
    private static final Map<String, String> EXCLUDED = Map.of();

    // ==================== Cases ====================

    static List<Case> cases() {
        List<Case> cases = new ArrayList<>();

        // --- BOOLEAN: false before true ---
        cases.add(new Case("bool", "eq(true)", FilterPredicate.eq("bool", true), matching(eq(true))));
        cases.add(new Case("bool", "notEq(true)", FilterPredicate.notEq("bool", true), matching(notEq(true))));
        cases.add(new Case("bool", "lt(true)", FilterPredicate.lt("bool", true), matching(cmp(Operator.LT, true))));
        cases.add(new Case("bool", "ltEq(false)", FilterPredicate.ltEq("bool", false),
                matching(cmp(Operator.LT_EQ, false))));
        cases.add(new Case("bool", "gt(false)", FilterPredicate.gt("bool", false), matching(cmp(Operator.GT, false))));
        cases.add(new Case("bool", "gtEq(true)", FilterPredicate.gtEq("bool", true),
                matching(cmp(Operator.GT_EQ, true))));
        cases.add(new Case("bool", "not(gt(false))", FilterPredicate.not(FilterPredicate.gt("bool", false)),
                matching(cmp(Operator.LT_EQ, false))));
        // The two values leave each of the other ordered operators a constant.
        cases.add(new Case("bool", "lt(false)", FilterPredicate.lt("bool", false), matching(never())));
        cases.add(new Case("bool", "ltEq(true)", FilterPredicate.ltEq("bool", true),
                matching(everyNonNullRow())));
        cases.add(new Case("bool", "gt(true)", FilterPredicate.gt("bool", true), matching(never())));
        cases.add(new Case("bool", "gtEq(false)", FilterPredicate.gtEq("bool", false),
                matching(everyNonNullRow())));
        cases.add(new Case("bool", "not(ltEq(true))", FilterPredicate.not(FilterPredicate.ltEq("bool", true)),
                matching(never())));
        cases.add(new Case("bool", "isNull", FilterPredicate.isNull("bool"), new MatchingNulls()));
        cases.add(new Case("bool", "isNotNull", FilterPredicate.isNotNull("bool"), matching(everyNonNullRow())));

        // --- INT32 / INT64, signed and unsigned ---
        intCases(cases, "i32", 0);
        // No nulls, so a row group whose every value matches is decided in full from its statistics.
        intCases(cases, "i32_req", 0);
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
        // A byte literal is the stored bytes, so it matches one NaN encoding rather than every NaN:
        // rows 10 and 390 store 00 7E, row 150 the NaN with its sign bit set, 00 FE.
        storedByteCases(cases, "f16", new byte[] { 0x00, 0x7E });
        cases.add(new Case("f16", "lt(bytes of 6.25)", binary("f16", Operator.LT, half(6.25f)),
                new Rejected(notByteOrdered("f16", "annotated FLOAT16", "a float"))));

        // --- DATE, TIME, TIMESTAMP ---
        cases.add(new Case("date", "eq(row 200)", FilterPredicate.eq("date", LocalDate.ofEpochDay(19000)),
                matching(eq(LocalDate.ofEpochDay(19000)))));
        cases.add(new Case("date", "lt(row 200)", FilterPredicate.lt("date", LocalDate.ofEpochDay(19000)),
                matching(cmp(Operator.LT, LocalDate.ofEpochDay(19000)))));
        cases.add(new Case("date", "eq(epoch day 19000 as int)", FilterPredicate.eq("date", 19000),
                matching(eqPhysical(19000))));
        cases.add(new Case("date", "in(row 200, row 202)",
                FilterPredicate.in("date", LocalDate.ofEpochDay(19000), LocalDate.ofEpochDay(19002)),
                matching(oneOf(LocalDate.ofEpochDay(19000), LocalDate.ofEpochDay(19002)))));
        cases.add(new Case("date", "not(in(row 200))",
                FilterPredicate.not(FilterPredicate.in("date", LocalDate.ofEpochDay(19000))),
                matching(noneOf(LocalDate.ofEpochDay(19000)))));

        timeCases(cases, "time_ms", LocalTime.ofNanoOfDay(3_600_000_000_000L + 200 * 60_000_000_000L));
        timeCases(cases, "time_us", LocalTime.ofNanoOfDay(200 * 60_000_000_000L + 200_000L));
        timeCases(cases, "time_ns", LocalTime.ofNanoOfDay(200 * 60_000_000_000L + 200L));
        instantCases(cases, "ts_ms_utc", Instant.ofEpochMilli(1_700_000_000_000L));
        instantCases(cases, "ts_us_utc", Instant.ofEpochSecond(1_700_000_000L, 200_000L));
        instantCases(cases, "ts_ns_utc", Instant.ofEpochSecond(1_700_000_000L, 200L));
        localDateTimeCases(cases, "ts_us_local", LocalDateTime.ofEpochSecond(1_700_000_000L, 200_000, ZoneOffset.UTC));
        cases.add(new Case("ts_us_local", "eq(an Instant), a local timestamp",
                FilterPredicate.eq("ts_us_local", Instant.ofEpochSecond(1_700_000_000L, 200_000L)),
                new Rejected("Column 'ts_us_local' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false),"
                        + " which takes LocalDateTime and long literals, not an Instant")));
        cases.add(new Case("ts_us_utc", "eq(a LocalDateTime), a UTC timestamp",
                FilterPredicate.eq("ts_us_utc", LocalDateTime.ofEpochSecond(1_700_000_000L, 200_000, ZoneOffset.UTC)),
                new Rejected("Column 'ts_us_utc' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true),"
                        + " which takes Instant and long literals, not a LocalDateTime")));
        cases.add(new Case("ts_us_local", "eq(stored microseconds as long)",
                FilterPredicate.eq("ts_us_local", 1_700_000_000_000_200L), matching(eqPhysical(1_700_000_000_000_200L))));

        // --- DECIMAL over INT32, INT64 and FIXED_LEN_BYTE_ARRAY ---
        decimalCases(cases, "dec_i32", new BigDecimal("0.00"));
        decimalCases(cases, "dec_i64", new BigDecimal("0.00"));
        decimalCases(cases, "dec_flba", new BigDecimal("0.00"));
        cases.add(new Case("dec_i32", "eq(unscaled 125 as int)", FilterPredicate.eq("dec_i32", 125),
                matching(eqPhysical(125))));
        // 1.25 as the nine bytes dec_flba stores it.
        storedByteCases(cases, "dec_flba", HEX.parseHex("00000000000000007d"));
        cases.add(new Case("dec_flba", "eq(125 in its fewest bytes)", binary("dec_flba", Operator.EQ, new byte[] { 0x7D }),
                new Rejected("Column 'dec_flba' holds a byte string of 9 bytes; "
                        + "the equality literal 7d (1 bytes) is not a value it can hold")));
        cases.add(new Case("dec_flba", "gt(the stored bytes of 1.25)",
                binary("dec_flba", Operator.GT, HEX.parseHex("00000000000000007d")),
                new Rejected(notByteOrdered("dec_flba", "annotated DECIMAL(20, 2)", "a BigDecimal"))));

        // --- Text and binary ---
        stringCases(cases, "str", "k0200");
        stringCases(cases, "json", "{\"k\":\"k0200\"}");
        stringCases(cases, "enum", "E3");
        binaryCases(cases, "ba", new byte[] { 0, (byte) 200 });
        binaryCases(cases, "flba5", new byte[] { 100, 0, 0, 0, (byte) 200 });
        binaryCases(cases, "bson", new byte[] { 0, (byte) 200, 0 });
        intervalCases(cases, "iv", new PqInterval(200 % 13, 200 % 29, 200 * 1000));

        // --- DECIMAL over BYTE_ARRAY: row 200 stores zero as no bytes, 202 and 198 a padded encoding ---
        decimalCases(cases, "dec_ba", new BigDecimal("0.000"));
        decimalCases(cases, "dec_ba", new BigDecimal("2.5"));
        decimalCases(cases, "dec_ba", new BigDecimal("-2.500"));
        storedByteCases(cases, "dec_ba", new byte[0]);
        storedByteCases(cases, "dec_ba", HEX.parseHex("0009c4"));
        storedByteCases(cases, "dec_ba", HEX.parseHex("09c4"));
        storedByteCases(cases, "dec_ba", HEX.parseHex("fff63c"));
        cases.add(new Case("dec_ba", "gtEq(the stored bytes of 2.500)", binary("dec_ba", Operator.GT_EQ, HEX.parseHex("0009c4")),
                new Rejected(notByteOrdered("dec_ba", "annotated DECIMAL(30, 3)", "a BigDecimal"))));

        // --- NULL: no row holds a value ---
        cases.add(new Case("nul", "eq(1)", FilterPredicate.eq("nul", 1), matching(never())));
        cases.add(new Case("nul", "notEq(1)", FilterPredicate.notEq("nul", 1), matching(never())));
        cases.add(new Case("nul", "in(1)", FilterPredicate.in("nul", 1), matching(never())));
        cases.add(new Case("nul", "not(in(1))", FilterPredicate.not(FilterPredicate.in("nul", 1)), matching(never())));
        cases.add(new Case("nul", "isNull", FilterPredicate.isNull("nul"), new MatchingNulls()));
        cases.add(new Case("nul", "lt(1), an ordered operator on a NULL column", FilterPredicate.lt("nul", 1),
                new Rejected("Column 'nul' is annotated NULL, which defines no order; "
                        + "it takes int literals with eq, notEq and in only")));

        // --- GEOMETRY: equality over the WKB bytes ---
        byte[] pointAt200 = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1).putInt(1).putDouble(0.0).putDouble(100.0).array();
        storedByteCases(cases, "geom", pointAt200);
        cases.add(new Case("geom", "lt(the WKB of row 200), an ordered operator on a GEOMETRY column",
                binary("geom", Operator.LT, pointAt200),
                new Rejected("Column 'geom' is annotated GEOMETRY(OGC:CRS84), which defines no order; "
                        + "it takes byte[] literals with eq, notEq and in only")));
        cases.add(new Case("geom", "eq(a String on a GEOMETRY column)", FilterPredicate.eq("geom", "POINT"),
                new Rejected("Column 'geom' is annotated GEOMETRY(OGC:CRS84), which takes byte[] literals, not a String")));
        cases.add(new Case("str", "eq(emoji at row 399)", FilterPredicate.eq("str", "😀"),
                matching(eq("😀"))));
        cases.add(new Case("str", "gt(full-width tilde at row 398)", FilterPredicate.gt("str", "～"),
                matching(cmp(Operator.GT, "～"))));

        // --- INT96 ---
        int96Cases(cases, "ts96");
        int96Cases(cases, "s.ts96");

        // --- TIMESTAMP over FIXED_LEN_BYTE_ARRAY(12) ---
        ts12Cases(cases, "ts12_ns", LogicalType.TimeUnit.NANOS);
        ts12Cases(cases, "s.ts12", LogicalType.TimeUnit.NANOS);
        ts12Cases(cases, "ts12_ms", LogicalType.TimeUnit.MILLIS);
        ts12LocalCases(cases, "ts12_us_local");

        // --- UUID ---
        UUID uuidAt200 = uuidOfRow(200);
        cases.add(new Case("uuid", "eq(row 200)", FilterPredicate.eq("uuid", uuidAt200), matching(eq(uuidAt200))));
        cases.add(new Case("uuid", "gt(row 200)", FilterPredicate.gt("uuid", uuidAt200),
                matching(cmp(Operator.GT, uuidAt200))));
        cases.add(new Case("uuid", "in(row 200, row 201)", FilterPredicate.in("uuid", uuidAt200, uuidOfRow(201)),
                matching(oneOf(uuidAt200, uuidOfRow(201)))));
        cases.add(new Case("uuid", "not(in(row 200))", FilterPredicate.not(FilterPredicate.in("uuid", uuidAt200)),
                matching(noneOf(uuidAt200))));

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
                new Rejected("Column 'i32' is an unannotated INT32"
                        + ", which takes int literals, not a long")));
        // A membership literal is the column's literal type, so a `double` set takes a DOUBLE
        // column only, as `eq(double)` does.
        cases.add(new Case("f16", "in(0.1), a double on a FLOAT16 column",
                FilterPredicate.in("f16", 0.1),
                new Rejected("Column 'f16' is annotated FLOAT16"
                        + ", which takes float and byte[] literals, not a double")));
        cases.add(new Case("f32", "in(12.5), a double on a FLOAT column",
                FilterPredicate.in("f32", 12.5),
                new Rejected("Column 'f32' is an unannotated FLOAT"
                        + ", which takes float literals, not a double")));

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
        cases.add(new Case("bson", "in(a String on a BSON column)",
                FilterPredicate.in("bson", "abc"),
                new Rejected("Column 'bson' is annotated BSON, "
                        + "which takes byte[] literals, not a String")));
        cases.add(new Case("iv", "eq(a String on an INTERVAL column)",
                FilterPredicate.eq("iv", "months days ms"),
                new Rejected("Column 'iv' is annotated INTERVAL"
                        + ", which takes PqInterval and byte[] literals, not a String")));
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
        LocalDateTime subMicrosecondWallClock = LocalDateTime.ofEpochSecond(1_700_000_000L, 200_500, ZoneOffset.UTC);
        cases.add(new Case("ts_us_local", "eq(a sub-microsecond wall clock)",
                FilterPredicate.eq("ts_us_local", subMicrosecondWallClock),
                new Rejected("Column 'ts_us_local' holds a whole number of microseconds within the INT64 range; "
                        + "the equality literal " + subMicrosecondWallClock + " is not a value it can hold")));
        cases.add(new Case("ts_us_local", "lt(a sub-microsecond wall clock)",
                FilterPredicate.lt("ts_us_local", subMicrosecondWallClock),
                matching(cmp(Operator.LT, subMicrosecondWallClock))));
        cases.add(new Case("ts_us_local", "gt(a sub-microsecond wall clock)",
                FilterPredicate.gt("ts_us_local", subMicrosecondWallClock),
                matching(cmp(Operator.GT, subMicrosecondWallClock))));

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
                new Rejected("Column 'dec_flba' holds a byte string of 9 bytes; "
                        + "the equality literal 01000000000000000000 (10 bytes) is not a value it can hold")));
        cases.add(new Case("dec_flba", "lt(a literal wider than the column)",
                binary("dec_flba", Operator.LT, tooWide),
                new Rejected(notByteOrdered("dec_flba", "annotated DECIMAL(20, 2)", "a BigDecimal"))));

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

        // Every probe of a set form is an equality literal, so one the column cannot hold refuses
        // the set, whatever the others are.
        cases.add(new Case("ts_us_utc", "in(row 200, a sub-microsecond instant)",
                FilterPredicate.in("ts_us_utc", Instant.ofEpochSecond(1_700_000_000L, 200_000L), subMicrosecond),
                new Rejected("Column 'ts_us_utc' holds a whole number of microseconds within the INT64 range; "
                        + "the equality literal 2023-11-14T22:13:20.000200500Z is not a value it can hold")));
        cases.add(new Case("ts_us_local", "in(a sub-microsecond wall clock)",
                FilterPredicate.in("ts_us_local", subMicrosecondWallClock),
                new Rejected("Column 'ts_us_local' holds a whole number of microseconds within the INT64 range; "
                        + "the equality literal " + subMicrosecondWallClock + " is not a value it can hold")));
        cases.add(new Case("time_ms", "in(a sub-millisecond time)",
                FilterPredicate.in("time_ms", subMillisecond),
                new Rejected("Column 'time_ms' holds a whole number of milliseconds; "
                        + "the equality literal 04:20:00.000000001 is not a value it can hold")));
        cases.add(new Case("dec_i64", "in(0.00, 1.255), past the scale",
                FilterPredicate.in("dec_i64", new BigDecimal("0.00"), pastTheScale),
                new Rejected("Column 'dec_i64' holds a DECIMAL of scale 2 within the INT64 range; "
                        + "the equality literal 1.255 is not a value it can hold")));
        cases.add(new Case("dec_i32", "in(99999999999.00), past the INT32 range",
                FilterPredicate.in("dec_i32", pastTheCarrier),
                new Rejected("Column 'dec_i32' holds a DECIMAL of scale 2 within the INT32 range; "
                        + "the equality literal 99999999999.00 is not a value it can hold")));
        cases.add(new Case("dec_flba", "in(a DECIMAL wider than the column)",
                FilterPredicate.in("dec_flba", new BigDecimal("1E+30")),
                new Rejected("Column 'dec_flba' holds a DECIMAL of scale 2 within 9 bytes; "
                        + "the equality literal 1000000000000000000000000000000 is not a value it can hold")));
        cases.add(new Case("f16", "in(6.25, 0.1), no half is 0.1",
                FilterPredicate.in("f16", 6.25f, 0.1f),
                new Rejected("Column 'f16' holds a value an IEEE half represents; "
                        + "the equality literal 0.1 is not a value it can hold")));
        cases.add(new Case("date", "in(LocalDate.MAX), past the INT32 range",
                FilterPredicate.in("date", LocalDate.MAX),
                new Rejected("Column 'date' holds an epoch day within the INT32 range; "
                        + "the equality literal +999999999-12-31 is not a value it can hold")));
        cases.add(new Case("iv", "in(an interval with a negative component)",
                FilterPredicate.in("iv", new PqInterval(-1, 0, 0)),
                new Rejected("Column 'iv' holds an interval whose months, days and milliseconds are each "
                        + "within [0, 4294967295]; the equality literal PqInterval[months=-1, days=0, milliseconds=0]"
                        + " is not a value it can hold")));
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
        float other = literal + 2;
        cases.add(new Case(column, "in(" + literal + ", " + other + ", NaN)",
                FilterPredicate.in(column, literal, other, Float.NaN), matching(oneOf(literal, other, Float.NaN))));
        cases.add(new Case(column, "not(in(" + literal + ", " + other + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal, other)), matching(noneOf(literal, other))));
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
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)), matching(noneOf(literal))));
    }

    private static void instantCases(List<Case> cases, String column, Instant literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
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

    private static void localDateTimeCases(List<Case> cases, String column, LocalDateTime literal) {
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
        cases.add(new Case(column, "not(lt(" + literal + "))",
                FilterPredicate.not(FilterPredicate.lt(column, literal)), matching(cmp(Operator.GT_EQ, literal))));
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)), matching(noneOf(literal))));
    }

    private static void decimalCases(List<Case> cases, String column, BigDecimal literal) {
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal), matching(eq(literal))));
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
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(oneOf(literal))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)), matching(noneOf(literal))));
    }

    private static void binaryCases(List<Case> cases, String column, byte[] literal) {
        String shown = HEX.formatHex(literal);
        storedByteCases(cases, column, literal);
        cases.add(new Case(column, "lt(" + shown + ")", binary(column, Operator.LT, literal),
                matching(cmpPhysical(Operator.LT, literal))));
        cases.add(new Case(column, "gtEq(" + shown + ")", binary(column, Operator.GT_EQ, literal),
                matching(cmpPhysical(Operator.GT_EQ, literal))));
    }

    /// Equality and membership over the stored bytes, which every binary column takes.
    private static void storedByteCases(List<Case> cases, String column, byte[] literal) {
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

    /// The refusal of an ordered operator over a `byte[]` literal on a column whose values do not
    /// order as their bytes.
    private static String notByteOrdered(String column, String description, String typedLiteral) {
        return "Column '" + column + "' is " + description + ", whose values do not order as their stored bytes; "
                + "a byte[] literal takes eq, notEq and in there, and an ordered predicate takes " + typedLiteral;
    }

    /// An `INTERVAL` defines no order, so it takes equality and the set form only, over either of
    /// its two literals: the [PqInterval] its accessor returns and the twelve stored bytes.
    private static void intervalCases(List<Case> cases, String column, PqInterval literal) {
        byte[] bytes = intervalBytes(literal);
        String shown = HEX.formatHex(bytes);
        cases.add(new Case(column, "eq(" + shown + ")", binary(column, Operator.EQ, bytes),
                matching(eqPhysical(bytes))));
        cases.add(new Case(column, "notEq(" + shown + ")", binary(column, Operator.NOT_EQ, bytes),
                matching(notEqPhysical(bytes))));
        cases.add(new Case(column, "in(" + shown + ")", FilterPredicate.in(column, bytes),
                matching(eqPhysical(bytes))));
        cases.add(new Case(column, "not(in(" + shown + "))",
                FilterPredicate.not(FilterPredicate.in(column, bytes)),
                matching(notEqPhysical(bytes))));
        cases.add(new Case(column, "eq(" + literal + ")", FilterPredicate.eq(column, literal),
                matching(eqPhysical(bytes))));
        cases.add(new Case(column, "notEq(" + literal + ")", FilterPredicate.notEq(column, literal),
                matching(notEqPhysical(bytes))));
        cases.add(new Case(column, "not(eq(" + literal + "))",
                FilterPredicate.not(FilterPredicate.eq(column, literal)),
                matching(notEqPhysical(bytes))));
        cases.add(new Case(column, "in(" + literal + ")", FilterPredicate.in(column, literal),
                matching(eqPhysical(bytes))));
        cases.add(new Case(column, "not(in(" + literal + "))",
                FilterPredicate.not(FilterPredicate.in(column, literal)),
                matching(notEqPhysical(bytes))));

        String noOrder = "Column '" + column + "' is annotated INTERVAL, which defines no order;"
                + " it takes PqInterval and byte[] literals with eq, notEq and in only";
        cases.add(new Case(column, "lt(" + shown + "), an ordered operator on an INTERVAL column",
                binary(column, Operator.LT, bytes), new Rejected(noOrder)));
        cases.add(new Case(column, "gtEq(" + literal + "), an ordered operator on an INTERVAL column",
                new FilterPredicate.IntervalColumnPredicate(column, Operator.GT_EQ, literal),
                new Rejected(noOrder)));
    }

    /// An `INT96` holds an instant, which the [Instant] `getTimestamp` returns denotes in every
    /// order, whatever encoding of it the file stores. A `byte[]` literal is the twelve stored
    /// bytes and takes equality and membership only.
    private static void int96Cases(List<Case> cases, String column) {
        Instant atRow200 = int96Instant(200);
        instantCases(cases, column, atRow200);
        cases.add(new Case(column, "notEq(" + atRow200 + ")", FilterPredicate.notEq(column, atRow200),
                matching(notEq(atRow200))));
        cases.add(new Case(column, "ltEq(" + atRow200 + ")", FilterPredicate.ltEq(column, atRow200),
                matching(cmp(Operator.LT_EQ, atRow200))));
        cases.add(new Case(column, "gt(" + atRow200 + ")", FilterPredicate.gt(column, atRow200),
                matching(cmp(Operator.GT, atRow200))));
        cases.add(new Case(column, "lt(the epoch)", FilterPredicate.lt(column, Instant.EPOCH),
                matching(cmp(Operator.LT, Instant.EPOCH))));
        storedByteCases(cases, column, int96Bytes(atRow200));
        cases.add(new Case(column, "lt(the bytes of row 200)", binary(column, Operator.LT, int96Bytes(atRow200)),
                new Rejected(notByteOrdered(column, "a legacy INT96 TIMESTAMP (no isAdjustedToUTC field)",
                        "an Instant"))));

        // Row 250 is stored with its day one lower and its nanoseconds of the day one day longer.
        Instant nonCanonical = int96Instant(NON_CANONICAL_ROW);
        cases.add(new Case(column, "eq(the instant stored non-canonically at row 250)",
                FilterPredicate.eq(column, nonCanonical), matching(eq(nonCanonical))));
        // The stored bytes of row 250 match it; the canonical encoding of the same instant does not.
        storedByteCases(cases, column, nonCanonicalInt96Bytes(nonCanonical));
        cases.add(new Case(column, "eq(the canonical bytes of row 250)",
                binary(column, Operator.EQ, int96Bytes(nonCanonical)),
                matching(eqPhysical(int96Bytes(nonCanonical)))));
        cases.add(new Case(column, "in(the canonical bytes of row 250)",
                FilterPredicate.in(column, int96Bytes(nonCanonical)),
                matching(eqPhysical(int96Bytes(nonCanonical)))));

        String wrongWidth = "Column '" + column + "' is an INT96, whose literal is 12 bytes, not 11";
        cases.add(new Case(column, "eq(an eleven-byte literal)", binary(column, Operator.EQ, new byte[11]),
                new Rejected(wrongWidth)));
        cases.add(new Case(column, "lt(an eleven-byte literal)", binary(column, Operator.LT, new byte[11]),
                new Rejected(notByteOrdered(column, "a legacy INT96 TIMESTAMP (no isAdjustedToUTC field)",
                        "an Instant"))));
        cases.add(new Case(column, "in(an eleven-byte literal)", FilterPredicate.in(column, new byte[11]),
                new Rejected(wrongWidth)));
        cases.add(new Case(column, "eq(a LocalDateTime), an INT96 column",
                FilterPredicate.eq(column, LocalDateTime.ofEpochSecond(1_700_000_000L, 200, ZoneOffset.UTC)),
                new Rejected("Column '" + column + "' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field),"
                        + " which takes Instant and byte[] literals, not a LocalDateTime")));
        cases.add(new Case(column, "eq(a long), an INT96 column", FilterPredicate.eq(column, 0L),
                new Rejected("Column '" + column + "' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field),"
                        + " which takes Instant and byte[] literals, not a long")));

        // Instant.MAX lies millions of years past the latest instant an INT96 encodes.
        cases.add(new Case(column, "eq(Instant.MAX), past every instant an INT96 encodes",
                FilterPredicate.eq(column, Instant.MAX),
                new Rejected("Column '" + column + "' holds an instant within the range an INT96 encodes; "
                        + "the equality literal +1000000000-12-31T23:59:59.999999999Z is not a value it can hold")));
        cases.add(new Case(column, "lt(Instant.MAX)", FilterPredicate.lt(column, Instant.MAX),
                matching(everyNonNullRow())));
        cases.add(new Case(column, "gt(Instant.MAX)", FilterPredicate.gt(column, Instant.MAX),
                matching(never())));
        cases.add(new Case(column, "not(lt(Instant.MIN))",
                FilterPredicate.not(FilterPredicate.lt(column, Instant.MIN)), matching(everyNonNullRow())));
    }

    /// A `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` orders by the instant its count stands for, which
    /// the stored bytes, little-endian with the sign in the last byte, do not. An [Instant] takes every
    /// operator; a `byte[]` literal is the twelve stored bytes and takes equality and membership only.
    private static void ts12Cases(List<Case> cases, String column, LogicalType.TimeUnit unit) {
        Instant atRow200 = ts12Instant(200, unit);
        instantCases(cases, column, atRow200);
        cases.add(new Case(column, "notEq(" + atRow200 + ")", FilterPredicate.notEq(column, atRow200),
                matching(notEq(atRow200))));
        cases.add(new Case(column, "ltEq(" + atRow200 + ")", FilterPredicate.ltEq(column, atRow200),
                matching(cmp(Operator.LT_EQ, atRow200))));
        cases.add(new Case(column, "gt(" + atRow200 + ")", FilterPredicate.gt(column, atRow200),
                matching(cmp(Operator.GT, atRow200))));
        cases.add(new Case(column, "lt(the epoch)", FilterPredicate.lt(column, Instant.EPOCH),
                matching(cmp(Operator.LT, Instant.EPOCH))));
        Instant year9999 = ts12Instant(6, unit);
        cases.add(new Case(column, "gtEq(the end of year 9999)", FilterPredicate.gtEq(column, year9999),
                matching(cmp(Operator.GT_EQ, year9999))));
        Instant bcYear = ts12Instant(40, unit);
        cases.add(new Case(column, "in(row 40, row 200)", FilterPredicate.in(column, bcYear, atRow200),
                matching(oneOf(bcYear, atRow200))));

        String description = "annotated TIMESTAMP(" + unit + ", UTC)";
        storedByteCases(cases, column, ts12Bytes(ts12Count(200, unit)));
        cases.add(new Case(column, "lt(the bytes of row 200)",
                binary(column, Operator.LT, ts12Bytes(ts12Count(200, unit))),
                new Rejected(notByteOrdered(column, description, "an Instant"))));
        String wrongWidth = "Column '" + column + "' is a FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP, whose literal is "
                + "12 bytes, not 11";
        cases.add(new Case(column, "eq(an eleven-byte literal)", binary(column, Operator.EQ, new byte[11]),
                new Rejected(wrongWidth)));
        cases.add(new Case(column, "in(an eleven-byte literal)", FilterPredicate.in(column, new byte[11]),
                new Rejected(wrongWidth)));
        cases.add(new Case(column, "gt(an eleven-byte literal)", binary(column, Operator.GT, new byte[11]),
                new Rejected(notByteOrdered(column, description, "an Instant"))));
        cases.add(new Case(column, "eq(a long)", FilterPredicate.eq(column, 0L),
                new Rejected("Column '" + column + "' is " + description
                        + ", which takes Instant and byte[] literals, not a long")));
        cases.add(new Case(column, "eq(a LocalDateTime), a UTC timestamp",
                FilterPredicate.eq(column, LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)),
                new Rejected("Column '" + column + "' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true),"
                        + " which takes Instant and byte[] literals, not a LocalDateTime")));

        // Every Instant is a count of nanoseconds the twelve bytes hold.
        cases.add(new Case(column, "eq(Instant.MAX)", FilterPredicate.eq(column, Instant.MAX),
                unit == LogicalType.TimeUnit.NANOS
                        ? matching(never())
                        : new Rejected("Column '" + column + "' holds a whole number of milliseconds; the equality "
                                + "literal +1000000000-12-31T23:59:59.999999999Z is not a value it can hold")));
        cases.add(new Case(column, "lt(Instant.MAX)", FilterPredicate.lt(column, Instant.MAX),
                matching(everyNonNullRow())));
        cases.add(new Case(column, "not(gtEq(Instant.MIN))",
                FilterPredicate.not(FilterPredicate.gtEq(column, Instant.MIN)), matching(never())));
        if (unit != LogicalType.TimeUnit.NANOS) {
            // Half a millisecond past row 200: no value the column holds is equal, and the ordered
            // operators move to the neighbouring whole millisecond.
            Instant between = atRow200.plusNanos(500_000);
            cases.add(new Case(column, "eq(between two milliseconds)", FilterPredicate.eq(column, between),
                    new Rejected("Column '" + column + "' holds a whole number of milliseconds; the equality "
                            + "literal " + between + " is not a value it can hold")));
            cases.add(new Case(column, "gt(between two milliseconds)", FilterPredicate.gt(column, between),
                    matching(cmp(Operator.GT, between))));
            cases.add(new Case(column, "ltEq(between two milliseconds)", FilterPredicate.ltEq(column, between),
                    matching(cmp(Operator.LT_EQ, between))));
        }
    }

    /// A local `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, whose literal is a [LocalDateTime].
    private static void ts12LocalCases(List<Case> cases, String column) {
        Instant atRow200 = ts12Instant(200, LogicalType.TimeUnit.MICROS);
        localDateTimeCases(cases, column, LocalDateTime.ofInstant(atRow200, ZoneOffset.UTC));
        LocalDateTime beforeEpoch = LocalDateTime.ofInstant(ts12Instant(3, LogicalType.TimeUnit.MICROS), ZoneOffset.UTC);
        cases.add(new Case(column, "gtEq(the microsecond before the epoch)", FilterPredicate.gtEq(column, beforeEpoch),
                matching(cmp(Operator.GT_EQ, beforeEpoch))));
        storedByteCases(cases, column, ts12Bytes(ts12Count(200, LogicalType.TimeUnit.MICROS)));
        cases.add(new Case(column, "gtEq(the bytes of row 200)",
                binary(column, Operator.GT_EQ, ts12Bytes(ts12Count(200, LogicalType.TimeUnit.MICROS))),
                new Rejected(notByteOrdered(column, "annotated TIMESTAMP(MICROS, local)", "a LocalDateTime"))));
        cases.add(new Case(column, "eq(an Instant), a local timestamp", FilterPredicate.eq(column, Instant.EPOCH),
                new Rejected("Column '" + column + "' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false),"
                        + " which takes LocalDateTime and byte[] literals, not an Instant")));
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

    /// A constant predicate inside a conjunction, negated. `gtEq("ts_us_utc", Instant.MIN)` matches
    /// every non-null row, since the column holds nothing earlier; negating the conjunction pushes
    /// its negation down to the leaves, and the branch that comes back must match no row rather
    /// than the rows `ts_us_utc` is null on.
    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("flatLayoutsAndPaths")
    void negatingAConstantInsideAConjunctionKeepsNullsOut(Layout layout, ReadPath path) throws Exception {
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.gtEq("ts_us_utc", Instant.MIN),
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
        return Stream.of(SINGLE, MULTI, DICTIONARY, BLOOM)
                .flatMap(layout -> Stream.of(ReadPath.values()).map(path -> Arguments.of(layout, path)));
    }

    /// `i32_req` is the only column whose row groups the harness sees decided as matching in full,
    /// which skips per-row evaluation. This pins that the multi-row-group layout keeps giving it such
    /// row groups, and that `i32`, holding the same values beside nulls, gets none.
    @Test
    void theMultiLayoutHoldsRowGroupsThatMatchInFull() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MULTI.path()), context)) {
            assertThat(rowGroupDecisions(reader, FilterPredicate.lt("i32_req", 0)))
                    .containsExactly(ALWAYS_MATCHES, ALWAYS_MATCHES, CANNOT_MATCH, CANNOT_MATCH);
            assertThat(rowGroupDecisions(reader, FilterPredicate.lt("i32", 0)))
                    .containsExactly(MIGHT_MATCH, MIGHT_MATCH, CANNOT_MATCH, CANNOT_MATCH);
        }
    }

    private static List<FilterDecision> rowGroupDecisions(ParquetFileReader reader, FilterPredicate predicate)
            throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(predicate, reader.getFileSchema());
        List<FilterDecision> decisions = new ArrayList<>();
        for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
            decisions.add(RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null,
                    new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP), BoundsReadability.ALL));
        }
        return decisions;
    }

    /// Every value the oracle reads is the value the fixture generator wrote, through the logical
    /// and the physical accessor alike.
    @ParameterizedTest(name = "{0}")
    @MethodSource("layouts")
    void readsTheValuesTheFixtureHolds(Layout layout) {
        Map<String, List<String[]>> written = writtenValues(layout);
        List<String> differences = new ArrayList<>();
        for (Map.Entry<String, List<String[]>> column : written.entrySet()) {
            List<Object[]> read = values(layout, column.getKey());
            assertThat(read).as("%s: rows of '%s'", layout, column.getKey()).hasSameSizeAs(column.getValue());
            for (int row = 0; row < read.size(); row++) {
                String[] expected = column.getValue().get(row);
                String logical = rendered(read.get(row)[0]);
                String physical = rendered(read.get(row)[1]);
                if (!logical.equals(expected[0]) || !physical.equals(expected[1])) {
                    differences.add("row " + row + ", " + column.getKey() + ": read " + logical + " / " + physical
                            + ", written " + expected[0] + " / " + expected[1]);
                }
            }
        }
        assertThat(differences).as("%s", layout).isEmpty();
    }

    static Stream<Layout> layouts() {
        return LAYOUTS.stream();
    }

    /// The values `tools/simple-datagen.py` wrote to the layout's corpus, by column, each as its
    /// logical and physical rendering, indexed by row.
    private static Map<String, List<String[]>> writtenValues(Layout layout) {
        return WRITTEN.computeIfAbsent(corpus(layout), corpus -> {
            try {
                return writtenValues(corpus);
            }
            catch (IOException e) {
                throw new IllegalStateException("Cannot read " + corpus + ".values.tsv.gz", e);
            }
        });
    }

    private static Map<String, List<String[]>> writtenValues(String corpus) throws IOException {
        Map<String, List<String[]>> columns = new LinkedHashMap<>();
        for (String line : writtenLines(RES.resolve(corpus + ".values.tsv.gz"))) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("\t", -1);
            List<String[]> rows = columns.computeIfAbsent(fields[1], key -> new ArrayList<>());
            if (Integer.parseInt(fields[0]) != rows.size()) {
                throw new IllegalStateException(corpus + ".values.tsv.gz: row " + fields[0] + " of '" + fields[1]
                        + "' out of order");
            }
            rows.add(new String[] { fields[2], fields[3] });
        }
        return columns;
    }

    private static String corpus(Layout layout) {
        return layout.path().getFileName().toString().replaceFirst("_(single|multi|dict|bloom|pages)\\.parquet$", "");
    }

    private static List<String> writtenLines(Path sidecar) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(sidecar)), StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    /// A value in the form `tools/simple-datagen.py` writes it: tagged with its type, and reduced to
    /// integers, raw bits or hex without decoding anything.
    private static String rendered(Object value) {
        return switch (value) {
            case null -> "null";
            case Boolean v -> "boolean:" + v;
            case Byte v -> "byte:" + v;
            case Integer v -> "int:" + v;
            case Long v -> "long:" + v;
            case Float v -> "float:" + HexFormat.of().toHexDigits(Float.floatToRawIntBits(v));
            case Double v -> "double:" + HexFormat.of().toHexDigits(Double.doubleToRawLongBits(v));
            case byte[] v -> "bytes:" + HexFormat.of().formatHex(v);
            case String v -> "string:" + HexFormat.of().formatHex(v.getBytes(StandardCharsets.UTF_8));
            case LocalDate v -> "date:" + v.toEpochDay();
            case LocalTime v -> "time:" + v.toNanoOfDay();
            case Instant v -> "instant:" + v.getEpochSecond() + ":" + v.getNano();
            case LocalDateTime v -> "datetime:" + v.toEpochSecond(ZoneOffset.UTC) + ":" + v.getNano();
            case BigDecimal v -> "decimal:" + v.unscaledValue() + ":" + v.scale();
            case UUID v -> "uuid:" + v;
            case PqInterval v -> "interval:" + v.months() + ":" + v.days() + ":" + v.milliseconds();
            default -> throw new IllegalArgumentException("No rendering for a " + value.getClass().getName());
        };
    }

    // ==================== Reading ====================

    private static List<Long> read(Layout layout, FilterPredicate predicate, ReadPath path) throws IOException {
        FilterPredicate filter = path.forceRecordPath ? FilterPredicate.or(predicate, NEVER) : predicate;
        ReaderConfig config = path.metadataFiltering ? ReaderConfig.defaults() : METADATA_FILTERING_OFF;
        List<Long> rows = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(layout.path()), context, config)) {
            if (path.forceRecordPath) {
                assertTakesRecordPath(filter, reader.getFileSchema());
            }
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

    /// The values of `column`, which must be among those [#readsTheValuesTheFixtureHolds] checks.
    private static List<Object[]> values(Layout layout, String column) {
        if (!writtenValues(layout).containsKey(column)) {
            throw new IllegalStateException(corpus(layout) + ".values.tsv.gz holds no values of '" + column
                    + "'; add the column to the corpus's _pv_write call in tools/simple-datagen.py");
        }
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
            case byte[] l -> compareBytes((byte[]) value, l);
            case LocalDate l -> ((LocalDate) value).compareTo(l);
            case Instant l -> ((Instant) value).compareTo(l);
            case LocalDateTime l -> ((LocalDateTime) value).compareTo(l);
            case LocalTime l -> ((LocalTime) value).compareTo(l);
            case BigDecimal l -> ((BigDecimal) value).compareTo(l);
            case UUID l -> Arrays.compareUnsigned(uuidBytes((UUID) value), uuidBytes(l));
            default -> throw new IllegalArgumentException("No rule for a literal of type "
                    + literal.getClass().getName());
        };
    }

    /// A byte literal is the stored bytes. The ordered cases reach this only on a column whose
    /// type orders as its bytes.
    private static int compareBytes(byte[] value, byte[] literal) {
        return Arrays.compareUnsigned(value, literal);
    }

    private static boolean unsigned(ColumnSchema column) {
        return column.logicalType() instanceof LogicalType.IntType intType && !intType.isSigned();
    }

    private static byte[] utf8(Object value) {
        return value instanceof String text ? text.getBytes(StandardCharsets.UTF_8) : (byte[]) value;
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
    private static byte[] intervalBytes(PqInterval interval) {
        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(Math.toIntExact(interval.months()))
                .putInt(Math.toIntExact(interval.days()))
                .putInt(Math.toIntExact(interval.milliseconds()));
        return bytes;
    }

    /// The row `predicate_int96` stores under a non-canonical encoding.
    private static final int NON_CANONICAL_ROW = 250;

    private static final long NANOS_PER_DAY = 86_400_000_000_000L;
    private static final long JULIAN_DAY_OF_EPOCH = 2_440_588L;

    /// The `ts96` column's value in `row`, as the fixture writes it; the three special rows
    /// before 200 stay clear of the cases.
    private static Instant int96Instant(int row) {
        return Instant.ofEpochSecond(0, 1_700_000_000_000_000_000L + (row - 200) * 3_600_000_000_123L + row);
    }

    /// The canonical twelve bytes of `instant`, whose nanoseconds of the day are less than a day.
    private static byte[] int96Bytes(Instant instant) {
        long nanos = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(Math.floorMod(nanos, NANOS_PER_DAY))
                .putInt(Math.toIntExact(Math.floorDiv(nanos, NANOS_PER_DAY) + JULIAN_DAY_OF_EPOCH))
                .array();
    }

    /// The twelve bytes the fixture stores for row 250: its Julian day one lower and its
    /// nanoseconds of the day one day longer.
    private static byte[] nonCanonicalInt96Bytes(Instant instant) {
        ByteBuffer canonical = ByteBuffer.wrap(int96Bytes(instant)).order(ByteOrder.LITTLE_ENDIAN);
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(canonical.getLong() + NANOS_PER_DAY)
                .putInt(canonical.getInt() - 1)
                .array();
    }

    /// Fifty years of 365.25 days, in nanoseconds: the step between the rows of `predicate_ts12`.
    private static final BigInteger TS12_STEP_NANOS = BigInteger.valueOf(50L * 31_557_600L * 1_000_000_000L);

    /// The count `predicate_ts12` stores in `row` of the column counting `unit`, as the fixture writes
    /// it: the nanosecond count floored to the unit.
    private static BigInteger ts12Count(int row, LogicalType.TimeUnit unit) {
        BigInteger nanos = switch (row) {
            case 3 -> BigInteger.valueOf(-1);
            case 6 -> BigInteger.valueOf(253_402_300_799L).multiply(BigInteger.TEN.pow(9))
                    .add(BigInteger.valueOf(999_999_999));
            default -> BigInteger.valueOf(row - 200).multiply(TS12_STEP_NANOS).add(BigInteger.valueOf(row * 1_000_001L));
        };
        BigInteger perUnit = BigInteger.valueOf(switch (unit) {
            case MILLIS -> 1_000_000L;
            case MICROS -> 1_000L;
            case NANOS -> 1L;
        });
        BigInteger[] quotientAndRemainder = nanos.divideAndRemainder(perUnit);
        return quotientAndRemainder[1].signum() < 0
                ? quotientAndRemainder[0].subtract(BigInteger.ONE)
                : quotientAndRemainder[0];
    }

    /// The instant `row` of the column counting `unit` stands for.
    private static Instant ts12Instant(int row, LogicalType.TimeUnit unit) {
        BigInteger[] secondsAndNanos = ts12Count(row, unit).multiply(BigInteger.valueOf(switch (unit) {
            case MILLIS -> 1_000_000L;
            case MICROS -> 1_000L;
            case NANOS -> 1L;
        })).divideAndRemainder(BigInteger.TEN.pow(9));
        long seconds = secondsAndNanos[0].longValueExact();
        long nanos = secondsAndNanos[1].longValueExact();
        return nanos < 0 ? Instant.ofEpochSecond(seconds - 1, nanos + 1_000_000_000L) : Instant.ofEpochSecond(seconds, nanos);
    }

    /// The twelve bytes of `count`: two's complement, least significant byte first.
    private static byte[] ts12Bytes(BigInteger count) {
        byte[] bigEndian = count.toByteArray();
        byte[] bytes = new byte[12];
        Arrays.fill(bytes, (byte) (count.signum() < 0 ? 0xFF : 0));
        for (int i = 0; i < bigEndian.length; i++) {
            bytes[i] = bigEndian[bigEndian.length - 1 - i];
        }
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

    /// Fails unless `filter` over `schema` falls back to the record-level filter. Which leaves the
    /// batch compiler supports changes over time, so a test that relies on the record path asserts
    /// it rather than trusting the filter's shape.
    private static void assertTakesRecordPath(FilterPredicate filter, FileSchema schema) {
        ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());
        assertThat(BatchFilterCompiler.tryCompile(FilterPredicateResolver.resolve(filter, schema), schema,
                projected::toProjectedIndex))
                .as("filter %s must take the record path", filter)
                .isNull();
    }
}
