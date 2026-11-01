/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.row.FlatRowReader;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Benchmark for record-level filtering overhead.
///
/// Compares RowReader performance across:
/// - **Baseline**: no filter at all (raw scan throughput).
/// - **Match-all / compound match-all**: predicates that keep every row — worst case
///   overhead because the filter is evaluated for each row but never prunes.
/// - **Selective**: predicates that drop most rows — real-world wins.
/// - **Batch-eligible compound ANDs** (2/3/4 leaves across `id`, `value`, `tag`, `flag`):
///   exercise the column-local AND fast path.
/// - **Fallback shapes** (single-leaf, OR, same-column range, IN-list): trip the
///   batch-eligibility gate so the record matcher handles them. See [BatchFilterCompiler].
/// - **Page+record**: id range that prunes ~99% of pages via column-index min/max,
///   then a per-row `value<500` filter on the survivors.
///
/// - **Binary**: `eq`, range, compound, `inStrings` and negated `inStrings` leaves on the
///   `category` string column, which the byte-array matchers decide by comparing each value's
///   bytes in place; and `eq`, `inStrings` and negated `inStrings` on two columns whose values are
///   all longer than eight bytes.
///
/// Schema: `id` (long, sequential 0..N), `value` (double uniform 0..1000),
/// `tag` (int uniform 0..99), `flag` (boolean uniform),
/// `category` (string uniform over `cat_00`..`cat_99`),
/// `url_tail` / `url_head` (the category spelled in 32 bytes, last or first),
/// `amount_fixed` (`DECIMAL(18, 2)` over `FIXED_LEN_BYTE_ARRAY(8)`),
/// `amount_var` (`DECIMAL(18, 2)` over `BYTE_ARRAY`).
///
/// The two decimal columns hold the same numbers and separate the byte-array matchers' two signed
/// modes. `amount_fixed` pads every value to the column width, so a comparison sees equal widths
/// and equality may test bytes. `amount_var` stores each value in the fewest bytes that hold it, so
/// widths differ per row, ordering has to sign-extend, and equality has to compare values — the
/// mode where a padded spelling of a number is still that number.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="RecordFilterBenchmarkTest" -Dperf.runs=10
///
/// [#compareStructFilterOverhead] runs the filters on the leaves of a struct over a file of its
/// own; run it alone with `-Dtest="RecordFilterBenchmarkTest#compareStructFilterOverhead"`.
class RecordFilterBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/record_filter_benchmark_with_long_strings.parquet");
    private static final int CATEGORY_COUNT = 100;
    /// Width of `amount_fixed`, enough for `DECIMAL(18, 2)`.
    private static final int DECIMAL_BYTES = 8;
    private static final int DECIMAL_PRECISION = 18;
    private static final int DECIMAL_SCALE = 2;
    /// Unscaled values run `-500_000 .. 500_000` cyclically, so the sign boundary falls mid-column
    /// and a signed comparison is the only one that orders them correctly.
    private static final int DECIMAL_SPAN = 1_000_001;
    private static final int DECIMAL_OFFSET = 500_000;
    /// Half the span sorts below zero, so the range contenders keep ~50% of rows.
    private static final BigDecimal DECIMAL_MID = BigDecimal.ZERO.setScale(DECIMAL_SCALE);
    /// One unscaled value out of the span — matches the ~10 rows per value at 10M rows.
    private static final BigDecimal DECIMAL_EQ =
            BigDecimal.valueOf(123_45, DECIMAL_SCALE);
    /// Five values of the span, negative and positive, one to three bytes in minimal encoding.
    private static final BigDecimal[] DECIMAL_IN = {BigDecimal.valueOf(123_45, DECIMAL_SCALE),
            BigDecimal.valueOf(-123_45, DECIMAL_SCALE), BigDecimal.valueOf(1, DECIMAL_SCALE),
            BigDecimal.valueOf(-4_000_00, DECIMAL_SCALE), BigDecimal.valueOf(4_999_99, DECIMAL_SCALE)};
    /// Written through Avro's own decimal encoding: padded to the width for `fixed`, minimal for `bytes`.
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();
    /// What every contender reads besides its filter's columns, and all the no-filter baseline reads.
    /// Fixing it keeps a contender's numbers comparable when the file gains a column.
    private static final String[] BASE_COLUMNS = {"id", "value", "tag", "flag"};
    /// Its own stream, so adding `category` left the other columns' values as they were.
    private static final int CATEGORY_SEED = 43;
    private static final String[] CATEGORIES = categories();
    /// One of the 100 categories — ~1% of rows.
    private static final String EQ_CATEGORY = CATEGORIES[42];
    /// Splits the pool in half, so the range contender keeps ~50% of rows. Zero-padded names sort
    /// in the same order as their numbers.
    private static final String RANGE_CATEGORY = CATEGORIES[CATEGORY_COUNT / 2];
    /// Five of the 100 categories, for the `inStrings` contender.
    private static final String[] IN_CATEGORIES = {
            CATEGORIES[1], CATEGORIES[5], CATEGORIES[10], CATEGORIES[25], CATEGORIES[50]};
    /// Three of the 100 categories — excluding them keeps ~97% of rows.
    private static final String[] NOT_IN_CATEGORIES = {CATEGORIES[7], CATEGORIES[42], CATEGORIES[91]};
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 10;
    private static final int CATEGORY_EQ_WARMUPS = 3;

    private static final String PATH_BATCH = "(Batch Matcher Filtration)";
    private static final String PATH_CONSUMER = "(Consumer Side Filtration)";
    private static final String PATH_NONE = "";
    private static final String PATH_NESTED = "(Nested Row Reader)";

    private static final Path STRUCT_FILE = Path.of("target/record_filter_benchmark_structs.parquet");
    /// Rows whose struct `s` is null.
    private static final double STRUCT_NULL_RATE = 0.05;
    /// Rows of a present `s` whose `s.amount` is null.
    private static final double AMOUNT_NULL_RATE = 0.10;
    /// What a struct contender reads: the struct, whose `amount` it reads per row, and `id`.
    private static final String[] STRUCT_COLUMNS = {"id", "s"};
    private static final int STRUCT_WARMUPS = 1;

    private record Run(long[] times, long[] rows, String path) {}

    @Test
    void compareRecordFilterOverhead() throws Exception {
        ensureBenchmarkFileExists();

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== Record Filter Benchmark ===");
        System.out.println("File: " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
        System.out.println("Total rows: " + String.format("%,d", TOTAL_ROWS));
        System.out.println("Runs per contender: " + runs);

        // Warmup
        System.out.println("\nWarmup...");
        runNoFilter();

        // ----- Baseline ---------------------
        Run noFilter = timeNoFilter(runs);

        Run matchAll = timeFilter(
                // id >= 0 matches every row — worst case for per-row evaluation overhead.
                FilterPredicate.gtEq("id", 0L),
                runs);

        Run selective = timeFilter(
                // id < 1% of range — should return ~100K rows out of 10M.
                FilterPredicate.lt("id", (long) (TOTAL_ROWS / 100)),
                runs);

        Run compound = timeFilter(
                // Two-leaf AND that matches every row — exercises tree recursion + per-leaf dispatch.
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run pageRecord = timeFilter(
                // id BETWEEN 9.9M and 10M — only the last few data pages overlap, so
                // column-index min/max should drop ~99% of pages before any row is decoded.
                // Then `value<500` runs as a per-row filter on the surviving ~100K rows.
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100)),
                        FilterPredicate.lt("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100) + (TOTAL_ROWS / 100)),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run and3 = timeFilter(
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE),
                        FilterPredicate.gtEq("tag", 0)),
                runs);

        Run and4 = timeFilter(
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE),
                        FilterPredicate.gtEq("tag", 0),
                        FilterPredicate.notEq("flag", false)),
                runs);

        Run compoundSelective = timeFilter(
                FilterPredicate.and(
                        FilterPredicate.lt("id", 10_000L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run compoundMid = timeFilter(
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run sortedCluster = timeFilter(
                // ~50% selectivity but on a **sorted** column — every batch's bitset
                // is one long run of 1s followed by 0s (or all 0s after id crosses
                // TOTAL/2). Pairs with `compoundMid` to isolate the run-structure
                // effect: same batch-matcher shape, same selectivity, very different runs.
                FilterPredicate.and(
                        FilterPredicate.lt("id", (long) (TOTAL_ROWS / 2)),
                        FilterPredicate.gtEq("tag", 0)),
                runs);

        Run empty = timeFilter(
                FilterPredicate.and(
                        FilterPredicate.lt("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run orFilter = timeFilter(
                FilterPredicate.or(
                        FilterPredicate.lt("id", 0L),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run mixedAndOr = timeFilter(
                // Outer AND narrows by `flag`, inner OR picks rows from either
                // end of the `id`/`tag` distributions — three distinct columns
                // so all branches stay batch-eligible. Roughly:
                //   flag!=false: ~50%
                //   (id<N/4 OR tag<25): id<N/4 is exactly 25%, tag<25 ~25% uniform,
                //                       independent OR ≈ 43.75%
                //   combined AND: ~22%
                FilterPredicate.and(
                        FilterPredicate.notEq("flag", false),
                        FilterPredicate.or(
                                FilterPredicate.lt("id", (long) (TOTAL_ROWS / 4)),
                                FilterPredicate.lt("tag", 25))),
                runs);

        Run rangeDup = timeFilter(
                // Same-column range on `id` is not batch-eligible (duplicate column).
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 1_000_000L),
                        FilterPredicate.lt("id", 2_000_000L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run intIn = timeFilter(
                FilterPredicate.in("tag", new int[] {1, 5, 10, 25, 50}),
                runs);

        Run binaryRange = timeFilter(
                FilterPredicate.lt("category", RANGE_CATEGORY),
                runs);

        Run binaryCompound = timeFilter(
                // Two independent ~50% leaves on distinct columns — ~25%.
                FilterPredicate.and(
                        FilterPredicate.lt("value", 500.0),
                        FilterPredicate.lt("category", RANGE_CATEGORY)),
                runs);

        Run binaryIn = timeFilter(
                // 5 of 100 categories — ~5% kept, so most rows scan every member before missing.
                FilterPredicate.inStrings("category", IN_CATEGORIES),
                runs);

        Run notBinaryIn = timeFilter(
                // Negation expands to an AND of `notEq` leaves on one column — 3 of 100 categories
                // out, ~97% kept.
                FilterPredicate.not(FilterPredicate.inStrings("category", NOT_IN_CATEGORIES)),
                runs);

        // The same three filters over 32-byte spellings of the category, so every literal and every
        // value is longer than eight bytes. `url_tail` shares its first eight bytes across all rows;
        // `url_head` differs within them.
        EqualityRuns urlTail = timeEqualityShapes("url_tail", RecordFilterBenchmarkTest::urlTail, runs);
        EqualityRuns urlHead = timeEqualityShapes("url_head", RecordFilterBenchmarkTest::urlHead, runs);

        Run fixedDecimalRange = timeFilter(
                // Equal widths throughout, so the sign byte decides and the rest compares unsigned.
                FilterPredicate.lt("amount_fixed", DECIMAL_MID),
                runs);

        Run fixedDecimalEq = timeFilter(
                // Byte equality is sound here: one spelling per number in a fixed-width column.
                FilterPredicate.eq("amount_fixed", DECIMAL_EQ),
                runs);

        Run variableDecimalRange = timeFilter(
                // Widths differ per row, so every comparison sign-extends the shorter side first.
                FilterPredicate.lt("amount_var", DECIMAL_MID),
                runs);

        Run variableDecimalEq = timeFilter(
                // Equality cannot test bytes here — it compares the value the bytes stand for.
                FilterPredicate.eq("amount_var", DECIMAL_EQ),
                runs);

        Run fixedDecimalIn = timeFilter(
                // Padded to the column width, so membership is byte equality.
                FilterPredicate.in("amount_fixed", paddedDecimalLiterals()),
                runs);

        Run variableDecimalIn = timeFilter(
                // Not byte equality: every row compares each member by value, sign-extending first.
                FilterPredicate.in("amount_var", decimalLiterals()),
                runs);

        // Category EQ is the first contender that exercises both the dictionary
        // wrapper and the short-value equality matcher. Run it last, with
        // dedicated unmeasured warmups, so class loading and JIT compilation do
        // not become part of its first measured samples.
        Run binaryEq = timeFilter(
                FilterPredicate.eq("category", EQ_CATEGORY),
                runs, CATEGORY_EQ_WARMUPS);

        // ----- Print results ------------------------------------------------
        System.out.println("\nResults:");
        System.out.printf("  %-50s %-26s %10s %15s %12s%n",
                "Contender", "Path", "Time (ms)", "Rows", "Records/sec");
        System.out.println("  " + "-".repeat(117));

        printResults("No filter (baseline)", noFilter, runs);
        System.out.println();
        printResults("Match-all (id>=0)", matchAll, runs);
        System.out.println();
        printResults("Selective (id<1%)", selective, runs);
        System.out.println();
        printResults("Compound match-all (id>=0 AND value<+inf)", compound, runs);
        System.out.println();
        printResults("Page+record (id top 1% AND value<500)", pageRecord, runs);
        System.out.println();
        printResults("And3 match-all (id+value+tag)", and3, runs);
        System.out.println();
        printResults("And4 ~50% (id+value+tag+!flag)", and4, runs);
        System.out.println();
        printResults("Compound selective (id<10K AND value<+inf)", compoundSelective, runs);
        System.out.println();
        printResults("Compound mid 50% (id>=0 AND value<500)", compoundMid, runs);
        System.out.println();
        printResults("Sorted cluster 50% (id<N/2 AND tag>=0)", sortedCluster, runs);
        System.out.println();
        printResults("Empty result (id<0 AND value<+inf)", empty, runs);
        System.out.println();
        printResults("OR (id<0 OR value<500)", orFilter, runs);
        System.out.println();
        printResults("Mixed AND/OR (flag!=false AND (id<N/4 OR tag<25))", mixedAndOr, runs);
        System.out.println();
        printResults("Range+value (id BETWEEN 1M..2M)", rangeDup, runs);
        System.out.println();
        printResults("intIn (tag IN [1,5,10,25,50])", intIn, runs);
        System.out.println();
        printResults("Binary eq (category=" + EQ_CATEGORY + ")", binaryEq, runs);
        System.out.println();
        printResults("Binary range (category<" + RANGE_CATEGORY + ")", binaryRange, runs);
        System.out.println();
        printResults("Binary compound (value<500 AND category<" + RANGE_CATEGORY + ")", binaryCompound, runs);
        System.out.println();
        printResults("binaryIn (category IN " + IN_CATEGORIES.length + " values)", binaryIn, runs);
        System.out.println();
        printResults("Not binary in (category NOT IN " + Arrays.toString(NOT_IN_CATEGORIES) + ")",
                notBinaryIn, runs);
        System.out.println();
        printEqualityShapes("url_tail", urlTail, runs);
        printEqualityShapes("url_head", urlHead, runs);
        printResults("Fixed decimal range (amount_fixed<0)", fixedDecimalRange, runs);
        System.out.println();
        printResults("Fixed decimal eq (amount_fixed=" + DECIMAL_EQ + ")", fixedDecimalEq, runs);
        System.out.println();
        printResults("Variable decimal range (amount_var<0)", variableDecimalRange, runs);
        System.out.println();
        printResults("Variable decimal eq (amount_var=" + DECIMAL_EQ + ")", variableDecimalEq, runs);
        System.out.println();
        printResults("Fixed decimal IN (amount_fixed, " + DECIMAL_IN.length + " values)", fixedDecimalIn, runs);
        System.out.println();
        printResults("Variable decimal IN (amount_var, " + DECIMAL_IN.length + " values)", variableDecimalIn, runs);

        // ----- Derived ratios vs no-filter baseline -------------------------
        double avgNoFilter = avg(noFilter.times) / 1_000_000.0;
        double avgMatchAll = avg(matchAll.times) / 1_000_000.0;
        double avgSelective = avg(selective.times) / 1_000_000.0;
        double avgCompound = avg(compound.times) / 1_000_000.0;
        double avgPageRecord = avg(pageRecord.times) / 1_000_000.0;

        System.out.printf("%n  Match-all overhead: %.1f%% (%.0f ms → %.0f ms)%n",
                100.0 * (avgMatchAll - avgNoFilter) / avgNoFilter, avgNoFilter, avgMatchAll);
        System.out.printf("  Selective speedup: %.1fx (%.0f ms → %.0f ms)%n",
                avgNoFilter / avgSelective, avgNoFilter, avgSelective);
        System.out.printf("  Compound overhead: %.1f%% (%.0f ms → %.0f ms)%n",
                100.0 * (avgCompound - avgNoFilter) / avgNoFilter, avgNoFilter, avgCompound);
        System.out.printf("  Page+record speedup: %.1fx (%.0f ms → %.1f ms)%n",
                avgNoFilter / avgPageRecord, avgNoFilter, avgPageRecord);

        // ----- Correctness --------------------------------------------------
        assertThat(noFilter.rows[0]).isEqualTo(TOTAL_ROWS);
        assertThat(matchAll.rows[0]).isEqualTo(TOTAL_ROWS);
        assertThat(selective.rows[0]).isEqualTo(TOTAL_ROWS / 100L);
        assertThat(compound.rows[0]).isEqualTo(TOTAL_ROWS);
        // Page+record: id range narrows to ~100K rows, value<500 keeps roughly half.
        assertThat(pageRecord.rows[0]).isGreaterThan(0L).isLessThan(TOTAL_ROWS / 50L);
        assertThat(and3.rows[0]).isEqualTo(TOTAL_ROWS);
        // and4 with `flag != false` is uniform-random, expect ~50%.
        assertThat(and4.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        assertThat(compoundSelective.rows[0]).isEqualTo(10_000L);
        // Compound mid: value uniform [0, 1000) so value<500 keeps ~50%.
        assertThat(compoundMid.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        // Sorted cluster: id<N/2 is exactly N/2 rows, tag>=0 always true.
        assertThat(sortedCluster.rows[0]).isEqualTo(TOTAL_ROWS / 2L);
        assertThat(empty.rows[0]).isEqualTo(0L);
        // OR: id<0 is empty so the result is the value<500 half.
        assertThat(orFilter.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        // Mixed AND/OR: expected ~22%; allow 15-30% to absorb tag's random jitter.
        assertThat(mixedAndOr.rows[0]).isBetween((long) (TOTAL_ROWS * 0.15), (long) (TOTAL_ROWS * 0.30));
        assertThat(rangeDup.rows[0]).isEqualTo(1_000_000L);
        // intIn: 5 of 100 values, expect ~5%.
        assertThat(intIn.rows[0]).isBetween((long) (TOTAL_ROWS * 0.03), (long) (TOTAL_ROWS * 0.07));
        // Binary eq: 1 of 100 categories, expect ~1%.
        assertThat(binaryEq.rows[0]).isBetween((long) (TOTAL_ROWS * 0.005), (long) (TOTAL_ROWS * 0.015));
        // Both decimal columns hold the same numbers, so the two encodings must agree row for row —
        // which is the point of running them as a pair rather than measuring either alone.
        assertThat(fixedDecimalRange.rows[0])
                .isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6))
                .isEqualTo(variableDecimalRange.rows[0]);
        assertThat(fixedDecimalEq.rows[0]).isPositive().isEqualTo(variableDecimalEq.rows[0]);
        assertThat(fixedDecimalIn.rows[0]).isPositive().isEqualTo(variableDecimalIn.rows[0]);
        assertThat(fixedDecimalIn.path()).isEqualTo(PATH_BATCH);
        assertThat(variableDecimalIn.path()).isEqualTo(PATH_BATCH);
        // Binary range: half the pool sorts below the literal.
        assertThat(binaryRange.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        // Binary compound: two independent ~50% leaves.
        assertThat(binaryCompound.rows[0]).isBetween((long) (TOTAL_ROWS * 0.2), (long) (TOTAL_ROWS * 0.3));
        // binaryIn: 5 of 100 categories, expect ~5%, and it must stay on the batch matchers.
        assertThat(binaryIn.rows[0]).isBetween((long) (TOTAL_ROWS * 0.035), (long) (TOTAL_ROWS * 0.065));
        assertThat(binaryIn.path()).isEqualTo(PATH_BATCH);
        // Not binary in: the complement of three ~1% categories, and it must stay on the batch matchers.
        assertThat(notBinaryIn.rows[0]).isBetween((long) (TOTAL_ROWS * 0.955), (long) (TOTAL_ROWS * 0.985));
        assertThat(notBinaryIn.path()).isEqualTo(PATH_BATCH);
        // The long spellings are one-to-one with the categories, so they keep the same rows.
        for (EqualityRuns longRuns : List.of(urlTail, urlHead)) {
            assertThat(longRuns.eq().rows[0]).isEqualTo(binaryEq.rows[0]);
            assertThat(longRuns.in().rows[0]).isEqualTo(binaryIn.rows[0]);
            assertThat(longRuns.notIn().rows[0]).isEqualTo(notBinaryIn.rows[0]);
            assertThat(longRuns.in().path()).isEqualTo(PATH_BATCH);
        }
    }

    /// Filters on the leaves of an optional struct `s` (`code` int 0..99, `amount` optional double
    /// 0..1000, `cat` string over the 100 categories; `s` null on ~5% of rows), with `id` and the
    /// top-level `value` (double 0..1000) beside it. Every contender but the filter-only one reads
    /// `s` and its `amount` per row, as a caller of the struct does.
    @Test
    void compareStructFilterOverhead() throws Exception {
        ensureStructFileExists();

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== Struct Filter Benchmark ===");
        System.out.println("File: " + STRUCT_FILE + " (" + Files.size(STRUCT_FILE) / (1024 * 1024) + " MB)");
        System.out.println("Total rows: " + String.format("%,d", TOTAL_ROWS));
        System.out.println("Runs per contender: " + runs);

        Run scan = timeStruct(null, STRUCT_COLUMNS, runs);
        Run amountHalf = timeStruct(FilterPredicate.lt("s.amount", 500.0), STRUCT_COLUMNS, runs);
        Run amountSelective = timeStruct(FilterPredicate.lt("s.amount", 10.0), STRUCT_COLUMNS, runs);
        Run codeIn = timeStruct(FilterPredicate.in("s.code", new int[] {1, 5, 10, 25, 50}), STRUCT_COLUMNS, runs);
        Run catEq = timeStruct(FilterPredicate.eq("s.cat", EQ_CATEGORY), STRUCT_COLUMNS, runs);
        Run twoLeaves = timeStruct(FilterPredicate.and(
                FilterPredicate.lt("s.amount", 500.0),
                FilterPredicate.lt("s.code", 50)), STRUCT_COLUMNS, runs);
        Run topAndLeaf = timeStruct(FilterPredicate.and(
                FilterPredicate.lt("value", 500.0),
                FilterPredicate.lt("s.amount", 500.0)), new String[] {"id", "value", "s"}, runs);
        Run amountNull = timeStruct(FilterPredicate.isNull("s.amount"), STRUCT_COLUMNS, runs);
        Run filterOnly = timeStruct(FilterPredicate.lt("s.amount", 500.0), new String[] {"id"}, runs);

        System.out.println("\nResults:");
        System.out.printf("  %-50s %-26s %10s %15s %12s%n",
                "Contender", "Path", "Time (ms)", "Rows", "Records/sec");
        System.out.println("  " + "-".repeat(117));
        printResults("Struct scan (id, s; no filter)", scan, runs);
        System.out.println();
        printResults("Struct leaf 50% (s.amount<500)", amountHalf, runs);
        System.out.println();
        printResults("Struct leaf 1% (s.amount<10)", amountSelective, runs);
        System.out.println();
        printResults("Struct leaf IN (s.code IN 5 values)", codeIn, runs);
        System.out.println();
        printResults("Struct leaf binary eq (s.cat=" + EQ_CATEGORY + ")", catEq, runs);
        System.out.println();
        printResults("Two struct leaves (s.amount<500 AND s.code<50)", twoLeaves, runs);
        System.out.println();
        printResults("Top-level and leaf (value<500 AND s.amount<500)", topAndLeaf, runs);
        System.out.println();
        printResults("Struct leaf IS NULL (s.amount)", amountNull, runs);
        System.out.println();
        printResults("Filter-only struct leaf (id; s.amount<500)", filterOnly, runs);

        double present = (1 - STRUCT_NULL_RATE) * (1 - AMOUNT_NULL_RATE);
        assertThat(scan.rows[0]).isEqualTo(TOTAL_ROWS);
        assertFraction(amountHalf, present * 0.5);
        assertFraction(amountSelective, present * 0.01);
        assertFraction(codeIn, (1 - STRUCT_NULL_RATE) * 0.05);
        assertFraction(catEq, (1 - STRUCT_NULL_RATE) * 0.01);
        assertFraction(twoLeaves, present * 0.25);
        assertFraction(topAndLeaf, present * 0.25);
        assertFraction(amountNull, 1 - present);
        assertThat(filterOnly.rows[0]).isEqualTo(amountHalf.rows[0]);
    }

    /// `run` kept `expected` of the rows, within half a percentage point.
    private static void assertFraction(Run run, double expected) {
        assertThat((double) run.rows[0] / TOTAL_ROWS).isBetween(expected - 0.005, expected + 0.005);
    }

    /// Sums the `s.amount` the struct contenders read, so that the reads are not optimised away.
    private double structSink;

    private Run timeStruct(FilterPredicate filter, String[] columns, int runs) throws Exception {
        ColumnProjection projection = ColumnProjection.columns(columns);
        boolean readsStruct = Arrays.asList(columns).contains("s");
        String path = structPath(filter, projection);
        for (int i = 0; i < STRUCT_WARMUPS; i++) {
            runStruct(filter, projection, readsStruct);
        }
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runStruct(filter, projection, readsStruct);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows, path);
    }

    /// The reader the read takes and, on the flat reader, the filter's path.
    private String structPath(FilterPredicate filter, ColumnProjection projection) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FILE));
             RowReader rows = structReader(reader, filter, projection)) {
            if (!(rows instanceof FlatRowReader)) {
                return PATH_NESTED;
            }
            // The read decodes the filter's columns too, which a filter-only contender leaves
            // out of its projection, so the probe takes every column.
            return filter == null ? PATH_NONE : probePath(filter, ColumnProjection.all(), reader.getFileSchema());
        }
    }

    private long runStruct(FilterPredicate filter, ColumnProjection projection, boolean readsStruct)
            throws IOException {
        long count = 0;
        double sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT_FILE));
             RowReader rows = structReader(reader, filter, projection)) {
            while (rows.hasNext()) {
                rows.next();
                count++;
                if (readsStruct) {
                    PqStruct s = rows.getStruct("s");
                    if (s != null && !s.isNull("amount")) {
                        sum += s.getDouble("amount");
                    }
                }
            }
        }
        structSink += sum;
        return count;
    }

    private static RowReader structReader(ParquetFileReader reader, FilterPredicate filter,
                                          ColumnProjection projection) throws IOException {
        ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader().projection(projection);
        return (filter != null ? builder.filter(filter) : builder).build();
    }

    private void ensureStructFileExists() throws IOException {
        if (Files.exists(STRUCT_FILE) && Files.size(STRUCT_FILE) > 0) {
            return;
        }
        System.out.println("Generating struct benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows)...");

        Schema struct = Schema.createRecord("s_t", null, "dev.hardwood.perf", false);
        struct.setFields(List.of(
                new Schema.Field("code", Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("amount", Schema.createUnion(Schema.create(Schema.Type.NULL),
                        Schema.create(Schema.Type.DOUBLE)), null, null),
                new Schema.Field("cat", Schema.create(Schema.Type.STRING), null, null)));
        Schema schema = Schema.createRecord("struct_benchmark", null, "dev.hardwood.perf", false);
        schema.setFields(List.of(
                new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
                new Schema.Field("value", Schema.create(Schema.Type.DOUBLE), null, null),
                new Schema.Field("s", Schema.createUnion(Schema.create(Schema.Type.NULL), struct), null, null)));

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(STRUCT_FILE.toAbsolutePath().toString());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // One row group for the whole file, as the main benchmark file has.
                .withRowGroupSize((long) TOTAL_ROWS * 256)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {
            Random rng = new Random(44);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                if (rng.nextDouble() >= STRUCT_NULL_RATE) {
                    GenericRecord s = new GenericData.Record(struct);
                    s.put("code", rng.nextInt(100));
                    s.put("amount", rng.nextDouble() < AMOUNT_NULL_RATE ? null : rng.nextDouble() * 1000.0);
                    s.put("cat", CATEGORIES[rng.nextInt(CATEGORY_COUNT)]);
                    record.put("s", s);
                }
                writer.write(record);
            }
        }
        System.out.println("Generated " + STRUCT_FILE + " (" + Files.size(STRUCT_FILE) / (1024 * 1024) + " MB)");
    }

    private record EqualityRuns(Run eq, Run in, Run notIn) {}

    private interface Spelling {
        String of(String category);
    }

    /// `eq`, `inStrings` and negated `inStrings` on `column`, with the category literals the
    /// `category` contenders use, spelled as `column` holds them.
    private EqualityRuns timeEqualityShapes(String column, Spelling spelling, int runs) throws Exception {
        return new EqualityRuns(
                timeFilter(FilterPredicate.eq(column, spelling.of(EQ_CATEGORY)), runs),
                timeFilter(FilterPredicate.inStrings(column, spelled(IN_CATEGORIES, spelling)), runs),
                timeFilter(FilterPredicate.not(FilterPredicate.inStrings(column, spelled(NOT_IN_CATEGORIES, spelling))),
                        runs));
    }

    private static void printEqualityShapes(String column, EqualityRuns longRuns, int runs) {
        printResults("Long eq (" + column + ")", longRuns.eq(), runs);
        System.out.println();
        printResults("Long IN (" + column + ", " + IN_CATEGORIES.length + " values)", longRuns.in(), runs);
        System.out.println();
        printResults("Long NOT IN (" + column + ", " + NOT_IN_CATEGORIES.length + " values)", longRuns.notIn(), runs);
        System.out.println();
    }

    private static String[] spelled(String[] categories, Spelling spelling) {
        String[] out = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            out[i] = spelling.of(categories[i]);
        }
        return out;
    }

    /// 32 bytes, the category last: every value starts with the same eight bytes (`https://`).
    private static String urlTail(String category) {
        return "https://example.com/items/" + category;
    }

    /// 32 bytes, the category first: values differ within their first eight bytes.
    private static String urlHead(String category) {
        return category + "/https://example.com/items";
    }

    private FileSchema fileSchema;

    private Run timeNoFilter(int runs) throws Exception {
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runNoFilter();
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows, PATH_NONE);
    }

    private Run timeFilter(FilterPredicate filter, int runs) throws Exception {
        return timeFilter(filter, runs, 0);
    }

    private Run timeFilter(FilterPredicate filter, int runs, int warmups) throws Exception {
        // Probe the actual path the reader will take, **outside** the timing loop, so
        // the resolve + tryCompile work is not counted in the numbers.
        ColumnProjection projection = projectionFor(filter);
        String path = probePath(filter, projection, fileSchema());
        for (int i = 0; i < warmups; i++) {
            runFilter(filter, projection);
        }
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runFilter(filter, projection);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows, path);
    }

    /// [#BASE_COLUMNS] plus the columns `filter` reads.
    private ColumnProjection projectionFor(FilterPredicate filter) throws IOException {
        FileSchema schema = fileSchema();
        Set<String> columns = new LinkedHashSet<>(List.of(BASE_COLUMNS));
        collectColumns(FilterPredicateResolver.resolve(filter, schema), schema, columns);
        return ColumnProjection.columns(columns.toArray(String[]::new));
    }

    private static void collectColumns(ResolvedPredicate predicate, FileSchema schema, Set<String> out) {
        switch (predicate) {
            case ResolvedPredicate.And and -> and.children().forEach(child -> collectColumns(child, schema, out));
            case ResolvedPredicate.Or or -> or.children().forEach(child -> collectColumns(child, schema, out));
            default -> out.add(schema.getColumn(ResolvedPredicate.leafColumnIndex(predicate)).name());
        }
    }

    private FileSchema fileSchema() throws IOException {
        if (fileSchema == null) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE))) {
                fileSchema = reader.getFileSchema();
            }
        }
        return fileSchema;
    }

    /// Probes whether `filter` is batch-eligible by calling [BatchFilterCompiler.tryCompile] once
    /// against the file schema and the contender's projection. Mirrors the gate in
    /// [dev.hardwood.internal.reader.row.FlatRowReader]: `tryCompile` returns `null` →
    /// consumer-side; all-null matcher array → consumer-side; otherwise → batch-matcher.
    private static String probePath(FilterPredicate filter, ColumnProjection projection, FileSchema schema) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        ProjectedSchema projected = ProjectedSchema.create(schema, projection);
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(
                resolved, schema, projected::toProjectedIndex);
        if (compiled == null) {
            return PATH_CONSUMER;
        }
        for (ColumnBatchMatcher matcher : compiled.columnMatchers()) {
            if (matcher != null) {
                return PATH_BATCH;
            }
        }
        return PATH_CONSUMER;
    }

    private long runNoFilter() throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns(BASE_COLUMNS)).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private long runFilter(FilterPredicate filter, ColumnProjection projection) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().projection(projection).filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private void ensureBenchmarkFileExists() throws IOException {
        if (Files.exists(BENCHMARK_FILE) && Files.size(BENCHMARK_FILE) > 0) {
            return;
        }

        System.out.println("Generating benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows, 9 columns)...");

        Schema schema = benchmarkSchema();

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(BENCHMARK_FILE.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // Byte budget (not a row count): TOTAL_ROWS * 256 bytes/row is a generous
                // ceiling that forces a single row group for the whole dataset, the string,
                // long-string and decimal columns included.
                .withRowGroupSize((long) TOTAL_ROWS * 256)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42);
            Random categoryRng = new Random(CATEGORY_SEED);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                record.put("tag", rng.nextInt(100));
                record.put("flag", rng.nextBoolean());
                String category = CATEGORIES[categoryRng.nextInt(CATEGORY_COUNT)];
                record.put("category", category);
                record.put("url_tail", urlTail(category));
                record.put("url_head", urlHead(category));
                BigDecimal amount = BigDecimal.valueOf(i % DECIMAL_SPAN - DECIMAL_OFFSET, DECIMAL_SCALE);
                Schema fixedSchema = schema.getField("amount_fixed").schema();
                Schema varSchema = schema.getField("amount_var").schema();
                record.put("amount_fixed", DECIMAL_CONVERSION.toFixed(amount, fixedSchema, fixedSchema.getLogicalType()));
                // The minimal encoding, which is the form the format asks a writer for — so these
                // values are 1 to 3 bytes wide and the widths differ down the column.
                record.put("amount_var", DECIMAL_CONVERSION.toBytes(amount, varSchema, varSchema.getLogicalType()));
                writer.write(record);
            }
        }

        System.out.println("Generated " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
    }

    /// Built field by field rather than through `SchemaBuilder`, which cannot annotate a `fixed`
    /// with a logical type. `fixed` + `DECIMAL` becomes a `FIXED_LEN_BYTE_ARRAY` column and `bytes`
    /// + `DECIMAL` a `BYTE_ARRAY` one, which is the pair this benchmark needs.
    private static Schema benchmarkSchema() {
        Schema fixedDecimal = LogicalTypes.decimal(DECIMAL_PRECISION, DECIMAL_SCALE)
                .addToSchema(Schema.createFixed("amount_fixed_t", null, "dev.hardwood.perf", DECIMAL_BYTES));
        Schema variableDecimal = LogicalTypes.decimal(DECIMAL_PRECISION, DECIMAL_SCALE)
                .addToSchema(Schema.create(Schema.Type.BYTES));

        Schema schema = Schema.createRecord("benchmark", null, "dev.hardwood.perf", false);
        schema.setFields(List.of(
                new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
                new Schema.Field("value", Schema.create(Schema.Type.DOUBLE), null, null),
                new Schema.Field("tag", Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("flag", Schema.create(Schema.Type.BOOLEAN), null, null),
                new Schema.Field("category", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("url_tail", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("url_head", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("amount_fixed", fixedDecimal, null, null),
                new Schema.Field("amount_var", variableDecimal, null, null)));
        return schema;
    }

    /// [#DECIMAL_IN] as unscaled two's complement bytes padded to [#DECIMAL_BYTES], the spelling
    /// `amount_fixed` holds every value under and the only one an equality literal on it takes.
    private static byte[][] paddedDecimalLiterals() {
        byte[][] literals = decimalLiterals();
        for (int i = 0; i < literals.length; i++) {
            byte[] padded = new byte[DECIMAL_BYTES];
            byte fill = literals[i][0] < 0 ? (byte) 0xFF : 0;
            Arrays.fill(padded, 0, DECIMAL_BYTES - literals[i].length, fill);
            System.arraycopy(literals[i], 0, padded, DECIMAL_BYTES - literals[i].length, literals[i].length);
            literals[i] = padded;
        }
        return literals;
    }

    /// [#DECIMAL_IN] as unscaled two's complement bytes, the literal form a decimal `IN` takes.
    private static byte[][] decimalLiterals() {
        byte[][] literals = new byte[DECIMAL_IN.length][];
        for (int i = 0; i < DECIMAL_IN.length; i++) {
            literals[i] = DECIMAL_IN[i].unscaledValue().toByteArray();
        }
        return literals;
    }

    private static String[] categories() {
        String[] names = new String[CATEGORY_COUNT];
        for (int i = 0; i < CATEGORY_COUNT; i++) {
            names[i] = String.format("cat_%02d", i);
        }
        return names;
    }

    private static void printResults(String name, Run run, int runs) {
        for (int i = 0; i < runs; i++) {
            double ms = run.times[i] / 1_000_000.0;
            System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                    name + " [" + (i + 1) + "]", run.path, ms, run.rows[i],
                    run.rows[i] / (ms / 1000.0));
        }
        double avgMs = avg(run.times) / 1_000_000.0;
        System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                name + " [AVG]", run.path, avgMs, run.rows[0],
                run.rows[0] / (avgMs / 1000.0));
        double medianMs = median(run.times) / 1_000_000.0;
        long[] sorted = run.times.clone();
        Arrays.sort(sorted);
        System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                name + " [MEDIAN]", run.path, medianMs, run.rows[0],
                run.rows[0] / (medianMs / 1000.0));
        System.out.printf("    distribution: min %.1f ms, median %.1f ms, max %.1f ms%n",
                sorted[0] / 1_000_000.0, medianMs, sorted[sorted.length - 1] / 1_000_000.0);
    }

    private static double avg(long[] values) {
        long total = 0;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }

    private static double median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int middle = sorted.length >>> 1;
        if ((sorted.length & 1) != 0) {
            return sorted[middle];
        }
        return (sorted[middle - 1] + sorted[middle]) / 2.0;
    }
}
