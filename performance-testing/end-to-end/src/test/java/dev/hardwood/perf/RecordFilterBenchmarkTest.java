/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Benchmark for record-level filtering overhead.
///
/// Compares RowReader performance across:
/// - **Baseline**: no filter at all (raw scan throughput).
/// - **Match-all / compound match-all**: predicates that keep every row — worst case
///   overhead because the filter is evaluated for each row but never prunes.
/// - **Selective**: predicates that drop most rows — real-world wins.
/// - **Drain-eligible compound ANDs** (2/3/4 leaves across `id`, `value`, `tag`, `flag`):
///   exercise the column-local AND fast path.
/// - **Fallback shapes** (single-leaf, OR, same-column range, IN-list): trip the
///   drain-eligibility gate so [FilteredRowReader] handles them. See [BatchFilterCompiler].
/// - **Page+record**: id range that prunes ~99% of pages via column-index min/max,
///   then a per-row `value<500` filter on the survivors.
///
/// Schema: `id` (long, sequential 0..N), `value` (double uniform 0..1000),
/// `tag` (int uniform 0..99), `flag` (boolean uniform).
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="RecordFilterBenchmarkTest" -Dperf.runs=5
class RecordFilterBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/record_filter_benchmark.parquet");
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;

    private static final String PATH_DRAIN = "(Drain Side filtration)";
    private static final String PATH_CONSUMER = "(Consumer Side Filtration)";
    private static final String PATH_NONE = "";

    private record Run(long[] times, long[] rows) {}

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
                // effect: same drain shape, same selectivity, very different runs.
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

        Run rangeDup = timeFilter(
                // Same-column range on `id` is not drain-eligible (duplicate column).
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 1_000_000L),
                        FilterPredicate.lt("id", 2_000_000L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run intIn = timeFilter(
                FilterPredicate.in("tag", new int[] {1, 5, 10, 25, 50}),
                runs);

        // ----- Print results ------------------------------------------------
        System.out.println("\nResults:");
        System.out.printf("  %-50s %-26s %10s %15s %12s%n",
                "Contender", "Path", "Time (ms)", "Rows", "Records/sec");
        System.out.println("  " + "-".repeat(117));

        printResults("No filter (baseline)", PATH_NONE, noFilter, runs);
        System.out.println();
        printResults("Match-all (id>=0)", PATH_DRAIN, matchAll, runs);
        System.out.println();
        printResults("Selective (id<1%)", PATH_DRAIN, selective, runs);
        System.out.println();
        printResults("Compound match-all (id>=0 AND value<+inf)", PATH_DRAIN, compound, runs);
        System.out.println();
        printResults("Page+record (id top 1% AND value<500)", PATH_CONSUMER, pageRecord, runs);
        System.out.println();
        printResults("And3 match-all (id+value+tag)", PATH_DRAIN, and3, runs);
        System.out.println();
        printResults("And4 ~50% (id+value+tag+!flag)", PATH_DRAIN, and4, runs);
        System.out.println();
        printResults("Compound selective (id<10K AND value<+inf)", PATH_DRAIN, compoundSelective, runs);
        System.out.println();
        printResults("Compound mid 50% (id>=0 AND value<500)", PATH_DRAIN, compoundMid, runs);
        System.out.println();
        printResults("Sorted cluster 50% (id<N/2 AND tag>=0)", PATH_DRAIN, sortedCluster, runs);
        System.out.println();
        printResults("Empty result (id<0 AND value<+inf)", PATH_DRAIN, empty, runs);
        System.out.println();
        printResults("OR fallback (id<0 OR value<500)", PATH_CONSUMER, orFilter, runs);
        System.out.println();
        printResults("Range+value (id BETWEEN 1M..2M)", PATH_CONSUMER, rangeDup, runs);
        System.out.println();
        printResults("intIn (tag IN [1,5,10,25,50])", PATH_DRAIN, intIn, runs);

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
        assertThat(rangeDup.rows[0]).isEqualTo(1_000_000L);
        // intIn: 5 of 100 values, expect ~5%.
        assertThat(intIn.rows[0]).isBetween((long) (TOTAL_ROWS * 0.03), (long) (TOTAL_ROWS * 0.07));
    }

    private Run timeNoFilter(int runs) throws Exception {
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runNoFilter();
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows);
    }

    private Run timeFilter(FilterPredicate filter, int runs) throws Exception {
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runFilter(filter);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows);
    }

    private long runNoFilter() throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private long runFilter(FilterPredicate filter) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
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

        System.out.println("Generating benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows, 4 columns)...");

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
                .requiredInt("tag")
                .requiredBoolean("flag")
                .endRecord();

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(BENCHMARK_FILE.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) TOTAL_ROWS * 16)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                record.put("tag", rng.nextInt(100));
                record.put("flag", rng.nextBoolean());
                writer.write(record);
            }
        }

        System.out.println("Generated " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
    }

    private static void printResults(String name, String path, Run run, int runs) {
        for (int i = 0; i < runs; i++) {
            double ms = run.times[i] / 1_000_000.0;
            System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                    name + " [" + (i + 1) + "]", "", ms, run.rows[i],
                    run.rows[i] / (ms / 1000.0));
        }
        double avgMs = avg(run.times) / 1_000_000.0;
        System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                name + " [AVG]", path, avgMs, run.rows[0],
                run.rows[0] / (avgMs / 1000.0));
    }

    private static double avg(long[] values) {
        long total = 0;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }
}