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
/// Compares RowReader performance across three scenarios:
/// - No filter (baseline)
/// - Match-all filter (worst case overhead — every row is evaluated but kept)
/// - Selective filter (real-world case — most rows are skipped)
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="RecordFilterBenchmarkTest" -Dperf.runs=5
class RecordFilterBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/record_filter_benchmark.parquet");
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;

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

        // No filter (baseline)
        long[] noFilterTimes = new long[runs];
        long[] noFilterRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            noFilterRows[i] = runNoFilter();
            noFilterTimes[i] = System.nanoTime() - start;
        }

        // Match-all filter (worst case overhead — predicate evaluates every row but keeps all)
        long[] matchAllTimes = new long[runs];
        long[] matchAllRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            matchAllRows[i] = runMatchAllFilter();
            matchAllTimes[i] = System.nanoTime() - start;
        }

        // Selective filter (id < 1% of range — skip 99% of rows)
        long[] selectiveTimes = new long[runs];
        long[] selectiveRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            selectiveRows[i] = runSelectiveFilter();
            selectiveTimes[i] = System.nanoTime() - start;
        }

        // Compound match-all And — exercises predicate-tree dispatch and recursion overhead
        long[] compoundTimes = new long[runs];
        long[] compoundRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            compoundRows[i] = runCompoundMatchAllFilter();
            compoundTimes[i] = System.nanoTime() - start;
        }

        // Page-prunable range + per-row value filter — exercises page-level filtering
        // (via column-index min/max statistics) AND record-level filtering on the rows
        // that survive page pruning.
        long[] combinedTimes = new long[runs];
        long[] combinedRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            combinedRows[i] = runPageAndRecordFilter();
            combinedTimes[i] = System.nanoTime() - start;
        }

        // Print results
        System.out.println("\nResults:");
        System.out.printf("  %-45s %10s %15s %12s%n", "Contender", "Time (ms)", "Rows", "Records/sec");
        System.out.println("  " + "-".repeat(85));

        printResults("No filter (baseline)", noFilterTimes, noFilterRows, runs);
        System.out.println();
        printResults("Match-all filter (worst case)", matchAllTimes, matchAllRows, runs);
        System.out.println();
        printResults("Selective filter (id < 1%)", selectiveTimes, selectiveRows, runs);
        System.out.println();
        printResults("Compound match-all (id>=0 AND value<+inf)", compoundTimes, compoundRows, runs);
        System.out.println();
        printResults("Page+record (id range AND value<500)", combinedTimes, combinedRows, runs);

        double avgNoFilter = avg(noFilterTimes) / 1_000_000.0;
        double avgMatchAll = avg(matchAllTimes) / 1_000_000.0;
        double avgSelective = avg(selectiveTimes) / 1_000_000.0;
        double avgCompound = avg(compoundTimes) / 1_000_000.0;
        double avgCombined = avg(combinedTimes) / 1_000_000.0;

        System.out.printf("%n  Match-all overhead: %.1f%% (%.0f ms → %.0f ms)%n",
                100.0 * (avgMatchAll - avgNoFilter) / avgNoFilter, avgNoFilter, avgMatchAll);
        System.out.printf("  Selective speedup: %.1fx (%.0f ms → %.0f ms)%n",
                avgNoFilter / avgSelective, avgNoFilter, avgSelective);
        System.out.printf("  Compound overhead: %.1f%% (%.0f ms → %.0f ms)%n",
                100.0 * (avgCompound - avgNoFilter) / avgNoFilter, avgNoFilter, avgCompound);
        System.out.printf("  Page+record speedup: %.1fx (%.0f ms → %.1f ms)%n",
                avgNoFilter / avgCombined, avgNoFilter, avgCombined);

        // Correctness
        assertThat(noFilterRows[0]).isEqualTo(TOTAL_ROWS);
        assertThat(matchAllRows[0]).isEqualTo(TOTAL_ROWS);
        assertThat(selectiveRows[0]).isLessThan(TOTAL_ROWS);
        assertThat(compoundRows[0]).isEqualTo(TOTAL_ROWS);
        // Combined: id range narrows to ~100K rows (1% of total), value < 500 keeps roughly half
        // (uniform 0..1000), so we expect somewhere between 30K and 70K matching rows.
        assertThat(combinedRows[0]).isGreaterThan(0L).isLessThan(TOTAL_ROWS / 50L);
    }

    private long runPageAndRecordFilter() throws Exception {
        // Page-prunable: id BETWEEN 9_500_000 and 9_600_000 — only the last few data
        // pages overlap this range, so column-index min/max should drop ~99% of pages
        // before any row is decoded.
        // Per-row only: value < 500.0 — value is random uniform [0, 1000), so its
        // statistics span the whole range and provide no page-level pruning.
        // Combined effect: page filter saves I/O + decode, record filter then runs on
        // the surviving ~100K rows.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100)),
                FilterPredicate.lt("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100) + (TOTAL_ROWS / 100)),
                FilterPredicate.lt("value", 500.0));
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

    private long runCompoundMatchAllFilter() throws Exception {
        // Two-leaf AND that matches every row — exercises tree recursion and per-leaf dispatch overhead.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("value", Double.MAX_VALUE));
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

    private long runMatchAllFilter() throws Exception {
        // id >= 0 matches every row — worst case for per-row evaluation overhead
        FilterPredicate filter = FilterPredicate.gtEq("id", 0L);
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

    private long runSelectiveFilter() throws Exception {
        // id < 1% of range — should return ~100K rows out of 10M
        FilterPredicate filter = FilterPredicate.lt("id", (long) (TOTAL_ROWS / 100));
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

        System.out.println("Generating benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows)...");

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
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
                writer.write(record);
            }
        }

        System.out.println("Generated " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
    }

    private static void printResults(String name, long[] times, long[] rows, int runs) {
        for (int i = 0; i < runs; i++) {
            double ms = times[i] / 1_000_000.0;
            System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                    name + " [" + (i + 1) + "]", ms, rows[i],
                    rows[i] / (ms / 1000.0));
        }
        double avgMs = avg(times) / 1_000_000.0;
        System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                name + " [AVG]", avgMs, rows[0],
                rows[0] / (avgMs / 1000.0));
    }

    private static double avg(long[] values) {
        long total = 0;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }
}
