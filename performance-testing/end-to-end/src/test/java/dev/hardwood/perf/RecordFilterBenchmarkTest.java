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
/// REQUIRED-column scenarios run once against a single file. The nullable-column
/// scenarios (`Nullable IS NOT NULL`, `Nullable selective`, `Scan nullable column`)
/// run once per density in [#NULL_PERCENTS] to expose how validity-bitmap handling
/// behaves at scarce / mixed / sparse densities.
class RecordFilterBenchmarkTest {

    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;
    /// Null densities at which the `score` column is regenerated and the three
    /// nullable scenarios are timed.
    private static final int[] NULL_PERCENTS = {1, 50, 90};

    private static final String PATH_DRAIN = "(Drain Side filtration)";
    private static final String PATH_CONSUMER = "(Consumer Side Filtration)";
    private static final String PATH_NONE = "";

    private record Run(long[] times, long[] rows) {}

    private static Path benchmarkFile(int nullPercent) {
        return Path.of("target/record_filter_benchmark_v2_n" + nullPercent + ".parquet");
    }

    @Test
    void compareRecordFilterOverhead() throws Exception {
        // Generate (if missing) a file per density up front so the row-group / page
        // structure is identical across the comparison.
        for (int p : NULL_PERCENTS) {
            ensureBenchmarkFileExists(benchmarkFile(p), p);
        }

        // REQUIRED-column scenarios are score-independent; pick the first density's file.
        Path requiredFile = benchmarkFile(NULL_PERCENTS[0]);

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== Record Filter Benchmark ===");
        System.out.println("File (REQUIRED scenarios): " + requiredFile
                + " (" + Files.size(requiredFile) / (1024 * 1024) + " MB)");
        System.out.println("Total rows: " + String.format("%,d", TOTAL_ROWS));
        System.out.println("Runs per contender: " + runs);
        System.out.print("Nullable densities: ");
        for (int p : NULL_PERCENTS) System.out.print(p + "% ");
        System.out.println();

        // Warmup
        System.out.println("\nWarmup...");
        runNoFilter(requiredFile);

        // ----- Baseline ---------------------
        Run noFilter = timeNoFilter(requiredFile, runs);

        Run matchAll = timeFilter(requiredFile,
                FilterPredicate.gtEq("id", 0L),
                runs);

        Run selective = timeFilter(requiredFile,
                FilterPredicate.lt("id", (long) (TOTAL_ROWS / 100)),
                runs);

        Run compound = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run pageRecord = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100)),
                        FilterPredicate.lt("id", (long) (TOTAL_ROWS - TOTAL_ROWS / 100) + (TOTAL_ROWS / 100)),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run and3 = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE),
                        FilterPredicate.gtEq("tag", 0)),
                runs);

        Run and4 = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE),
                        FilterPredicate.gtEq("tag", 0),
                        FilterPredicate.notEq("flag", false)),
                runs);

        Run compoundSelective = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.lt("id", 10_000L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run compoundMid = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 0L),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run sortedCluster = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.lt("id", (long) (TOTAL_ROWS / 2)),
                        FilterPredicate.gtEq("tag", 0)),
                runs);

        Run empty = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.lt("id", 0L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run orFilter = timeFilter(requiredFile,
                FilterPredicate.or(
                        FilterPredicate.lt("id", 0L),
                        FilterPredicate.lt("value", 500.0)),
                runs);

        Run rangeDup = timeFilter(requiredFile,
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 1_000_000L),
                        FilterPredicate.lt("id", 2_000_000L),
                        FilterPredicate.lt("value", Double.MAX_VALUE)),
                runs);

        Run intIn = timeFilter(requiredFile,
                FilterPredicate.in("tag", new int[] {1, 5, 10, 25, 50}),
                runs);

        // Nullable scenarios per density.
        Run[] nullableMatchAll = new Run[NULL_PERCENTS.length];
        Run[] nullableSelective = new Run[NULL_PERCENTS.length];
        Run[] scanNullable = new Run[NULL_PERCENTS.length];
        for (int i = 0; i < NULL_PERCENTS.length; i++) {
            Path file = benchmarkFile(NULL_PERCENTS[i]);
            nullableMatchAll[i] = timeFilter(file, FilterPredicate.isNotNull("score"), runs);
            nullableSelective[i] = timeFilter(file, FilterPredicate.lt("score", 100.0), runs);
            scanNullable[i] = timeScanScoreColumn(file, runs);
        }

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

        for (int i = 0; i < NULL_PERCENTS.length; i++) {
            int p = NULL_PERCENTS[i];
            System.out.println();
            printResults("Nullable IS NOT NULL (score, " + p + "% null)", PATH_DRAIN, nullableMatchAll[i], runs);
            System.out.println();
            printResults("Nullable selective (score<100, " + p + "% null)", PATH_DRAIN, nullableSelective[i], runs);
            System.out.println();
            printResults("Scan nullable column (read score, " + p + "% null)", PATH_NONE, scanNullable[i], runs);
        }

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
        assertThat(pageRecord.rows[0]).isGreaterThan(0L).isLessThan(TOTAL_ROWS / 50L);
        assertThat(and3.rows[0]).isEqualTo(TOTAL_ROWS);
        assertThat(and4.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        assertThat(compoundSelective.rows[0]).isEqualTo(10_000L);
        assertThat(compoundMid.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        assertThat(sortedCluster.rows[0]).isEqualTo(TOTAL_ROWS / 2L);
        assertThat(empty.rows[0]).isEqualTo(0L);
        assertThat(orFilter.rows[0]).isBetween((long) (TOTAL_ROWS * 0.4), (long) (TOTAL_ROWS * 0.6));
        assertThat(rangeDup.rows[0]).isEqualTo(1_000_000L);
        assertThat(intIn.rows[0]).isBetween((long) (TOTAL_ROWS * 0.03), (long) (TOTAL_ROWS * 0.07));
        for (int i = 0; i < NULL_PERCENTS.length; i++) {
            int p = NULL_PERCENTS[i];
            double nonNullFrac = (100.0 - p) / 100.0;
            long expectedNonNull = (long) (TOTAL_ROWS * nonNullFrac);
            // ±2pp tolerance for the random null mask.
            assertThat(nullableMatchAll[i].rows[0])
                    .isBetween((long) (TOTAL_ROWS * (nonNullFrac - 0.02)),
                            (long) (TOTAL_ROWS * (nonNullFrac + 0.02)));
            // score<100 over the present rows with score uniform [0,1000) → ~10% of present.
            assertThat(nullableSelective[i].rows[0])
                    .isBetween((long) (expectedNonNull * 0.08), (long) (expectedNonNull * 0.12));
            assertThat(scanNullable[i].rows[0]).isEqualTo(nullableMatchAll[i].rows[0]);
        }
    }

    private Run timeNoFilter(Path file, int runs) throws Exception {
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runNoFilter(file);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows);
    }

    private Run timeFilter(Path file, FilterPredicate filter, int runs) throws Exception {
        long[] times = new long[runs];
        long[] rows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rows[i] = runFilter(file, filter);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rows);
    }

    private long runNoFilter(Path file) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private long runScanScoreColumn(Path file) throws Exception {
        long nonNullCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                if (!rows.isNull("score")) {
                    if (rows.getDouble("score") >= 0.0) {
                        nonNullCount++;
                    }
                }
            }
        }
        return nonNullCount;
    }

    private Run timeScanScoreColumn(Path file, int runs) throws Exception {
        long[] times = new long[runs];
        long[] rowsCount = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rowsCount[i] = runScanScoreColumn(file);
            times[i] = System.nanoTime() - start;
        }
        return new Run(times, rowsCount);
    }

    private long runFilter(Path file, FilterPredicate filter) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private void ensureBenchmarkFileExists(Path file, int nullPercent) throws IOException {
        if (Files.exists(file) && Files.size(file) > 0) {
            return;
        }

        System.out.println("Generating " + file + " (" + TOTAL_ROWS / 1_000_000
                + "M rows, 5 columns, ~" + nullPercent + "% nulls on score)...");

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
                .requiredInt("tag")
                .requiredBoolean("flag")
                .optionalDouble("score")
                .endRecord();

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) TOTAL_ROWS * 16)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42 + nullPercent);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                record.put("tag", rng.nextInt(100));
                record.put("flag", rng.nextBoolean());
                if (rng.nextInt(100) < nullPercent) {
                    record.put("score", null);
                }
                else {
                    record.put("score", rng.nextDouble() * 1000.0);
                }
                writer.write(record);
            }
        }

        System.out.println("Generated " + file + " (" + Files.size(file) / (1024 * 1024) + " MB)");
    }

    private static void printResults(String name, String path, Run run, int runs) {
        for (int i = 0; i < runs; i++) {
            double ms = run.times[i] / 1_000_000.0;
            System.out.printf("  %-50s %-26s %10.1f %,15d %,12.0f%n",
                    name + " [" + (i + 1) + "]", path, ms, run.rows[i],
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
