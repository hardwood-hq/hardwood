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
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Benchmark for page-level Column Index filtering.
///
/// Generates a synthetic file with sorted id column and small pages so that
/// page-level filtering can skip the majority of pages for selective predicates.
/// The file is created lazily on first run and reused thereafter.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="PageFilterBenchmarkTest" -Dperf.runs=5
class PageFilterBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/page_filter_benchmark.parquet");
    private static final int ROWS_PER_ROW_GROUP = 10_000_000;
    private static final int NUM_ROW_GROUPS = 5;
    private static final long TOTAL_ROWS = (long) ROWS_PER_ROW_GROUP * NUM_ROW_GROUPS;
    private static final int DEFAULT_RUNS = 5;

    @Test
    void compareFilteredVsUnfiltered() throws Exception {
        ensureBenchmarkFileExists();

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== Page Filter Benchmark ===");
        System.out.println("File: " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
        System.out.println("Total rows: " + String.format("%,d", TOTAL_ROWS));
        System.out.println("Runs per contender: " + runs);

        // Warmup
        System.out.println("\nWarmup...");
        runUnfiltered();

        // Timed runs: unfiltered
        long[] unfilteredTimes = new long[runs];
        long[] unfilteredRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            long[] result = runUnfiltered();
            unfilteredTimes[i] = System.nanoTime() - start;
            unfilteredRows[i] = result[0];
        }

        // Timed runs: filtered (id in first 10% of range → should skip ~90% of pages)
        long[] filteredTimes = new long[runs];
        long[] filteredRows = new long[runs];
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            long[] result = runFiltered();
            filteredTimes[i] = System.nanoTime() - start;
            filteredRows[i] = result[0];
        }

        // Print results
        System.out.println("\nResults:");
        System.out.printf("  %-45s %10s %15s %12s%n", "Contender", "Time (ms)", "Rows", "Records/sec");
        System.out.println("  " + "-".repeat(85));

        for (int i = 0; i < runs; i++) {
            double ms = unfilteredTimes[i] / 1_000_000.0;
            System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                    "Unfiltered [" + (i + 1) + "]", ms, unfilteredRows[i],
                    unfilteredRows[i] / (ms / 1000.0));
        }
        double avgUnfilteredMs = avg(unfilteredTimes) / 1_000_000.0;
        System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                "Unfiltered [AVG]", avgUnfilteredMs, unfilteredRows[0],
                unfilteredRows[0] / (avgUnfilteredMs / 1000.0));

        System.out.println();
        for (int i = 0; i < runs; i++) {
            double ms = filteredTimes[i] / 1_000_000.0;
            System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                    "Filtered (id < 10%) [" + (i + 1) + "]", ms, filteredRows[i],
                    filteredRows[i] / (ms / 1000.0));
        }
        double avgFilteredMs = avg(filteredTimes) / 1_000_000.0;
        System.out.printf("  %-45s %10.1f %,15d %,12.0f%n",
                "Filtered (id < 10%) [AVG]", avgFilteredMs, filteredRows[0],
                filteredRows[0] / (avgFilteredMs / 1000.0));

        System.out.printf("%n  Speedup: %.1fx (%.0f ms → %.0f ms)%n",
                avgUnfilteredMs / avgFilteredMs, avgUnfilteredMs, avgFilteredMs);
        System.out.printf("  Rows: %,d → %,d (%.1f%% filtered out)%n",
                unfilteredRows[0], filteredRows[0],
                100.0 * (1.0 - (double) filteredRows[0] / unfilteredRows[0]));

        // Verify that filtering actually reduced the number of rows read
        assertThat(filteredRows[0])
                .as("Filtered run should read fewer rows than unfiltered")
                .isLessThan(unfilteredRows[0]);
        assertThat(unfilteredRows[0])
                .as("Unfiltered run should read all rows")
                .isEqualTo(TOTAL_ROWS);
    }

    /// Reads all rows, summing the value column.
    private long[] runUnfiltered() throws Exception {
        long rowCount = 0;
        double sum = 0.0;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             ColumnReader idCol = reader.columnReader("id");
             ColumnReader valCol = reader.columnReader("value")) {

            while (idCol.nextBatch() & valCol.nextBatch()) {
                int count = idCol.getRecordCount();
                double[] values = valCol.getDoubles();
                for (int i = 0; i < count; i++) {
                    sum += values[i];
                }
                rowCount += count;
            }
        }

        return new long[]{ rowCount, Double.doubleToLongBits(sum) };
    }

    /// Reads with a filter on id (first 10% of total range).
    /// With sorted id and small pages, ~90% of pages should be skipped.
    private long[] runFiltered() throws Exception {
        long rowCount = 0;
        double sum = 0.0;

        FilterPredicate filter = FilterPredicate.lt("id", (long) (TOTAL_ROWS / 10));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             ColumnReader idCol = reader.buildColumnReader("id").filter(filter).build();
             ColumnReader valCol = reader.buildColumnReader("value").filter(filter).build()) {

            while (idCol.nextBatch() & valCol.nextBatch()) {
                int count = idCol.getRecordCount();
                double[] values = valCol.getDoubles();
                for (int i = 0; i < count; i++) {
                    sum += values[i];
                }
                rowCount += count;
            }
        }

        return new long[]{ rowCount, Double.doubleToLongBits(sum) };
    }

    /// Generates the benchmark file if it doesn't already exist.
    /// 50M rows across 5 row groups, sorted id, small pages, Parquet v2 with Column Index.
    private void ensureBenchmarkFileExists() throws IOException {
        if (Files.exists(BENCHMARK_FILE) && Files.size(BENCHMARK_FILE) > 0) {
            return;
        }

        System.out.println("Generating benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows)...");

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
                .requiredInt("category")
                .endRecord();

        Configuration conf = new Configuration();
        conf.setInt("parquet.page.size", 4096);
        conf.setBoolean("parquet.page.write-checksum.enabled", false);
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(BENCHMARK_FILE.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ROWS_PER_ROW_GROUP * 28)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42);
            for (int rg = 0; rg < NUM_ROW_GROUPS; rg++) {
                long startId = (long) rg * ROWS_PER_ROW_GROUP;
                for (int i = 0; i < ROWS_PER_ROW_GROUP; i++) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("id", startId + i);
                    record.put("value", rng.nextDouble() * 1000.0);
                    record.put("category", rg);
                    writer.write(record);
                }
                System.out.printf("  Row group %d: id [%,d, %,d], category=%d%n",
                        rg, startId, startId + ROWS_PER_ROW_GROUP - 1, rg);
            }
        }

        long fileSizeMb = Files.size(BENCHMARK_FILE) / (1024 * 1024);
        System.out.println("Generated " + BENCHMARK_FILE + " (" + fileSizeMb + " MB)");
    }

    private static double avg(long[] values) {
        long total = 0;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }
}
