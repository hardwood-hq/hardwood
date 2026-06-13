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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Performance test for flat schemas containing non-repeated struct columns.
///
/// Uses a generated dataset with top-level primitives mixed with struct columns
/// containing only primitive fields. Exercises the flat reader path introduced
/// to support structs without record assembly overhead.
///
/// Run tools/flat-struct-perf-datagen.py first to generate the test data.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlatStructPerformanceTest {

    private static final Path DATA_FILE = Path.of(
            "../test-data-setup/target/flat-struct-perf-data/flat_struct_perf.parquet");
    private static final int DEFAULT_RUNS = 5;
    private static final String RUNS_PROPERTY = "perf.runs";

    record Result(long idSum, double latSum, double scoreSum, long rowCount, long durationMs) {}

    @Test
    void comparePerformance() throws IOException {
        assertThat(Files.exists(DATA_FILE))
                .as("Test data file not found. Run tools/flat-struct-perf-datagen.py first.")
                .isTrue();

        int runCount = getRunCount();

        System.out.println("\n=== Flat Struct Performance Test ===");
        System.out.println("Runs: " + runCount);

        // Warmup
        System.out.println("\nWarmup runs (3):");
        for (int i = 0; i < 3; i++) {
            long start = System.currentTimeMillis();
            run();
            System.out.printf("  Warmup [%d/3]: %.2f s%n", i + 1, (System.currentTimeMillis() - start) / 1000.0);
        }

        // Timed runs
        System.out.println("\nTimed runs:");
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < runCount; i++) {
            long start = System.currentTimeMillis();
            Result result = run();
            long duration = System.currentTimeMillis() - start;
            results.add(new Result(result.idSum(), result.latSum(), result.scoreSum(), result.rowCount(), duration));
            System.out.printf("  [%d/%d]: %.2f s%n", i + 1, runCount, duration / 1000.0);
        }

        printResults(results);
        // After the run, assert we processed all rows and got non-zero sums
        System.out.println("Asserting results");
        assertThat(results.get(0).rowCount()).isEqualTo(10_000_000L);
    }

    private Result run() throws IOException {
        long idSum = 0;
        double latSum = 0.0;
        double scoreSum = 0.0;
        long rowCount = 0;

        // Projected indices: 0=id, 1=location (struct), 2=score
        ColumnProjection projection = ColumnProjection.columns("id", "location", "score");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.open(InputFile.of(DATA_FILE));
             RowReader rowReader = parquet.buildRowReader().projection(projection).build()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;

                if (!rowReader.isNull(0)) {
                    idSum += rowReader.getLong(0);
                }

                PqStruct location = rowReader.getStruct(1);
                if (location != null) {
                    latSum += location.getDouble("lat");
                }

                if (!rowReader.isNull(2)) {
                    scoreSum += rowReader.getDouble(2);
                }
            }
        }

        return new Result(idSum, latSum, scoreSum, rowCount, 0);
    }

    private int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) return DEFAULT_RUNS;
        return Integer.parseInt(property);
    }

    private void printResults(List<Result> results) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long totalBytes;
        try {
            totalBytes = Files.size(DATA_FILE);
        }
        catch (IOException e) {
            totalBytes = 0;
        }

        System.out.println("\n" + "=".repeat(100));
        System.out.println("FLAT STRUCT PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println("CPU cores:   " + cpuCores);
        System.out.println("Total rows:  " + String.format("%,d", results.get(0).rowCount()));
        System.out.println("File size:   " + String.format("%,.1f MB", totalBytes / (1024.0 * 1024.0)));
        System.out.println();
        System.out.println(String.format("  %-10s %12s %15s %18s %12s",
                "Run", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(70));

        for (int i = 0; i < results.size(); i++) {
            Result r = results.get(i);
            printResultRow("[" + (i + 1) + "]", r, cpuCores, totalBytes);
        }

        double avgMs = results.stream().mapToLong(Result::durationMs).average().orElse(0);
        long minMs = results.stream().mapToLong(Result::durationMs).min().orElse(0);
        long maxMs = results.stream().mapToLong(Result::durationMs).max().orElse(0);
        Result avgResult = new Result(0, 0, 0, results.get(0).rowCount(), (long) avgMs);
        printResultRow("[AVG]", avgResult, cpuCores, totalBytes);
        System.out.printf("  min: %.2fs, max: %.2fs, spread: %.2fs%n",
                minMs / 1000.0, maxMs / 1000.0, (maxMs - minMs) / 1000.0);
        System.out.println("=".repeat(100));
    }

    private void printResultRow(String label, Result r, int cpuCores, long totalBytes) {
        double seconds = r.durationMs() / 1000.0;
        double recsPerSec = r.rowCount() / seconds;
        System.out.printf("  %-10s %12.2f %,15.0f %,18.0f %12.1f%n",
                label, seconds, recsPerSec, recsPerSec / cpuCores,
                (totalBytes / (1024.0 * 1024.0)) / seconds);
    }
}