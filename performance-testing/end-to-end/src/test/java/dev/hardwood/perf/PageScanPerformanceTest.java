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
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/// End-to-end performance comparison of page scanning strategies.
///
/// Reads two identical ~1GB Parquet files that differ only in whether an Offset Index
/// is present. When offset index metadata exists, `PageScanner` uses direct lookup
/// instead of sequentially reading page headers. This test quantifies the impact on
/// full-file read throughput.
///
/// Generate the benchmark data first:
/// ```shell
///   source .docker-venv/bin/activate && python performance-testing/generate_benchmark_data.py
/// ```
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("No significant difference between having the index or not with memory-mapped files.")
class PageScanPerformanceTest {

    private static final Path DEFAULT_DATA_DIR = Path.of("../test-data-setup/target/benchmark-data");
    private static final int DEFAULT_RUNS = 5;
    private static final String DATA_DIR_PROPERTY = "pagescan.dataDir";
    private static final String RUNS_PROPERTY = "pagescan.runs";
    private static final String CONTENDERS_PROPERTY = "pagescan.contenders";

    enum Contender {
        SEQUENTIAL("Sequential (no index)", "page_scan_no_index.parquet"),
        OFFSET_INDEX("Offset Index", "page_scan_with_index.parquet");

        private final String displayName;
        private final String fileName;

        Contender(String displayName, String fileName) {
            this.displayName = displayName;
            this.fileName = fileName;
        }

        String displayName() {
            return displayName;
        }

        String fileName() {
            return fileName;
        }

        static Contender fromString(String name) {
            for (Contender c : values()) {
                if (c.name().equalsIgnoreCase(name) || c.displayName.equalsIgnoreCase(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown contender: " + name +
                    ". Valid values: " + Arrays.toString(values()));
        }
    }

    record Result(long passengerCount, double tripDistance, double fareAmount,
                  long durationMs, long rowCount) {
    }

    private Path getDataDir() {
        String property = System.getProperty(DATA_DIR_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_DATA_DIR;
        }
        return Path.of(property);
    }

    private int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_RUNS;
        }
        return Integer.parseInt(property);
    }

    private Set<Contender> getEnabledContenders() {
        String property = System.getProperty(CONTENDERS_PROPERTY);
        if (property == null || property.isBlank() || property.equalsIgnoreCase("all")) {
            return EnumSet.allOf(Contender.class);
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Contender::fromString)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Contender.class)));
    }

    @Test
    void comparePerformance() throws IOException {
        Path dataDir = getDataDir();
        Set<Contender> enabledContenders = getEnabledContenders();
        int runCount = getRunCount();

        // Verify all files exist
        for (Contender contender : enabledContenders) {
            Path file = dataDir.resolve(contender.fileName());
            assertThat(file).as("Benchmark file not found: %s. " +
                    "Run 'python performance-testing/generate_benchmark_data.py' first.", file)
                    .exists();
        }

        System.out.println("\n=== Page Scan Performance Test ===");
        System.out.println("Data directory: " + dataDir.toAbsolutePath().normalize());
        System.out.println("Runs per contender: " + runCount);
        System.out.println("Enabled contenders: " + enabledContenders.stream()
                .map(Contender::displayName)
                .collect(Collectors.joining(", ")));

        // Warmup run
        System.out.println("\nWarmup run...");
        Contender warmupContender = enabledContenders.iterator().next();
        readFile(dataDir.resolve(warmupContender.fileName()));

        // Timed runs
        System.out.println("\nTimed runs:");
        java.util.Map<Contender, List<Result>> results = new java.util.EnumMap<>(Contender.class);

        for (Contender contender : enabledContenders) {
            Path file = dataDir.resolve(contender.fileName());
            List<Result> contenderResults = new ArrayList<>();
            for (int i = 0; i < runCount; i++) {
                Result result = timeRun(contender.displayName() + " [" + (i + 1) + "/" + runCount + "]",
                        () -> readFile(file));
                contenderResults.add(result);
            }
            results.put(contender, contenderResults);
        }

        // Print results
        printResults(dataDir, runCount, enabledContenders, results);

        // Verify correctness — both files contain identical data
        verifyCorrectness(results);
    }

    private Result readFile(Path file) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.open(InputFile.of(file));
             ColumnReader col0 = reader.createColumnReader("passenger_count");
             ColumnReader col1 = reader.createColumnReader("trip_distance");
             ColumnReader col2 = reader.createColumnReader("fare_amount")) {

            while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
                int count = col0.getRecordCount();
                int[] v0 = col0.getInts();
                double[] v1 = col1.getDoubles();
                double[] v2 = col2.getDoubles();
                BitSet n0 = col0.getElementNulls();
                BitSet n1 = col1.getElementNulls();
                BitSet n2 = col2.getElementNulls();

                for (int i = 0; i < count; i++) {
                    if (n0 == null || !n0.get(i)) {
						passengerCount += v0[i];
					}
                    if (n1 == null || !n1.get(i)) {
						tripDistance += v1[i];
					}
                    if (n2 == null || !n2.get(i)) {
						fareAmount += v2[i];
					}
                }
                rowCount += count;
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + file, e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        System.out.println("  Running " + name + "...");
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    private void verifyCorrectness(java.util.Map<Contender, List<Result>> results) {
        if (results.size() < 2) {
            return;
        }

        java.util.Map.Entry<Contender, List<Result>> first = results.entrySet().iterator().next();
        Result reference = first.getValue().get(0);
        String referenceName = first.getKey().displayName();

        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            if (entry.getKey() == first.getKey()) {
                continue;
            }
            Result other = entry.getValue().get(0);
            String otherName = entry.getKey().displayName();

            assertThat(other.rowCount())
                    .as("%s row count should match %s", otherName, referenceName)
                    .isEqualTo(reference.rowCount());
            assertThat(other.passengerCount())
                    .as("%s passenger_count should match %s", otherName, referenceName)
                    .isEqualTo(reference.passengerCount());
            assertThat(other.tripDistance())
                    .as("%s trip_distance should match %s", otherName, referenceName)
                    .isCloseTo(reference.tripDistance(), withinPercentage(0.0001));
            assertThat(other.fareAmount())
                    .as("%s fare_amount should match %s", otherName, referenceName)
                    .isCloseTo(reference.fareAmount(), withinPercentage(0.0001));
        }

        System.out.println("\nCorrectness verified: both scan paths produce identical results.");
    }

    private void printResults(Path dataDir, int runCount, Set<Contender> enabledContenders,
                              java.util.Map<Contender, List<Result>> results) throws IOException {
        int cpuCores = Runtime.getRuntime().availableProcessors();

        // Calculate total bytes across all contender files
        java.util.Map<Contender, Long> fileSizes = new java.util.EnumMap<>(Contender.class);
        for (Contender c : enabledContenders) {
            fileSizes.put(c, Files.size(dataDir.resolve(c.fileName())));
        }

        Result firstResult = results.values().iterator().next().get(0);

        System.out.println("\n" + "=".repeat(100));
        System.out.println("PAGE SCAN PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Total rows:      " + String.format("%,d", firstResult.rowCount()));
        System.out.println("  Runs per contender: " + runCount);
        for (Contender c : enabledContenders) {
            System.out.println(String.format("  %-20s %,.1f MB (%s)",
                    c.displayName() + ":", fileSizes.get(c) / (1024.0 * 1024.0), c.fileName()));
        }
        System.out.println();

        // Correctness
        if (results.size() > 1) {
            System.out.println("Correctness Verification:");
            System.out.println(String.format("  %-25s %17s %17s %17s", "", "passenger_count", "trip_distance", "fare_amount"));
            for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
                Result r = entry.getValue().get(0);
                System.out.println(String.format("  %-25s %,17d %,17.2f %,17.2f",
                        entry.getKey().displayName(), r.passengerCount(), r.tripDistance(), r.fareAmount()));
            }
            System.out.println();
        }

        // Performance
        System.out.println("Performance (all runs):");
        System.out.println(String.format("  %-30s %12s %15s %18s %12s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(95));

        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            Contender c = entry.getKey();
            List<Result> contenderResults = entry.getValue();
            long totalBytes = fileSizes.get(c);

            for (int i = 0; i < contenderResults.size(); i++) {
                String label = c.displayName() + " [" + (i + 1) + "]";
                printResultRow(label, contenderResults.get(i), cpuCores, totalBytes);
            }

            // Average
            double avgDurationMs = contenderResults.stream()
                    .mapToLong(Result::durationMs)
                    .average()
                    .orElse(0);
            long avgRowCount = contenderResults.get(0).rowCount();
            printResultRow(c.displayName() + " [AVG]",
                    new Result(0, 0, 0, (long) avgDurationMs, avgRowCount), cpuCores, totalBytes);

            // Min/max
            long minDuration = contenderResults.stream().mapToLong(Result::durationMs).min().orElse(0);
            long maxDuration = contenderResults.stream().mapToLong(Result::durationMs).max().orElse(0);
            System.out.println(String.format("  %-30s   min: %.2fs, max: %.2fs, spread: %.2fs",
                    "", minDuration / 1000.0, maxDuration / 1000.0, (maxDuration - minDuration) / 1000.0));
            System.out.println();
        }

        System.out.println("=".repeat(100));
    }

    private void printResultRow(String name, Result result, int cpuCores, long totalBytes) {
        double seconds = result.durationMs() / 1000.0;
        double recordsPerSec = result.rowCount() / seconds;
        double recordsPerSecPerCore = recordsPerSec / cpuCores;
        double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / seconds;

        System.out.println(String.format("  %-30s %12.2f %,15.0f %,18.0f %12.1f",
                name, seconds, recordsPerSec, recordsPerSecPerCore, mbPerSec));
    }
}
