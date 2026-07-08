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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/// Performance test for flat schemas containing non-repeated struct columns.
///
/// Uses a generated dataset with top-level primitives mixed with struct columns
/// containing only primitive fields. Exercises the flat reader path introduced
/// to support structs without record assembly overhead.
///
/// Run tools/simple-datagen.py first to generate the test data.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlatStructPerformanceTest {

    private static final Path DATA_FILE = Path.of(
            "../test-data-setup/target/flat-struct-perf-data/flat_struct_perf.parquet");
    private static final int DEFAULT_RUNS = 5;
    private static final String RUNS_PROPERTY = "perf.runs";
    private static final String CONTENDERS_PROPERTY = "perf.contenders";

    record Result(long idSum, double latSum, double scoreSum, long rowCount, long durationMs) {}

    enum Contender {
        HARDWOOD_ROW_READER_NAMED("Hardwood (row reader named)"),
        HARDWOOD_ROW_READER_INDEXED("Hardwood (row reader indexed)");

        private final String displayName;

        Contender(String displayName) {
            this.displayName = displayName;
        }

        String displayName() {
            return displayName;
        }

        static FlatStructPerformanceTest.Contender fromString(String name) {
            for (FlatStructPerformanceTest.Contender c : values()) {
                if (c.name().equalsIgnoreCase(name) || c.displayName.equalsIgnoreCase(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown contender: " + name +
                    ". Valid values: " + Arrays.toString(values()));
        }
    }

    private Set<FlatStructPerformanceTest.Contender> getEnabledContenders() {
        String property = System.getProperty(CONTENDERS_PROPERTY);
        if (property == null || property.isBlank()) {
            return EnumSet.of(FlatStructPerformanceTest.Contender.HARDWOOD_ROW_READER_INDEXED);
        }
        if (property.equalsIgnoreCase("all")) {
            return EnumSet.allOf(Contender.class);
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(FlatStructPerformanceTest.Contender::fromString)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(FlatStructPerformanceTest.Contender.class)));
    }

    @Test
    void comparePerformance() throws IOException {
        assertThat(Files.exists(DATA_FILE))
                .as("Test data file not found. Run the datagen script first.")
                .isTrue();

        Set<Contender> enabledContenders = getEnabledContenders();
        assertThat(enabledContenders).as("At least one contender must be enabled").isNotEmpty();

        int runCount = getRunCount();

        System.out.println("\n=== Flat Struct Performance Test ===");
        System.out.println("Runs per contender: " + runCount);
        System.out.println("Enabled contenders: " + enabledContenders.stream()
                .map(Contender::displayName)
                .collect(Collectors.joining(", ")));

        // Warmup: 3 full runs per contender
        System.out.println("\nWarmup runs (3 full runs per contender):");
        for (Contender contender : enabledContenders) {
            for (int i = 0; i < 3; i++) {
                long warmStart = System.currentTimeMillis();
                getRunner(contender).get();
                long warmDuration = System.currentTimeMillis() - warmStart;
                System.out.printf("  Warmup %s [%d/3]: %.2f s%n",
                        contender.displayName(), i + 1, warmDuration / 1000.0);
            }
        }

        // Timed runs
        System.out.println("\nTimed runs:");
        java.util.Map<Contender, List<Result>> results = new java.util.EnumMap<>(Contender.class);

        for (Contender contender : enabledContenders) {
            List<Result> contenderResults = new ArrayList<>();
            for (int i = 0; i < runCount; i++) {
                Result result = timeRun(contender.displayName() + " [" + (i + 1) + "/" + runCount + "]",
                        () -> getRunner(contender).get());
                contenderResults.add(result);
            }
            results.put(contender, contenderResults);
        }

        printResults(runCount, enabledContenders, results);
        verifyCorrectness(results);
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

            System.out.println("Reader class: " + rowReader.getClass().getSimpleName());
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

    private Supplier<Result> getRunner(FlatStructPerformanceTest.Contender contender) {
        return switch (contender) {
            case HARDWOOD_ROW_READER_NAMED -> this::runHardwoodRowReaderNamed;
            case HARDWOOD_ROW_READER_INDEXED -> this::runHardwoodRowReaderIndexed;
        };
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        System.out.printf("  %s: %.2f s%n", name, duration / 1000.0);
        return new Result(result.idSum(), result.latSum(), result.scoreSum(), result.rowCount(), duration);
    }

    private void verifyCorrectness(java.util.Map<Contender, List<Result>> results) {
        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            Result r = entry.getValue().get(0);
            String name = entry.getKey().displayName();

            assertThat(r.rowCount())
                    .as("%s should process all rows", name)
                    .isEqualTo(10_000_000L);
            assertThat(r.idSum())
                    .as("%s idSum should be non-zero", name)
                    .isGreaterThan(0L);
            assertThat(r.latSum())
                    .as("%s latSum should be non-zero", name)
                    .isGreaterThan(0.0);
            assertThat(r.scoreSum())
                    .as("%s scoreSum should be non-zero", name)
                    .isGreaterThan(0.0);
        }

        // If multiple contenders ran, cross-check they agree with each other
        if (results.size() > 1) {
            java.util.Map.Entry<Contender, List<Result>> first = results.entrySet().iterator().next();
            Result reference = first.getValue().get(0);
            String referenceName = first.getKey().displayName();

            for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
                if (entry.getKey() == first.getKey()) {
                    continue;
                }
                Result other = entry.getValue().get(0);
                String otherName = entry.getKey().displayName();

                assertThat(other.idSum())
                        .as("%s idSum should match %s", otherName, referenceName)
                        .isEqualTo(reference.idSum());
                assertThat(other.latSum())
                        .as("%s latSum should match %s", otherName, referenceName)
                        .isCloseTo(reference.latSum(), withinPercentage(0.0001));
                assertThat(other.scoreSum())
                        .as("%s scoreSum should match %s", otherName, referenceName)
                        .isCloseTo(reference.scoreSum(), withinPercentage(0.0001));
            }

            System.out.println("\nAll results match!");
        }
    }

    /// projection and indexed field access.
    private Result runHardwoodRowReaderIndexed() {
        System.out.println("Running contender: HARDWOOD_ROW_READER_INDEXED");
        long idSum = 0;
        double latSum = 0.0;
        double scoreSum = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns("id", "location", "score");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.open(InputFile.of(DATA_FILE));
             RowReader rowReader = parquet.buildRowReader().projection(projection).build()) {

            System.out.println("Reader class: " + rowReader.getClass().getSimpleName());
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
        catch (IOException e) {
            throw new RuntimeException("Failed to read files", e);
        }

        return new Result(idSum, latSum, scoreSum, rowCount, 0);
    }

    /// Same as [#runHardwoodRowReaderIndexed] but uses field names instead of projected indices.
    private Result runHardwoodRowReaderNamed() {
        System.out.println("Running contender: HARDWOOD_ROW_READER_NAMED");
        long idSum = 0;
        double latSum = 0.0;
        double scoreSum = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns("id", "location", "score");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.open(InputFile.of(DATA_FILE));
             RowReader rowReader = parquet.buildRowReader().projection(projection).build()) {

            System.out.println("Reader class: " + rowReader.getClass().getSimpleName());
            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;

                if (!rowReader.isNull("id")) {
                    idSum += rowReader.getLong("id");
                }

                PqStruct location = rowReader.getStruct("location");
                if (location != null) {
                    latSum += location.getDouble("lat");
                }

                if (!rowReader.isNull("score")) {
                    scoreSum += rowReader.getDouble("score");
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read files", e);
        }

        return new Result(idSum, latSum, scoreSum, rowCount, 0);
    }

    private int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) return DEFAULT_RUNS;
        return Integer.parseInt(property);
    }

    private void printResults(int runCount, Set<Contender> enabledContenders,
                              java.util.Map<Contender, List<Result>> results) throws IOException {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long totalBytes = Files.size(DATA_FILE);

        Result firstResult = results.values().iterator().next().get(0);

        System.out.println("\n" + "=".repeat(100));
        System.out.println("FLAT STRUCT PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Total rows:      " + String.format("%,d", firstResult.rowCount()));
        System.out.println("  File size:       " + String.format("%,.1f MB", totalBytes / (1024.0 * 1024.0)));
        System.out.println("  Runs per contender: " + runCount);
        System.out.println();

        if (results.size() > 1) {
            System.out.println("Correctness Verification:");
            System.out.println(String.format("  %-35s %12s %15s %15s", "", "idSum", "latSum", "scoreSum"));
            for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
                Result r = entry.getValue().get(0);
                System.out.println(String.format("  %-35s %,12d %,15.2f %,15.2f",
                        entry.getKey().displayName(), r.idSum(), r.latSum(), r.scoreSum()));
            }
            System.out.println();
        }

        System.out.println("Performance (all runs):");
        System.out.println(String.format("  %-35s %12s %15s %18s %12s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(100));

        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            Contender c = entry.getKey();
            List<Result> contenderResults = entry.getValue();

            for (int i = 0; i < contenderResults.size(); i++) {
                String label = c.displayName() + " [" + (i + 1) + "]";
                printResultRow(label, contenderResults.get(i), cpuCores, totalBytes);
            }

            double avgDurationMs = contenderResults.stream()
                    .mapToLong(Result::durationMs)
                    .average()
                    .orElse(0);
            long avgRowCount = contenderResults.get(0).rowCount();
            Result avgResult = new Result(0, 0, 0, avgRowCount, (long) avgDurationMs);
            printResultRow(c.displayName() + " [AVG]", avgResult, cpuCores, totalBytes);

            long minDuration = contenderResults.stream().mapToLong(Result::durationMs).min().orElse(0);
            long maxDuration = contenderResults.stream().mapToLong(Result::durationMs).max().orElse(0);
            System.out.println(String.format("  %-35s   min: %.2fs, max: %.2fs, spread: %.2fs",
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

        System.out.println(String.format("  %-35s %12.2f %,15.0f %,18.0f %12.1f",
                name, seconds, recordsPerSec, recordsPerSecPerCore, mbPerSec));
    }
}
