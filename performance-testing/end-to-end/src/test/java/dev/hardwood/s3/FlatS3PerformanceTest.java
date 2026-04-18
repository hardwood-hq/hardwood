/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// Performance test for Hardwood reading NYC Yellow Taxi Trip Records from S3.
///
/// Uses [S3Proxy](https://github.com/gaul/s3proxy) in a testcontainer with its
/// `filesystem` backend. The local NYC taxi data directory is bind-mounted as
/// the bucket root, so files appear as S3 objects with no upload step.
///
/// **Caveat:** loopback HTTP to a local proxy. This measures Hardwood's S3
/// client + HTTP overhead vs local files; it is not representative of WAN
/// latency or real AWS S3 throughput.
///
/// File range defaults to the last 6 months of the data set
/// (`2025-07`..`2025-12`), and can be widened via `-Dperf.start=YYYY-MM`
/// and `-Dperf.end=YYYY-MM`. Run count via `-Dperf.runs=N`.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlatS3PerformanceTest {

    private static final Path DATA_DIR = Path.of("../test-data-setup/target/tlc-trip-record-data");
    private static final YearMonth DEFAULT_START = YearMonth.of(2025, 7);
    private static final YearMonth DEFAULT_END = YearMonth.of(2025, 12);
    private static final int DEFAULT_RUNS = 10;
    private static final String START_PROPERTY = "perf.start";
    private static final String END_PROPERTY = "perf.end";
    private static final String RUNS_PROPERTY = "perf.runs";
    private static final String BUCKET = "perf-bucket";
    private static final String ACCESS_KEY = "access";
    private static final String SECRET_KEY = "secret";

    @Container
    static GenericContainer<?> s3proxy = new GenericContainer<>("andrewgaul/s3proxy:latest")
            .withExposedPorts(80)
            .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
            .withEnv("S3PROXY_IDENTITY", ACCESS_KEY)
            .withEnv("S3PROXY_CREDENTIAL", SECRET_KEY)
            .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
            .withEnv("JCLOUDS_PROVIDER", "filesystem")
            .withEnv("JCLOUDS_FILESYSTEM_BASEDIR", "/data")
            .withFileSystemBind(
                    DATA_DIR.toAbsolutePath().normalize().toString(),
                    "/data/" + BUCKET,
                    BindMode.READ_ONLY);

    private static S3Source source;
    private static List<String> availableKeys;

    record Result(long passengerCount, double tripDistance, double fareAmount, long durationMs, long rowCount) {
    }

    record SchemaGroup(List<InputFile> files, boolean passengerCountIsLong) {
    }

    @BeforeAll
    static void setup() throws Exception {
        String endpoint = "http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80);
        source = S3Source.builder()
                .endpoint(endpoint)
                .pathStyle(true)
                .credentials(S3Credentials.of(ACCESS_KEY, SECRET_KEY))
                .build();

        YearMonth start = getStartMonth();
        YearMonth end = getEndMonth();
        if (start.isAfter(end)) {
            throw new IllegalArgumentException(String.format(
                    "perf.start (%s) is after perf.end (%s); did you set only one of them?", start, end));
        }

        availableKeys = new ArrayList<>();
        long totalBytes = 0;
        for (YearMonth ym = start; !ym.isAfter(end); ym = ym.plusMonths(1)) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", ym.getYear(), ym.getMonthValue());
            Path file = DATA_DIR.resolve(filename);
            if (Files.exists(file) && Files.size(file) > 0) {
                availableKeys.add(filename);
                totalBytes += Files.size(file);
            }
        }
        System.out.println(String.format("S3Proxy serving %d files (%,.1f MB) from %s as s3://%s/ (range %s..%s)",
                availableKeys.size(), totalBytes / (1024.0 * 1024.0),
                DATA_DIR.toAbsolutePath().normalize(), BUCKET, start, end));
    }

    @AfterAll
    static void tearDown() {
        if (source != null) {
            source.close();
        }
    }

    @Test
    void comparePerformance() {
        assertThat(availableKeys)
                .as("At least one data file should be available. Run test-data-setup first.")
                .isNotEmpty();

        int runCount = getRunCount();

        System.out.println("\n=== S3 Performance Test ===");
        System.out.println("Files available: " + availableKeys.size());
        System.out.println("Runs: " + runCount);

        // Warmup: 3 full runs to let the JIT stabilize
        System.out.println("\nWarmup runs (3 full runs):");
        for (int i = 0; i < 3; i++) {
            long warmStart = System.currentTimeMillis();
            runHardwoodRowReaderIndexed(availableKeys);
            long warmDuration = System.currentTimeMillis() - warmStart;
            System.out.println(String.format("  Warmup [%d/3]: %.2f s", i + 1, warmDuration / 1000.0));
        }

        // Timed runs
        System.out.println("\nTimed runs:");
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < runCount; i++) {
            Result result = timeRun("Hardwood (row reader indexed) [" + (i + 1) + "/" + runCount + "]",
                    () -> runHardwoodRowReaderIndexed(availableKeys));
            results.add(result);
        }

        printResults(availableKeys.size(), runCount, results);
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("  %s: %.2f s", name, duration / 1000.0));
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    /// Row-oriented reader opened via [MultiFileParquetReader#openAll] with
    /// projection and indexed field access, reading from S3.
    ///
    /// Groups files by schema compatibility (passenger_count type varies across
    /// files) and reads each group as a single multi-file read.
    private Result runHardwoodRowReaderIndexed(List<String> keys) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");
        // Projected indices: 0=passenger_count, 1=trip_distance, 2=fare_amount

        List<SchemaGroup> groups = groupFilesBySchema(keys);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (MultiFileParquetReader parquet = hardwood.openAll(group.files());
                     RowReader rowReader = parquet.createRowReader(projection)) {
                    boolean pcIsLong = group.passengerCountIsLong();

                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rowCount++;

                        if (!rowReader.isNull(0)) { // passenger_count
                            if (pcIsLong) {
                                passengerCount += rowReader.getLong(0);
                            }
                            else {
                                passengerCount += (long) rowReader.getDouble(0);
                            }
                        }

                        if (!rowReader.isNull(1)) { // trip_distance
                            tripDistance += rowReader.getDouble(1);
                        }

                        if (!rowReader.isNull(2)) { // fare_amount
                            fareAmount += rowReader.getDouble(2);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read files from S3", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Groups files by full schema fingerprint (column name + physical type for
    /// every leaf column). When reading all columns, drift in *any* column —
    /// not just `passenger_count` — would make a multi-file open fail, so we
    /// bucket by the entire schema shape.
    private List<SchemaGroup> groupFilesBySchema(List<String> keys) {
        java.util.LinkedHashMap<String, List<InputFile>> bySchema = new java.util.LinkedHashMap<>();
        java.util.Map<String, Boolean> pcIsLongBySchema = new java.util.HashMap<>();

        for (String key : keys) {
            try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(BUCKET, key))) {
                StringBuilder fp = new StringBuilder();
                for (dev.hardwood.schema.ColumnSchema col : reader.getFileSchema().getColumns()) {
                    fp.append(col.name()).append(':').append(col.type()).append(';');
                }
                String fingerprint = fp.toString();

                SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");
                boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                        && pn.type() == PhysicalType.INT64;

                bySchema.computeIfAbsent(fingerprint, k -> new ArrayList<>())
                        .add(source.inputFile(BUCKET, key));
                pcIsLongBySchema.putIfAbsent(fingerprint, pcIsLong);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read schema from: " + key, e);
            }
        }

        List<SchemaGroup> groups = new ArrayList<>();
        for (java.util.Map.Entry<String, List<InputFile>> e : bySchema.entrySet()) {
            groups.add(new SchemaGroup(e.getValue(), pcIsLongBySchema.get(e.getKey())));
        }
        return groups;
    }

    private static YearMonth getStartMonth() {
        String property = System.getProperty(START_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_START;
        }
        return YearMonth.parse(property);
    }

    private static YearMonth getEndMonth() {
        String property = System.getProperty(END_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_END;
        }
        return YearMonth.parse(property);
    }

    private static int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_RUNS;
        }
        return Integer.parseInt(property);
    }

    private void printResults(int fileCount, int runCount, List<Result> results) {
        int cpuCores = Runtime.getRuntime().availableProcessors();

        Result firstResult = results.get(0);

        System.out.println("\n" + "=".repeat(100));
        System.out.println("S3 PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Files processed: " + fileCount);
        System.out.println("  Total rows:      " + String.format("%,d", firstResult.rowCount()));
        System.out.println("  Runs:            " + runCount);
        System.out.println();

        System.out.println("Performance (all runs):");
        System.out.println(String.format("  %-40s %12s %15s %18s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core"));
        System.out.println("  " + "-".repeat(90));

        for (int i = 0; i < results.size(); i++) {
            String label = "Hardwood (row reader indexed) [" + (i + 1) + "]";
            printResultRow(label, results.get(i), cpuCores);
        }

        double avgDurationMs = results.stream().mapToLong(Result::durationMs).average().orElse(0);
        long avgRowCount = results.get(0).rowCount();
        Result avgResult = new Result(0, 0, 0, (long) avgDurationMs, avgRowCount);
        printResultRow("Hardwood (row reader indexed) [AVG]", avgResult, cpuCores);

        long minDuration = results.stream().mapToLong(Result::durationMs).min().orElse(0);
        long maxDuration = results.stream().mapToLong(Result::durationMs).max().orElse(0);
        System.out.println(String.format("  %-40s   min: %.2fs, max: %.2fs, spread: %.2fs",
                "", minDuration / 1000.0, maxDuration / 1000.0, (maxDuration - minDuration) / 1000.0));

        System.out.println();
        System.out.println("=".repeat(100));
    }

    private void printResultRow(String name, Result result, int cpuCores) {
        double seconds = result.durationMs() / 1000.0;
        double recordsPerSec = result.rowCount() / seconds;
        double recordsPerSecPerCore = recordsPerSec / cpuCores;

        System.out.println(String.format("  %-40s %12.2f %,15.0f %,18.0f",
                name, seconds, recordsPerSec, recordsPerSecPerCore));
    }
}
