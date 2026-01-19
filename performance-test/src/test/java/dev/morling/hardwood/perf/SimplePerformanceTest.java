/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.perf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Performance comparison test between Hardwood, and parquet-java.
 *
 * <p>Downloads NYC Yellow Taxi Trip Records for 2025 and compares reading
 * performance while verifying correctness by comparing calculated sums.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimplePerformanceTest {

    private static final String BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/";
    private static final Path DATA_DIR = Path.of("target/tlc-trip-record-data");
    private static final int YEAR = 2025;

    record Result(long passengerCount, double tripDistance, double fareAmount, long durationMs, long rowCount) {
    }

    @BeforeAll
    void downloadData() throws IOException {
        Files.createDirectories(DATA_DIR);
        for (int month = 1; month <= 12; month++) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", YEAR, month);
            Path target = DATA_DIR.resolve(filename);
            if (!Files.exists(target)) {
                downloadFile(BASE_URL + filename, target);
            }
        }
    }

    @Test
    void comparePerformance() throws IOException {
        List<Path> files = getAvailableFiles();
        assertThat(files).as("At least one data file should be available").isNotEmpty();

        System.out.println("\n=== Performance Test ===");
        System.out.println("Files available: " + files.size());

        // Warmup run (not timed)
        System.out.println("\nWarmup run...");
        runHardwood(files);

        // Timed runs
        System.out.println("\nTimed runs:");
        Result hardwoodResult = timeRun("Hardwood", () -> runHardwood(files));
        Result parquetJavaResult = timeRun("parquet-java", () -> runParquetJava(files));

        // Print results
        printResults(files.size(), hardwoodResult, parquetJavaResult);

        // Verify correctness - compare against parquet-java as reference
        // Use relative tolerance for floating-point sums (accumulation error over millions of rows)
        assertThat(hardwoodResult.passengerCount())
                .as("Hardwood passenger_count should match parquet-java")
                .isEqualTo(parquetJavaResult.passengerCount());
        assertThat(hardwoodResult.tripDistance())
                .as("Hardwood trip_distance should match parquet-java")
                .isCloseTo(parquetJavaResult.tripDistance(), withinPercentage(0.0001));
        assertThat(hardwoodResult.fareAmount())
                .as("Hardwood fare_amount should match parquet-java")
                .isCloseTo(parquetJavaResult.fareAmount(), withinPercentage(0.0001));

        System.out.println("\nAll results match!");
    }

    private List<Path> getAvailableFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        for (int month = 1; month <= 11; month++) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", YEAR, month);
            Path file = DATA_DIR.resolve(filename);
            if (Files.exists(file) && Files.size(file) > 0) {
                files.add(file);
            }
        }
        return files;
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        System.out.println("  Running " + name + "...");
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    private Result runHardwood(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        for (Path file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    RowReader rowReader = reader.createRowReader()) {
                for (PqRow row : rowReader) {
                    rowCount++;
                    Long pc = row.getLong("passenger_count");
                    if (pc != null) {
                        passengerCount += pc;
                    }

                    Double td = row.getDouble("trip_distance");
                    if (td != null) {
                        tripDistance += td;
                    }

                    Double fa = row.getDouble("fare_amount");
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result runParquetJava(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;
        Configuration conf = new Configuration();

        for (Path file : files) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    rowCount++;
                    Long pc = (Long) record.get("passenger_count");
                    if (pc != null) {
                        passengerCount += pc;
                    }

                    Double td = (Double) record.get("trip_distance");
                    if (td != null) {
                        tripDistance += td;
                    }

                    Double fa = (Double) record.get("fare_amount");
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private void printResults(int fileCount, Result hardwood, Result parquetJava) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long totalBytes = 0;
        try {
            for (Path file : getAvailableFiles()) {
                totalBytes += Files.size(file);
            }
        }
        catch (IOException ignored) {
        }

        System.out.println("\n" + "=".repeat(100));
        System.out.println("PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Files processed: " + fileCount);
        System.out.println("  Total rows:      " + String.format("%,d", hardwood.rowCount()));
        System.out.println("  Total size:      " + String.format("%,.1f MB", totalBytes / (1024.0 * 1024.0)));
        System.out.println();

        // Correctness verification
        System.out.println("Correctness Verification:");
        System.out.println(String.format("  %-20s %17s %17s %17s", "", "passenger_count", "trip_distance", "fare_amount"));
        System.out.println(String.format("  %-20s %,17d %,17.2f %,17.2f", "Hardwood", hardwood.passengerCount(), hardwood.tripDistance(), hardwood.fareAmount()));
        System.out.println(String.format("  %-20s %,17d %,17.2f %,17.2f", "parquet-java", parquetJava.passengerCount(), parquetJava.tripDistance(), parquetJava.fareAmount()));
        System.out.println();

        // Performance comparison
        System.out.println("Performance:");
        System.out.println(String.format("  %-20s %12s %15s %18s %12s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(85));

        printResultRow("Hardwood", hardwood, cpuCores);
        printResultRow("parquet-java", parquetJava, 1);

        // Speedup
        System.out.println();
        double speedup = (double) parquetJava.durationMs() / hardwood.durationMs();
        System.out.println(String.format("  Speedup: %.2fx %s",
                speedup,
                speedup > 1 ? "(Hardwood is faster)" : "(parquet-java is faster)"));
        System.out.println();
        System.out.println("=".repeat(100));
    }

    private void printResultRow(String name, Result result, int cpuCores) {
        double seconds = result.durationMs() / 1000.0;
        double recordsPerSec = result.rowCount() / seconds;
        double recordsPerSecPerCore = recordsPerSec / cpuCores;

        // Estimate MB/sec (rough, based on total file size / time)
        long totalBytes = 0;
        try {
            for (Path file : getAvailableFiles()) {
                totalBytes += Files.size(file);
            }
        }
        catch (IOException ignored) {
        }
        double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / seconds;

        System.out.println(String.format("  %-20s %12.2f %,15.0f %,18.0f %12.1f",
                name,
                seconds,
                recordsPerSec,
                recordsPerSecPerCore,
                mbPerSec));
    }

    private void downloadFile(String url, Path target) {
        System.out.println("Downloading: " + url);
        try {
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<Path> response = client.send(request,
                    HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(target);
                System.out.println("  Failed (status " + response.statusCode() + ") - skipping");
            }
            else {
                System.out.println("  Downloaded: " + Files.size(target) + " bytes");
            }
        }
        catch (Exception e) {
            System.out.println("  Failed: " + e.getMessage() + " - skipping");
            try {
                Files.deleteIfExists(target);
            }
            catch (IOException ignored) {
            }
        }
    }
}
