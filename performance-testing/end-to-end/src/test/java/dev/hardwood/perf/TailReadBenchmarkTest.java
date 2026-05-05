/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Benchmark for the tail-read path ([ParquetFileReader.RowReaderBuilder#tail]).
///
/// On the slow path (decode-and-discard fallback), the reader decodes every
/// leading row of the first kept row group and discards them via a
/// post-iteration `next()` loop. On the fast path (per-page mask machinery
/// extended to flat columns), the leading pages are dropped entirely and the
/// straddling page is trimmed in place, so the reader yields the tail rows
/// without touching the rest of the row group.
///
/// Two paired fixtures share schema and data but differ only in whether a Page
/// Index is written, which is exactly the gate the tail-read fast path keys
/// on. Both are produced by `generate_benchmark_data.py`.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="TailReadBenchmarkTest" -Dperf.runs=5
class TailReadBenchmarkTest {

    private static final Path INDEXED_FIXTURE = Path.of(
            "../test-data-setup/target/benchmark-data/page_scan_with_index.parquet");
    private static final Path NO_INDEX_FIXTURE = Path.of(
            "../test-data-setup/target/benchmark-data/page_scan_no_index.parquet");

    private static final int DEFAULT_RUNS = 5;
    private static final long TAIL_ROWS = 10_000;

    @Test
    void tailReadThroughputWithIndex() throws Exception {
        runBenchmark("Tail Read Benchmark (with Page Index)", INDEXED_FIXTURE);
    }

    @Test
    void tailReadThroughputNoIndex() throws Exception {
        runBenchmark("Tail Read Benchmark (no Page Index)", NO_INDEX_FIXTURE);
    }

    private void runBenchmark(String label, Path fixture) throws Exception {
        Assumptions.assumeTrue(Files.exists(fixture),
                () -> "Fixture not present: " + fixture
                        + " (run generate_benchmark_data.py to produce it)");
        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== " + label + " ===");
        System.out.println("File: " + fixture.getFileName() + " ("
                + Files.size(fixture) / (1024 * 1024) + " MB)");
        System.out.println("Tail rows requested: " + TAIL_ROWS);
        System.out.println("Runs: " + runs);

        // Warm up
        runTail(fixture);
        runTail(fixture);

        long[] times = new long[runs];
        long rowsRead = 0;
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rowsRead = runTail(fixture);
            times[i] = System.nanoTime() - start;
        }

        long totalNanos = 0;
        for (int i = 0; i < runs; i++) {
            double ms = times[i] / 1_000_000.0;
            System.out.printf("  Tail [%d]  %.1f ms  (%d rows)%n", i + 1, ms, rowsRead);
            totalNanos += times[i];
        }
        double avgMs = (totalNanos / (double) runs) / 1_000_000.0;
        System.out.printf("  Tail [AVG] %.1f ms%n", avgMs);

        assertThat(rowsRead).isEqualTo(TAIL_ROWS);
    }

    private long runTail(Path fixture) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture));
             RowReader rows = reader.buildRowReader().tail(TAIL_ROWS).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }
}
