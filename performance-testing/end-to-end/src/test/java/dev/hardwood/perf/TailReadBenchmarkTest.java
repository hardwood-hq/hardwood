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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Benchmark for the tail-read path ([ParquetFileReader.RowReaderBuilder#tail]).
///
/// On the slow path (`main` / no OffsetIndex / pre-#277 fast path), the reader
/// decodes every leading row of the first kept row group and discards them via
/// a post-iteration `next()` loop. On the fast path (#277 / OffsetIndex
/// available), per-page masking drops the leading pages entirely and trims the
/// straddling page in IndexedFetchPlan, so the reader yields the tail rows
/// without touching the rest of the row group.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="TailReadBenchmarkTest" -Dperf.runs=5
@EnabledIf("hasTaxiFile")
class TailReadBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/page_filter_benchmark.parquet");
    private static final int DEFAULT_RUNS = 5;
    private static final long TAIL_ROWS = 10_000;

    @SuppressWarnings("unused") // Used by @EnabledIf
    static boolean hasTaxiFile() {
        return Files.exists(BENCHMARK_FILE);
    }

    @Test
    void tailReadThroughput() throws Exception {
        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));

        System.out.println("\n=== Tail Read Benchmark ===");
        System.out.println("File: " + BENCHMARK_FILE.getFileName() + " ("
                + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
        System.out.println("Tail rows requested: " + TAIL_ROWS);
        System.out.println("Runs: " + runs);

        // Warm up
        runTail();
        runTail();

        long[] times = new long[runs];
        long rowsRead = 0;
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            rowsRead = runTail();
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

    private long runTail() throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().tail(TAIL_ROWS).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }
}
