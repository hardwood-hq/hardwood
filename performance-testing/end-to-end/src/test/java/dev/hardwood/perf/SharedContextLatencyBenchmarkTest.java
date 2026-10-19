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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

/// Latency of small reads while a large scan runs on the same [HardwoodContext].
///
/// Every read sharing a context submits its page tasks to one FIFO pool, so the read-ahead tasks
/// of a large scan can sit in the queue ahead of a small read's first pages. The benchmark runs
/// `N` consumer threads, each opening a small file and reading it to the end, over and over, in
/// three settings per pool size:
///
/// - `idle`: the small reads alone on a context of `T` threads;
/// - `shared`: the small reads on a context of `T` threads on which a large scan runs at the same
///   time, its consumer taking batches as fast as they come;
/// - `separate`: the small reads on one context of `T` threads and the scan on another of `T`
///   threads, so the two share the CPUs but not the queue.
///
/// `separate` against `idle` is the cost of sharing the CPUs; `shared` against `separate` is the
/// cost of sharing the queue. Per small read it reports the time from open to the first batch,
/// the time from open to close, and the `BatchWait` JFR time its consumer spent stalled; for the
/// scan, its rows per second. A `solo` line gives the scan's rate alone.
///
/// The small file is written on first use: one row group of four columns (two `INT64`, two
/// `DOUBLE`) of random values.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl core,performance-testing/end-to-end -am -Dtestdata.download.skip=true \
///     -Dsurefire.failIfNoSpecifiedTests=false \
///     -Dtest=SharedContextLatencyBenchmarkTest -Dperf.scanFile=/path/to/yellow_tripdata_2016-01.parquet
///
/// Options (system properties):
/// - `perf.scanFile`: the file the large scan reads, all columns; required
/// - `perf.smallFile`: where the small file is written (default `target/shared-context-small.parquet`)
/// - `perf.smallRows`: rows of the small file (default 500000)
/// - `perf.threads`: comma-separated pool sizes (default `4,8`)
/// - `perf.readers`: comma-separated small-reader counts (default `1,2,4,16`)
/// - `perf.seconds`: seconds per setting (default 4)
/// - `perf.jfr`: where the JFR recording is written (default `target/shared-context.jfr`)
class SharedContextLatencyBenchmarkTest {

    private static final String SMALL_PREFIX = "small-";
    private static final String SCAN_PREFIX = "scan-";

    @Test
    void smallReadsBesideLargeScan() throws Exception {
        String scanFile = System.getProperty("perf.scanFile");
        Assumptions.assumeTrue(scanFile != null && Files.exists(Path.of(scanFile)),
                "Set perf.scanFile to the file the large scan reads");
        run(Path.of(scanFile));
    }

    /// Runs the benchmark outside a test runner: `java -cp ... SharedContextLatencyBenchmarkTest <scanFile>`.
    public static void main(String[] args) throws Exception {
        run(Path.of(args[0]));
    }

    private static void run(Path scanFile) throws Exception {
        Path smallFile = Path.of(System.getProperty("perf.smallFile", "target/shared-context-small.parquet"));
        int smallRows = Integer.getInteger("perf.smallRows", 500_000);
        int[] poolSizes = ints(System.getProperty("perf.threads", "4,8"));
        int[] readerCounts = ints(System.getProperty("perf.readers", "1,2,4,16"));
        long nanos = Duration.ofMillis((long) (Double.parseDouble(System.getProperty("perf.seconds", "4")) * 1000)).toNanos();
        Path jfr = Path.of(System.getProperty("perf.jfr", "target/shared-context.jfr"));

        writeSmallFile(smallFile, smallRows);
        long expectedScanRows;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(scanFile))) {
            expectedScanRows = reader.getFileMetaData().numRows();
        }
        System.out.printf("scan file %s (%d rows), small file %s (%d rows), %.1f s per setting, %d processors%n",
                scanFile.getFileName(), expectedScanRows, smallFile.getFileName(), smallRows,
                nanos / 1e9, Runtime.getRuntime().availableProcessors());

        List<Result> results = new ArrayList<>();
        try (Recording recording = new Recording()) {
            recording.enable("dev.hardwood.BatchWait").withThreshold(Duration.ZERO);
            recording.enable("dev.hardwood.PageDecoded").withThreshold(Duration.ZERO);
            recording.start();

            // Warm-up: every code path once, results discarded
            Bench warm = new Bench(scanFile, expectedScanRows, smallFile, smallRows, Math.min(nanos, Duration.ofSeconds(2).toNanos()));
            warm.run("warm", poolSizes[0], 2, Mode.SHARED);
            warm.run("warm", poolSizes[0], 2, Mode.SEPARATE);

            Bench bench = new Bench(scanFile, expectedScanRows, smallFile, smallRows, nanos);
            for (int threads : poolSizes) {
                results.add(bench.run("T" + threads, threads, 0, Mode.SOLO));
                for (int readers : readerCounts) {
                    for (Mode mode : new Mode[]{ Mode.IDLE, Mode.SHARED, Mode.SEPARATE }) {
                        results.add(bench.run("T" + threads, threads, readers, mode));
                    }
                }
            }
            recording.stop();
            recording.dump(jfr);
        }
        Map<String, double[]> waits = batchWaits(jfr);
        double[] scanPages = scanPageDecode(jfr);

        System.out.println();
        System.out.println("pool readers mode      reads  firstBatch ms p50/p95/p99   total ms p50/p95/p99     waitMs/read  scan Mrows/s");
        for (Result r : results) {
            double[] w = waits.getOrDefault(r.key(), new double[2]);
            System.out.printf("T%-3d %7d %-8s %6d  %6.1f %6.1f %6.1f   %6.1f %6.1f %6.1f   %10.2f  %10.2f%n",
                    r.threads, r.readers, r.mode.name().toLowerCase(), r.firstBatch.length,
                    pct(r.firstBatch, 50), pct(r.firstBatch, 95), pct(r.firstBatch, 99),
                    pct(r.total, 50), pct(r.total, 95), pct(r.total, 99),
                    r.firstBatch.length == 0 ? 0 : w[0] / r.firstBatch.length,
                    r.scanRowsPerSecond / 1e6);
        }
        System.out.printf("%nscan PageDecoded events over the run: %.0f, mean %.3f ms%n", scanPages[0], scanPages[1]);
        System.out.println("JFR recording: " + jfr.toAbsolutePath());
    }

    private enum Mode {
        SOLO, IDLE, SHARED, SEPARATE
    }

    private record Result(String prefix, int threads, int readers, Mode mode, long[] firstBatch, long[] total,
                          double scanRowsPerSecond) {

        String key() {
            return prefix + "-" + readers + "-" + mode;
        }
    }

    private record Bench(Path scanFile, long scanRows, Path smallFile, int smallRows, long nanos) {

        Result run(String prefix, int threads, int readers, Mode mode) throws Exception {
            String key = prefix + "-" + readers + "-" + mode;
            HardwoodContext smallContext = HardwoodContext.create(threads);
            HardwoodContext scanContext = mode == Mode.SEPARATE ? HardwoodContext.create(threads) : smallContext;
            try {
                AtomicBoolean stop = new AtomicBoolean();
                AtomicLong scanned = new AtomicLong();
                Thread scan = null;
                if (mode != Mode.IDLE) {
                    scan = Thread.ofPlatform().name(SCAN_PREFIX + key).start(() -> scanLoop(scanContext, stop, scanned));
                    // Let the scan fill its read-ahead before the small reads start
                    Thread.sleep(300);
                }
                if (readers == 0) {
                    long before = scanned.get();
                    long t0 = System.nanoTime();
                    Thread.sleep(Duration.ofNanos(nanos));
                    long rows = scanned.get() - before;
                    stop.set(true);
                    scan.join();
                    if (rows == 0) {
                        throw new IllegalStateException("The scan read no rows");
                    }
                    return new Result(prefix, threads, 0, mode, new long[0], new long[0], rows * 1e9 / (System.nanoTime() - t0));
                }
                List<Thread> smallThreads = new ArrayList<>();
                List<long[][]> outs = new ArrayList<>();
                long scannedBefore = scanned.get();
                long t0 = System.nanoTime();
                long deadline = t0 + nanos;
                for (int i = 0; i < readers; i++) {
                    long[][] out = new long[2][];
                    outs.add(out);
                    smallThreads.add(Thread.ofPlatform().name(SMALL_PREFIX + key + "-" + i)
                            .start(() -> smallLoop(smallContext, deadline, out)));
                }
                for (Thread t : smallThreads) {
                    t.join();
                }
                long elapsed = System.nanoTime() - t0;
                long scanRows = scanned.get() - scannedBefore;
                stop.set(true);
                if (scan != null) {
                    scan.join();
                }
                if (mode != Mode.IDLE && scanRows == 0) {
                    throw new IllegalStateException("The scan read no rows while the small reads ran");
                }
                for (long[][] out : outs) {
                    if (out[0] == null) {
                        throw new IllegalStateException("A small reader failed");
                    }
                }
                return new Result(prefix, threads, readers, mode, merge(outs, 0), merge(outs, 1),
                        mode == Mode.IDLE ? 0 : scanRows * 1e9 / elapsed);
            }
            finally {
                smallContext.close();
                if (scanContext != smallContext) {
                    scanContext.close();
                }
            }
        }

        private static long[] merge(List<long[][]> outs, int which) {
            long[] all = outs.stream().flatMapToLong(out -> Arrays.stream(out[which])).sorted().toArray();
            return all;
        }

        private void scanLoop(HardwoodContext context, AtomicBoolean stop, AtomicLong scanned) {
            try {
                while (!stop.get()) {
                    long rows = 0;
                    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(scanFile), context);
                            ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
                        while (!stop.get() && columns.nextBatch()) {
                            int count = columns.getRecordCount();
                            rows += count;
                            scanned.addAndGet(count);
                        }
                    }
                    if (!stop.get() && rows != scanRows) {
                        throw new IllegalStateException("Scan read " + rows + " rows, expected " + scanRows);
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void smallLoop(HardwoodContext context, long deadline, long[][] out) {
            long[] first = new long[1024];
            long[] total = new long[1024];
            int n = 0;
            long sink = 0;
            try {
                while (System.nanoTime() < deadline) {
                    long start = System.nanoTime();
                    long firstBatch = -1;
                    long rows = 0;
                    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(smallFile), context);
                            ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
                        ColumnReader a = columns.getColumnReader("a");
                        ColumnReader x = columns.getColumnReader("x");
                        while (columns.nextBatch()) {
                            if (firstBatch < 0) {
                                firstBatch = System.nanoTime() - start;
                            }
                            int count = columns.getRecordCount();
                            rows += count;
                            sink += a.getLongs()[count - 1] + (long) x.getDoubles()[count - 1];
                        }
                    }
                    long elapsed = System.nanoTime() - start;
                    if (rows != smallRows) {
                        throw new IllegalStateException("Small read read " + rows + " rows, expected " + smallRows);
                    }
                    if (n == first.length) {
                        first = Arrays.copyOf(first, n * 2);
                        total = Arrays.copyOf(total, n * 2);
                    }
                    first[n] = firstBatch;
                    total[n] = elapsed;
                    n++;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (sink == 42) {
                System.out.print("");
            }
            out[0] = Arrays.copyOf(first, n);
            out[1] = Arrays.copyOf(total, n);
        }
    }

    private static void writeSmallFile(Path file, int rows) throws IOException {
        if (Files.exists(file)) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
                if (reader.getFileMetaData().numRows() == rows) {
                    return;
                }
            }
        }
        Files.createDirectories(file.toAbsolutePath().getParent());
        FileSchema schema = FileSchema.builder("small")
                .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("x", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("y", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        SplittableRandom random = new SplittableRandom(42);
        long[] a = new long[rows];
        long[] b = new long[rows];
        double[] x = new double[rows];
        double[] y = new double[rows];
        for (int i = 0; i < rows; i++) {
            a[i] = random.nextLong();
            b[i] = random.nextLong(1_000);
            x[i] = random.nextDouble();
            y[i] = random.nextDouble() * 100;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("a", a).longs("b", b).doubles("x", x).doubles("y", y));
        }
    }

    /// Per result key: total `BatchWait` milliseconds and event count on the small readers' threads.
    private static Map<String, double[]> batchWaits(Path jfr) throws IOException {
        Map<String, double[]> waits = new HashMap<>();
        for (RecordedEvent event : RecordingFile.readAllEvents(jfr)) {
            if (!event.getEventType().getName().equals("dev.hardwood.BatchWait") || event.getThread() == null) {
                continue;
            }
            String thread = event.getThread().getJavaName();
            if (thread == null || !thread.startsWith(SMALL_PREFIX)) {
                continue;
            }
            String key = thread.substring(SMALL_PREFIX.length(), thread.lastIndexOf('-'));
            double[] w = waits.computeIfAbsent(key, k -> new double[2]);
            w[0] += event.getDuration().toNanos() / 1e6;
            w[1]++;
        }
        return waits;
    }

    /// The scan's `PageDecoded` count and mean duration, over the whole recording.
    private static double[] scanPageDecode(Path jfr) throws IOException {
        long count = 0;
        double millis = 0;
        for (RecordedEvent event : RecordingFile.readAllEvents(jfr)) {
            if (!event.getEventType().getName().equals("dev.hardwood.PageDecoded")) {
                continue;
            }
            String column = event.getString("column");
            if (column.equals("a") || column.equals("b") || column.equals("x") || column.equals("y")) {
                continue;
            }
            count++;
            millis += event.getDuration().toNanos() / 1e6;
        }
        return new double[]{ count, count == 0 ? 0 : millis / count };
    }

    private static double pct(long[] sorted, int percentile) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, index)] / 1e6;
    }

    private static int[] ints(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
    }
}
