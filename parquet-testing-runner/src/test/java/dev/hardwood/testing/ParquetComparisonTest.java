/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.HotSpotDiagnosticMXBean.ThreadDumpFormat;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/// Comparison tests that validate [ParquetFileReader]'s output against the reference
/// parquet-java implementation by comparing parsed results row-by-row, field-by-field
/// for every fixture in `parquet-testing`. Bad-file rejection lives in
/// [BadDataHandlingTest].
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetComparisonTest {

    @BeforeAll
    void setUp() throws IOException {
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        Utils.ensureGoodCFile(repoDir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        // Skip individual files
        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");
        String blockedBy = Utils.rowComparisonSkipReason(testFile);
        assumeFalse(blockedBy != null,
                () -> "Skipping " + fileName + " (blocked by " + blockedBy + ")");

        compareParquetFile(testFile);
    }

    @ParameterizedTest(name = "column: {0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareColumnsWithReference(Path testFile) throws Exception {
        String fileName = testFile.getFileName().toString();

        // Skip files that are in either skip list
        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");
        assumeFalse(Utils.COLUMN_SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in column skip list)");

        runWithThreadDumpOnTimeout(() -> compareColumnsParquetFile(testFile), 120, fileName);
    }

    // ==================== Helpers ====================

    /// Runs an action with a watchdog. If it doesn't complete within
    /// `timeoutSeconds`, dumps all thread stack traces and interrupts
    /// the test thread. Used to diagnose hangs on CI.
    private void runWithThreadDumpOnTimeout(ThrowingRunnable action, int timeoutSeconds,
            String context) throws Exception {
        Thread testThread = Thread.currentThread();
        java.util.concurrent.ScheduledExecutorService watchdog =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "test-watchdog");
                    t.setDaemon(true);
                    return t;
                });
        watchdog.schedule(() -> {
            System.err.println("=== THREAD DUMP (timeout after " + timeoutSeconds
                    + "s in " + context + ") ===");
            dumpAllThreadsIncludingVirtual();
            System.err.println("=== END THREAD DUMP ===");
            testThread.interrupt();
        }, timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
        try {
            action.run();
        }
        finally {
            watchdog.shutdownNow();
        }
    }

    /// Dumps all platform and virtual threads via [HotSpotDiagnosticMXBean] (JDK 21+).
    /// Falls back to [Thread#getAllStackTraces] (platform threads only) if the
    /// JSON dump fails for any reason.
    private static void dumpAllThreadsIncludingVirtual() {
        Path dumpFile = null;
        try {
            dumpFile = Files.createTempFile("hardwood-threaddump-", ".json");
            Files.deleteIfExists(dumpFile);
            HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(
                    HotSpotDiagnosticMXBean.class);
            bean.dumpThreads(dumpFile.toAbsolutePath().toString(), ThreadDumpFormat.JSON);
            Files.readAllLines(dumpFile).forEach(System.err::println);
            return;
        }
        catch (Throwable t) {
            System.err.println("[watchdog] JSON thread dump failed (" + t
                    + "), falling back to platform-thread stacks:");
        }
        finally {
            if (dumpFile != null) {
                try {
                    Files.deleteIfExists(dumpFile);
                }
                catch (IOException ignored) {
                }
            }
        }
        for (java.util.Map.Entry<Thread, StackTraceElement[]> entry
                : Thread.getAllStackTraces().entrySet()) {
            Thread t = entry.getKey();
            System.err.println("\n\"" + t.getName() + "\" " + t.getState());
            for (StackTraceElement frame : entry.getValue()) {
                System.err.println("    at " + frame);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /// Compare a Parquet file column-by-column using the batch ColumnReader API
    /// against parquet-java reference data.
    private void compareColumnsParquetFile(Path testFile) throws IOException {
        System.out.println("Column comparing: " + testFile.getFileName());

        List<GenericRecord> referenceRows = Utils.readWithParquetJava(testFile);

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile))) {
            Utils.compareColumns(fileReader, referenceRows);
        }

        System.out.println("  Column comparison passed!");
    }

    /// Compare a Parquet file using both implementations.
    private void compareParquetFile(Path testFile) throws IOException {
        System.out.println("Comparing: " + testFile.getFileName());

        // Read with parquet-java (reference)
        List<GenericRecord> referenceRows = Utils.readWithParquetJava(testFile);
        System.out.println("  parquet-java rows: " + referenceRows.size());

        // Compare with Hardwood row by row
        int hardwoodRowCount = compareWithHardwood(testFile, referenceRows);
        System.out.println("  Hardwood rows: " + hardwoodRowCount);

        // Verify row counts match
        assertThat(hardwoodRowCount)
                .as("Row count mismatch")
                .isEqualTo(referenceRows.size());

        System.out.println("  All " + referenceRows.size() + " rows match!");
    }

    /// Read with Hardwood and compare row by row against reference.
    /// Returns the number of rows read.
    private int compareWithHardwood(Path file, List<GenericRecord> referenceRows) throws IOException {
        int rowIndex = 0;

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.rowReader()) {
            FileSchema schema = fileReader.getFileSchema();
            while (rowReader.hasNext()) {
                rowReader.next();
                assertThat(rowIndex)
                        .as("Hardwood has more rows than reference")
                        .isLessThan(referenceRows.size());
                Utils.compareRow(rowIndex, referenceRows.get(rowIndex), rowReader, schema);
                rowIndex++;
            }
        }

        return rowIndex;
    }
}
