/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.Arrays;
import java.util.List;

/// The read engine's mechanism in a few small files: a read is a state machine ([Engine]) that
/// never runs work itself; its poll looks at the state and returns [Command]s, a generic driver
/// (package `runtime`, which knows nothing of reading) runs them on other threads, and each result
/// comes back as a [Fact], posted under one lock and followed by another poll.
///
/// Nothing here is Parquet ([SimulatedFile]). The consumer takes batches of
/// [Engine#BATCH_ROWS] rows, in row order, holding every column.
///
/// Run from the repository root:
/// `java core/src/test/java/dev/hardwood/internal/reader/engine/micro/MicroReader.java [threads] [trace]`
/// (threads 0: the consumer's thread decodes). See `README.md` beside it.
public class MicroReader {

    public static void main(String[] args) {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        List<String> flags = Arrays.asList(args).subList(Math.min(1, args.length), args.length);
        boolean trace = flags.contains("trace");
        if (!trace) {
            // loads the classes (which the source launcher compiles on first use) and warms the JIT
            read(threads, false);
        }
        long start = System.nanoTime();
        long checksum = read(threads, trace);
        long expected = 0;
        for (int c = 0; c < SimulatedFile.PAGE_ROWS.length; c++) {
            expected += (c + 1) * (SimulatedFile.TOTAL_ROWS * (SimulatedFile.TOTAL_ROWS - 1) / 2);
        }
        System.out.printf("threads=%d rows=%d checksum %s, %d ms%n", threads, SimulatedFile.TOTAL_ROWS,
                checksum == expected ? "ok" : "WRONG", (System.nanoTime() - start) / 1_000_000);
    }

    /// Reads every batch, checking that they arrive in row order and hold every row; returns the
    /// sum of every value.
    private static long read(int threads, boolean trace) {
        long checksum = 0;
        long expectedRow = 0;
        try (Reader reader = new Reader(threads, trace)) {
            Batch batch;
            while ((batch = reader.next()) != null) {
                if (batch.firstRow() != expectedRow) {
                    throw new AssertionError("batch out of order: " + batch.firstRow() + ", expected " + expectedRow);
                }
                for (long[] column : batch.columns()) {
                    for (long v : column) {
                        checksum += v;
                    }
                }
                expectedRow += batch.columns()[0].length;
            }
        }
        if (expectedRow != SimulatedFile.TOTAL_ROWS) {
            throw new AssertionError("read " + expectedRow + " rows of " + SimulatedFile.TOTAL_ROWS);
        }
        return checksum;
    }
}
