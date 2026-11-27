/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.forked.ForkedJvm;
import dev.hardwood.internal.thrift.RleExpansionFile;

import static org.assertj.core.api.Assertions.assertThat;

/// A read whose decodes exhaust the heap fails with the `OutOfMemoryError` rather than leaving its
/// consumer waiting, also where reporting a decode's failure, which allocates, throws in turn.
///
/// The reads run in a JVM of 64 MiB ([ForkedJvm]) with the working-set limit lifted, over 40
/// `INT64` columns whose one data page each holds 400,000 values (3.2 MB of batches per column,
/// 128 MB in all). Each test reads the file three times on one context, so that a read that leaves
/// the context's pool threads broken fails the reads after it.
@ForkedJvm(jvmArgs = { "-Xmx64m", "-Dhardwood.internal.maxWorkingSetBytes=" + Long.MAX_VALUE }, timeoutSeconds = 60)
class HeapExhaustionTest {

    private static final int READS = 3;
    /// what the test allocates to learn that the heap has room again
    private static final int HEAP_PROBE_BYTES = 8 * 1024 * 1024;

    private static byte[] heapProbe;

    /// Nothing the test runs once the heap is exhausted may run for the first time, as linking code
    /// allocates: the outcome of each read goes to an array allocated up front, and the test waits
    /// for the heap to have room again, once the pool has dropped the failed reads' remaining
    /// work, before it asserts. Closing the context can fail for the same reason and is not part of
    /// what is checked.
    @ParameterizedTest
    @ValueSource(ints = { 4, 0 })
    void readThatExhaustsTheHeapFailsWithOutOfMemoryError(int threads) {
        byte[] file = RleExpansionFile.buildWide(40, 1, 64, 400_000);
        Throwable[] outcomes = new Throwable[READS];
        HardwoodContext context = HardwoodContext.create(threads);
        for (int i = 0; i < READS; i++) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                    RowReader rows = reader.buildRowReader().build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            catch (Throwable t) {
                outcomes[i] = t;
            }
        }
        try {
            context.close();
        }
        catch (Throwable t) {
            // the heap may still be exhausted
        }
        awaitHeapRoom();

        assertThat(outcomes).as("what each read threw: %s", Arrays.toString(outcomes))
                .allMatch(OutOfMemoryError.class::isInstance);
    }

    private static void awaitHeapRoom() {
        while (true) {
            try {
                heapProbe = new byte[HEAP_PROBE_BYTES];
                heapProbe = null;
                return;
            }
            catch (OutOfMemoryError e) {
                Thread.onSpinWait();
            }
        }
    }
}
