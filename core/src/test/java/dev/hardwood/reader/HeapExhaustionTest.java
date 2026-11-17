/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.RleExpansionFile;

import static org.assertj.core.api.Assertions.assertThat;

/// A read that exhausts the heap fails with the `OutOfMemoryError` rather than leaving its
/// consumer waiting, however many of the read's threads meet it and wherever: in a decode, or in
/// reporting the decode's failure, which allocates too.
///
/// The reads run in a child JVM of 64 MiB with the working-set limit and the heap quota lifted,
/// which would otherwise fail them cleanly first, over 40 `INT64`
/// columns whose one data page each holds 400,000 values (3.2 MB of batches per column, 128 MB
/// in all). Each JVM reads the file three times on one context, so that a read that leaves the
/// context's pool threads broken fails the reads after it. The child reports through its exit
/// status, the number of reads that failed with `OutOfMemoryError`, since printing allocates and
/// the heap may still be exhausted when the reads are done.
class HeapExhaustionTest {

    @ParameterizedTest
    @ValueSource(ints = { 4, 0 })
    void readThatExhaustsTheHeapFailsWithOutOfMemoryError(int threads, @TempDir Path workDir) throws Exception {
        Path stdout = workDir.resolve("stdout.txt");
        Path stderr = workDir.resolve("stderr.txt");
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m",
                "-Dhardwood.internal.maxWorkingSetBytes=" + Long.MAX_VALUE,
                "-Dhardwood.internal.readAheadBytes=" + Long.MAX_VALUE,
                "-cp", System.getProperty("java.class.path"),
                Reads.class.getName(), Integer.toString(threads)))
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile())
                .start();
        boolean exited = process.waitFor(1, TimeUnit.MINUTES);
        if (!exited) {
            process.destroyForcibly().waitFor();
        }
        String output = Files.readString(stdout) + Files.readString(stderr);

        assertThat(exited).as("the child JVM exited within a minute; its output:%n%s", output).isTrue();
        assertThat(process.exitValue()).as("reads failing with OutOfMemoryError; output:%n%s", output)
                .isEqualTo(Reads.READS);
    }

    /// Reads the file [#READS] times on a context of the given threads and halts with the number
    /// of reads that failed with `OutOfMemoryError`, plus [#OTHER] for each that ended otherwise.
    ///
    /// Nothing the child runs once the heap is exhausted may be run for the first time, as linking
    /// code allocates: [#outcome] runs once up front, and the halt is retried until the pool has dropped the failed reads' remaining work and the heap
    /// has room again. Closing the context can fail for the same reason and is not part of what is
    /// checked.
    public static final class Reads {

        static final int READS = 3;
        static final int OTHER = 10;

        public static void main(String[] args) {
            int threads = Integer.parseInt(args[0]);
            byte[] file = RleExpansionFile.buildWide(40, 1, 64, 400_000);
            Runtime runtime = Runtime.getRuntime();
            int status = outcome(new OutOfMemoryError()) - 1;
            HardwoodContext context = HardwoodContext.create(threads);
            for (int i = 0; i < READS; i++) {
                try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                        RowReader rows = reader.buildRowReader().build()) {
                    while (rows.hasNext()) {
                        rows.next();
                    }
                    status += OTHER;
                }
                catch (Throwable t) {
                    status += outcome(t);
                }
            }
            try {
                context.close();
            }
            catch (Throwable t) {
                // the heap may still be exhausted
            }
            while (true) {
                try {
                    runtime.halt(status);
                }
                catch (Throwable t) {
                    Thread.onSpinWait();
                }
            }
        }

        private static int outcome(Throwable t) {
            return t instanceof OutOfMemoryError ? 1 : OTHER;
        }
    }
}
