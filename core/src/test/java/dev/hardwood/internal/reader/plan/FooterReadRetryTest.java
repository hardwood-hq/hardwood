/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A footer read ahead of the read that fails is read again at once, as a data request that fails
/// is, and once more as the read lists the file where it fails again: a transient failure ahead
/// does not fail the read, which returns every row of every file.
class FooterReadRetryTest {

    private static final Path FIRST = Path.of("src/test/resources/multi_file_part0.parquet");
    private static final Path SECOND = Path.of("src/test/resources/multi_file_part1.parquet");

    @Test
    void aFooterReadThatFailsOnceIsReadAgain() throws Exception {
        long expectedRows = rows(FIRST) + rows(SECOND) + rows(SECOND);
        // Not in memory, so the read reads the footers of the files after the next one ahead
        FailingReads third = new FailingReads(InputFile.of(SECOND), 1);

        long rows = 0;
        try (ParquetFileReader parquet = ParquetFileReader.openAll(List.of(
                new CountingInputFile(InputFile.of(FIRST)), new CountingInputFile(InputFile.of(SECOND)), third));
                RowReader reader = parquet.rowReader()) {
            while (reader.hasNext()) {
                reader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(expectedRows);
        assertThat(third.failures.get()).isEqualTo(1);
    }

    /// A footer read ahead that fails again on its second read gives up the read's claim, so the
    /// file fails no other read of the reader, and is read once more as the read lists the file.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aFooterReadThatFailsTwiceAheadIsReadAgainOnceTheFileIsListed() throws Exception {
        long expectedRows = rows(FIRST) + rows(SECOND) + rows(SECOND);
        FailingReads third = new FailingReads(InputFile.of(SECOND), 2);
        // the listing does not reach the third file before its footer failed twice ahead
        HeldUntil second = new HeldUntil(InputFile.of(SECOND), third.failed);

        long rows = 0;
        try (ParquetFileReader parquet = ParquetFileReader.openAll(List.of(
                new CountingInputFile(InputFile.of(FIRST)), second, third));
                RowReader reader = parquet.rowReader()) {
            while (reader.hasNext()) {
                reader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(expectedRows);
        assertThat(third.failures.get()).isEqualTo(2);
    }

    private static long rows(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            return reader.getFileMetaData().numRows();
        }
    }

    /// Fails its first `count` reads, as a backend does on a transient fault, and reads `delegate`
    /// after; `failed` is counted down at the last failure.
    private static final class FailingReads extends CountingInputFile {

        final AtomicInteger failures = new AtomicInteger();
        final CountDownLatch failed = new CountDownLatch(1);
        private final int count;

        FailingReads(InputFile delegate, int count) {
            super(delegate);
            this.count = count;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            int failure = failures.incrementAndGet();
            if (failure <= count) {
                if (failure == count) {
                    failed.countDown();
                }
                throw new IOException("Connection reset");
            }
            failures.decrementAndGet();
            return super.readRange(offset, length);
        }
    }

    /// Holds every read of `delegate` until `released` is counted down.
    private static final class HeldUntil extends CountingInputFile {

        private final CountDownLatch released;

        HeldUntil(InputFile delegate, CountDownLatch released) {
            super(delegate);
            this.released = released;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            try {
                if (!released.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The read was not released");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return super.readRange(offset, length);
        }
    }
}
