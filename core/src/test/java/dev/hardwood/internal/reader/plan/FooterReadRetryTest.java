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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A footer read that fails is read again at once, as a data request that fails is: a transient
/// failure does not fail the read. One that fails again fails the read at its file's first row, and
/// the next read of the reader reads the footer again.
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

    @Test
    void aFooterReadThatFailsTwiceFailsTheReadAtItsFileAndIsReadAgainByTheNextRead() throws Exception {
        long rowsBefore = rows(FIRST) + rows(SECOND);
        FailingReads third = new FailingReads(InputFile.of(SECOND), 2);

        try (ParquetFileReader parquet = ParquetFileReader.openAll(List.of(
                new CountingInputFile(InputFile.of(FIRST)), new CountingInputFile(InputFile.of(SECOND)), third))) {
            AtomicLong returned = new AtomicLong();
            assertThatThrownBy(() -> {
                try (RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                        returned.incrementAndGet();
                    }
                }
            }).isInstanceOf(IOException.class)
                    .hasMessage("[multi_file_part1.parquet] Failed to read metadata");
            assertThat(returned).hasValue(rowsBefore);

            returned.set(0);
            try (RowReader reader = parquet.rowReader()) {
                while (reader.hasNext()) {
                    reader.next();
                    returned.incrementAndGet();
                }
            }
            assertThat(returned).hasValue(rowsBefore + rows(SECOND));
        }
        assertThat(third.failures.get()).isEqualTo(2);
    }

    private static long rows(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            return reader.getFileMetaData().numRows();
        }
    }

    /// Fails its first `count` reads, as a backend does on a transient fault, and reads `delegate`
    /// after.
    private static final class FailingReads extends CountingInputFile {

        final AtomicInteger failures = new AtomicInteger();
        private final int count;

        FailingReads(InputFile delegate, int count) {
            super(delegate);
            this.count = count;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            int failure = failures.incrementAndGet();
            if (failure <= count) {
                throw new IOException("Connection reset");
            }
            failures.decrementAndGet();
            return super.readRange(offset, length);
        }
    }
}
