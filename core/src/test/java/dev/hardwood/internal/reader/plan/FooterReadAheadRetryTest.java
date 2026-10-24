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

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// A footer read ahead of the read that fails is read again once the read needs the file, as a
/// data read started ahead of demand is: a transient failure ahead does not fail the read.
class FooterReadAheadRetryTest {

    private static final Path FIRST = Path.of("src/test/resources/multi_file_part0.parquet");
    private static final Path SECOND = Path.of("src/test/resources/multi_file_part1.parquet");

    @Test
    void aFooterReadAheadThatFailedIsReadAgainOnDemand() throws Exception {
        long expectedRows = rows(FIRST) + rows(SECOND) + rows(SECOND);
        // Not in memory, so the read reads the footers of the files after the next one ahead
        FailsFirstRead third = new FailsFirstRead(InputFile.of(SECOND));

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

    private static long rows(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            return reader.getFileMetaData().numRows();
        }
    }

    /// Fails its first read, as a backend does on a transient fault, and reads `delegate` after.
    private static final class FailsFirstRead extends CountingInputFile {

        final AtomicInteger failures = new AtomicInteger();

        FailsFirstRead(InputFile delegate) {
            super(delegate);
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (failures.compareAndSet(0, 1)) {
                throw new IOException("Connection reset");
            }
            return super.readRange(offset, length);
        }
    }
}
