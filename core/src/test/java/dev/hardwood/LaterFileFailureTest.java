/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A later file whose footer cannot be read fails the read when the read reaches it: every row
/// of the files before it is returned first, and the failure names the file it belongs to
/// rather than the row group the read was in when that file was planned.
class LaterFileFailureTest {

    private static final Path FIRST = Paths.get("src/test/resources/multi_file_part0.parquet");

    @Test
    void everyRowOfTheFileBeforeIsReturnedFirst() throws Exception {
        long expectedRows;
        try (ParquetFileReader first = ParquetFileReader.open(InputFile.of(FIRST))) {
            expectedRows = first.getFileMetaData().numRows();
        }

        AtomicLong rows = new AtomicLong();
        try (ParquetFileReader parquet = ParquetFileReader.openAll(
                List.of(InputFile.of(FIRST), new UnreadableInputFile()))) {
            assertThatThrownBy(() -> {
                try (RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                        rows.incrementAndGet();
                    }
                }
            }).isInstanceOf(IOException.class)
                    .hasMessage("[unreadable.parquet] Failed to read metadata");
        }
        assertThat(rows.get()).isEqualTo(expectedRows);
    }

    /// Opens, but fails every read, so its footer cannot be loaded.
    private static final class UnreadableInputFile implements InputFile {

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            throw new IOException("Device not ready");
        }

        @Override
        public long length() {
            return 1024;
        }

        @Override
        public String name() {
            return "unreadable.parquet";
        }

        @Override
        public void close() {
        }
    }
}
