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
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/// Pins the [AutoCloseable] contract that `close()` is idempotent: calling it
/// repeatedly after a reader is depleted must not redo the pipeline teardown.
///
/// The decode-pipeline rework in v1.0.0.Beta2 turned `close()` from a single
/// boolean write into a per-column worker quiesce (virtual-thread joins,
/// in-flight decode draining) plus, on the column path, an [InputFile] close.
/// Without an idempotency guard every redundant `close()` re-ran that work
/// (issue #659).
class RowReaderCloseIdempotencyTest {

    /// Counts how often the wrapped [InputFile] is closed.
    private static final class CountingInputFile implements InputFile {

        private final InputFile delegate;
        private final AtomicInteger closeCount = new AtomicInteger();

        CountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
            delegate.close();
        }
    }

    @Test
    void columnReaderClosesUnderlyingFileExactlyOnce() throws Exception {
        CountingInputFile file = new CountingInputFile(
                InputFile.of(Paths.get("src/test/resources/page_index_test.parquet")));

        ParquetFileReader fileReader = ParquetFileReader.open(file);
        ColumnReader columnReader = fileReader.columnReader(0);
        while (columnReader.nextBatch()) {
            // drain
        }

        for (int i = 0; i < 5; i++) {
            columnReader.close();
        }

        assertThat(file.closeCount.get())
                .as("redundant ColumnReader.close() must not re-close the input file")
                .isEqualTo(1);

        fileReader.close();
    }

    @Test
    void flatRowReaderCloseIsIdempotent() throws Exception {
        assertRowReaderCloseIdempotent("src/test/resources/page_index_test.parquet");
    }

    @Test
    void nestedRowReaderCloseIsIdempotent() throws Exception {
        assertRowReaderCloseIdempotent("src/test/resources/nested_struct_test.parquet");
    }

    private static void assertRowReaderCloseIdempotent(String path) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(Paths.get(path)))) {
            RowReader rowReader = fileReader.rowReader();
            while (rowReader.hasNext()) {
                rowReader.next();
            }

            assertThatCode(() -> {
                for (int i = 0; i < 5; i++) {
                    rowReader.close();
                }
            }).doesNotThrowAnyException();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }
}
