/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/// Pins the [AutoCloseable] contract that `close()` is idempotent: calling it
/// repeatedly after a reader is depleted must not redo the pipeline teardown.
///
/// `close()` stops every column — it waits for the column's fetch steps and
/// page tasks — plus, on the column path, tears down the iterator-local
/// caches.
/// Without an idempotency guard every redundant `close()` re-ran that work
/// (issue #659).
class RowReaderCloseIdempotencyTest {

    @Test
    void columnReaderLeavesInputLifecycleToParent() throws Exception {
        CountingInputFile file = new CountingInputFile(
                InputFile.of(Paths.get("src/test/resources/page_index_test.parquet")));

        ParquetFileReader fileReader = ParquetFileReader.open(file);
        try {
            ColumnReader columnReader = fileReader.columnReader(0);
            // Drain first: the teardown #659 is about — stopping started
            // columns — only exists once decoding has run.
            while (columnReader.nextBatch()) {
                // drain
            }
            assertThatCode(() -> {
                for (int i = 0; i < 5; i++) {
                    columnReader.close();
                }
            }).doesNotThrowAnyException();

            assertThat(file.closeCount())
                    .as("ColumnReader.close() must leave the parent-owned input open")
                    .isZero();

            try (RowReader rowReader = fileReader.rowReader()) {
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
            }
            assertThat(file.closeCount()).isZero();
        }
        finally {
            fileReader.close();
        }

        assertThat(file.closeCount())
                .as("ParquetFileReader.close() must close its input exactly once")
                .isEqualTo(1);
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
