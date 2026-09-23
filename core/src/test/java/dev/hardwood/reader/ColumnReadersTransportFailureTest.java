/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A transport failure reaches the caller as the [IOException] `ColumnReaders` declares.
///
/// The decode pipeline crosses task boundaries a checked exception cannot travel through, so a
/// failed read arrives at the reader wrapped in an [UncheckedIOException] and has to be unwrapped
/// before it escapes. Every column reader, filtered or not, reaches its columns through the same
/// `ColumnCursor` advance; the filtered case is the one asserted here because it decodes the
/// predicate columns alongside the payload columns.
class ColumnReadersTransportFailureTest {

    /// Reads normally until [#failFrom] is set, so a file can be opened and its footer parsed
    /// before the transport starts failing.
    private static final class FailingInputFile implements InputFile {

        private final InputFile delegate;
        private volatile boolean failing;

        FailingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        void failFrom() {
            failing = true;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (failing) {
                throw new IOException("simulated transport failure");
            }
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
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void aFilteredColumnReadersReportsAFailedReadAsIOException() throws Exception {
        FailingInputFile file = new FailingInputFile(
                InputFile.of(Paths.get("src/test/resources/plain_uncompressed.parquet")));

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            // Set before the readers are built rather than after. The fixture is
            // small enough that building them prefetches every byte it needs, so a
            // failure armed afterwards would find nothing left to read and the
            // assertion would pass or fail on timing.
            file.failFrom();

            assertThatThrownBy(() -> {
                ColumnReaders readers =
                        reader.buildColumnReaders(ColumnProjection.columns("id", "value"))
                                .filter(FilterPredicate.gtEq("id", 0L))
                                .build();
                readers.nextBatch();
            })
                    .as("a failed read must arrive as the IOException the signatures declare")
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(UncheckedIOException.class)
                    .hasRootCauseMessage("simulated transport failure");
        }
    }
}
